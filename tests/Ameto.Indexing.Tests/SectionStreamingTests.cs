using System.Buffers;
using System.Buffers.Binary;
using Ameto.Core;
using Ameto.Indexing;
using Ameto.Storage;
using MessagePack;

namespace Ameto.Indexing.Tests;

/// <summary>
/// <see cref="SegmentIndexBuilder.WriteSections"/> is what the segment writer calls now. It
/// must put on the stream exactly what the writer used to put there from the three blobs —
/// <c>uint32 length</c> + bytes per section — at the offsets it reports, from any starting
/// position, and for sections larger than its streaming buffer.
/// </summary>
public sealed class SectionStreamingTests
{
    private static SegmentIndexBuilder Build(int events, int distinctValues)
    {
        var pool   = new StringInternPool();
        int svcIdx = pool.Intern("Wallet.API");
        int tmplIdx = pool.Intern("HTTP {Method} {Route} responded {Status} in {Elapsed} ms");
        string tmpl = pool.Get(tmplIdx);
        using var hot = new HotTierSegment(events + 1, (long)events * 256 + 1024 * 1024);
        long baseTicks = DateTimeOffset.UtcNow.UtcTicks;
        var buf = new ArrayBufferWriter<byte>(256);
        var rng = new Random(3);
        for (int i = 0; i < events; i++)
        {
            buf.Clear();
            var w = new MessagePackWriter(buf);
            w.WriteMapHeader(2);
            w.Write("customerId"); w.Write("cust-" + rng.Next(distinctValues));
            w.Write("status");     w.Write((long)(200 + i % 5));
            w.Flush();
            hot.TryWrite(new LogEventHeader
            {
                Id = new EventId(0u, (uint)i).RawValue, TimestampUtcTicks = baseTicks + i,
                Level = LogLevel.Information, MessageTemplatePoolIndex = tmplIdx, ServiceNamePoolIndex = svcIdx,
                TraceIdHi = (ulong)rng.NextInt64(), TraceIdLo = (ulong)rng.NextInt64(), SpanId = (ulong)rng.NextInt64(),
            }, buf.WrittenSpan, tmpl);
        }
        var builder = new SegmentIndexBuilder(events);
        builder.Build(hot, pool);
        return builder;
    }

    private static byte[] Framed(byte[] blob)
    {
        var framed = new byte[4 + blob.Length];
        BinaryPrimitives.WriteUInt32LittleEndian(framed, (uint)blob.Length);
        blob.CopyTo(framed, 4);
        return framed;
    }

    [Theory]
    [InlineData(2_000, 500, 0)]
    [InlineData(2_000, 500, 12345)]
    [InlineData(120_000, 100_000, 7)]   // trigram + inverted sections each past the 1 MB buffer
    public void StreamedSections_AreTheFramedBlobs_AtTheReportedOffsets(int events, int distinct, int leadingBytes)
    {
        using var builder = Build(events, distinct);
        var (inverted, trigram, bloom) = builder.Serialise();

        var ms = new MemoryStream();
        ms.Write(new byte[leadingBytes]);
        builder.WriteSections(ms, out long invOff, out long triOff, out long bloomOff);

        Assert.Equal(leadingBytes, invOff);
        Assert.Equal(invOff + 4 + inverted.Length, triOff);
        Assert.Equal(triOff + 4 + trigram.Length,  bloomOff);
        Assert.Equal(bloomOff + 4 + bloom.Length,  ms.Length);
        Assert.Equal(ms.Length, ms.Position);

        var all = ms.ToArray();
        Assert.Equal(Framed(inverted), all[(int)invOff..(int)triOff]);
        Assert.Equal(Framed(trigram),  all[(int)triOff..(int)bloomOff]);
        Assert.Equal(Framed(bloom),    all[(int)bloomOff..]);
    }

    /// <summary>
    /// An out-of-order caller (ISegmentIndexSink is public; the writer itself is monotonic)
    /// makes the trigram postings shrink when they are normalised. The length written ahead of
    /// the section must be the normalised length, or the framed section is corrupt.
    /// </summary>
    [Fact]
    public void OutOfOrderOrdinals_StreamTheNormalisedSections()
    {
        var pool   = new StringInternPool();
        int svcIdx = pool.Intern("Wallet.API");
        int tmplIdx = pool.Intern("settlement {Id} failed for {wallet}");
        string tmpl = pool.Get(tmplIdx);
        const int events = 300;
        using var hot = new HotTierSegment(events + 1, (long)events * 256 + 1024 * 1024);
        long baseTicks = DateTimeOffset.UtcNow.UtcTicks;
        var buf = new ArrayBufferWriter<byte>(128);
        for (int i = 0; i < events; i++)
        {
            buf.Clear();
            var w = new MessagePackWriter(buf);
            w.WriteMapHeader(1);
            w.Write("wallet"); w.Write("wallet:" + i % 7);
            w.Flush();
            hot.TryWrite(new LogEventHeader
            {
                Id = new EventId(0u, (uint)i).RawValue, TimestampUtcTicks = baseTicks + i,
                Level = LogLevel.Error, MessageTemplatePoolIndex = tmplIdx, ServiceNamePoolIndex = svcIdx,
            }, buf.WrittenSpan, tmpl);
        }

        using var builder = new SegmentIndexBuilder(events);
        var rng = new Random(3);
        // Descending and repeated ordinals: every bucket becomes unsorted with duplicates.
        for (int i = events - 1; i >= 0; i--) builder.Add((uint)i, HotTierEventSource.EventAt(hot, pool, i));
        for (int k = 0; k < 100; k++) { int i = rng.Next(events); builder.Add((uint)i, HotTierEventSource.EventAt(hot, pool, i)); }

        var ms = new MemoryStream();
        builder.WriteSections(ms, out long invOff, out long triOff, out long bloomOff);
        var all = ms.ToArray();

        // The framed lengths must be the lengths on disk, and the sections must decode to the
        // in-order build's answers.
        uint invLen = BinaryPrimitives.ReadUInt32LittleEndian(all.AsSpan((int)invOff));
        uint triLen = BinaryPrimitives.ReadUInt32LittleEndian(all.AsSpan((int)triOff));
        Assert.Equal(invOff + 4 + invLen, triOff);
        Assert.Equal(triOff + 4 + triLen, bloomOff);

        var tri = SegmentTrigramIndex.Deserialise(all.AsSpan((int)triOff + 4, (int)triLen));
        var hits = tri.Lookup("wallet:3")!;
        Assert.Equal(Enumerable.Range(0, events).Where(i => i % 7 == 3).Select(i => (uint)i).ToArray(), hits);
        var inv = SegmentInvertedIndex.Deserialise(all.AsSpan((int)invOff + 4, (int)invLen));
        Assert.Equal(Enumerable.Range(0, events).Select(i => (uint)i).ToArray(), inv.Lookup("@l", "Error"));
    }

    [Fact]
    public void StreamedSections_AllocateNoBlobs()
    {
        using var builder = Build(60_000, 50_000);
        builder.WriteSections(Stream.Null, out _, out _, out _);   // warm the pool

        long before = GC.GetAllocatedBytesForCurrentThread();
        builder.WriteSections(Stream.Null, out _, out _, out _);
        long bytes = GC.GetAllocatedBytesForCurrentThread() - before;

        // The blob path allocates the three sections (several MB here); the streamed path
        // allocates the writer object and nothing proportional to the index.
        Assert.True(bytes < 4096, $"{bytes} B allocated streaming three sections");
    }

    [Fact]
    public void WriteSections_AfterDispose_Throws()
    {
        var builder = Build(100, 10);
        builder.Dispose();
        Assert.Throws<ObjectDisposedException>(() => builder.WriteSections(Stream.Null, out _, out _, out _));
    }
}
