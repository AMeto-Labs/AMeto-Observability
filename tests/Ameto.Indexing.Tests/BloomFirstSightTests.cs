using System.Buffers;
using Ameto.Core;
using Ameto.Indexing;
using Ameto.Storage;
using MessagePack;

namespace Ameto.Indexing.Tests;

/// <summary>
/// The builder adds a term to the bloom filter the first time it sees it and not again. The
/// filter is a set, so the section must be byte-identical to adding on every event (the
/// reference build still does that), and the count the writer sizes the next group from must
/// still be the number of terms PRESENTED — repeats included — or every forecast after the
/// first group would silently shrink.
/// </summary>
public sealed class BloomFirstSightTests
{
    private static HotTierSegment Tier(int events, StringInternPool pool, int svcIdx, int tmplIdx, string tmpl)
    {
        var hot = new HotTierSegment(events + 1, (long)events * 512 + 1024 * 1024);
        long baseTicks = DateTimeOffset.UtcNow.UtcTicks;
        var buf = new ArrayBufferWriter<byte>(256);
        for (int i = 0; i < events; i++)
        {
            buf.Clear();
            var w = new MessagePackWriter(buf);
            w.WriteMapHeader(4);
            w.Write("region");  w.Write("ae-dxb");                 // repeats every event
            w.Write("attempt"); w.Write((long)(i % 3));             // three distinct values
            w.Write("ok");      w.Write(i % 2 == 0);                // both bools
            w.Write("user");    w.Write("u-" + i);                  // unique per event
            w.Flush();
            hot.TryWrite(new LogEventHeader
            {
                Id = new EventId(0u, (uint)i).RawValue, TimestampUtcTicks = baseTicks + i,
                Level = LogLevel.Warning, MessageTemplatePoolIndex = tmplIdx, ServiceNamePoolIndex = svcIdx,
                TraceIdHi = 0x11UL, TraceIdLo = (ulong)(i % 5) + 1, SpanId = 0x22UL,
            }, buf.WrittenSpan, tmpl);
        }
        return hot;
    }

    [Fact]
    public void FirstSightBloom_IsByteIdenticalToAddOnEveryEvent_AndCountsPresentedTerms()
    {
        const int events = 1_000;
        var pool   = new StringInternPool();
        int svcIdx = pool.Intern("Wallet.API");
        int tmplIdx = pool.Intern("settlement {Id} done");
        using var hot = Tier(events, pool, svcIdx, tmplIdx, pool.Get(tmplIdx));

        using var streaming = new SegmentIndexBuilder(events);
        streaming.Build(hot, pool);
        using var reference = new SegmentIndexBuilder(events);
        reference.BuildReference(hot, pool);

        Assert.Equal(reference.SerialisedBloomFilter, streaming.SerialisedBloomFilter);

        // Per event: level, template, trace, span, service, and 4 keys + 4 values = 13 presented.
        Assert.Equal(13L * events, streaming.BloomTermsAdded);

        using var bloom = SegmentBloomFilter.Deserialise(streaming.SerialisedBloomFilter);
        foreach (var term in new[] { "warning", "settlement {id} done", "wallet.api", "ae-dxb", "region", "attempt", "2", "ok", "true", "false", "user", "u-999", "0000000000000011" + "0000000000000003" })
            Assert.True(bloom.MightContain(term), term);
    }
}
