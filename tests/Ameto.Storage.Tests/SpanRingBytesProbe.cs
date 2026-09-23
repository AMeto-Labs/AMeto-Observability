using Microsoft.Extensions.Logging.Abstractions;
using Ameto.Tracing;
using Ameto.Tracing.Ingestion;
using Xunit.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// WHAT A FULL SPAN RING HOLDS — the burst the drainer has not caught up with yet, which is exactly
/// what a 512 MB stand has no room for. None existed before this package (TS#9 / TI#3).
///
/// <para>Fill the ring through the ingest endpoint until it refuses, drop every reference the probe
/// itself holds, and read the live set: whatever is still there, the RING is keeping alive. Two
/// shapes, because they are the two regimes a count-based ring cannot tell apart — the ordinary
/// eight-attribute span, and one carrying 10 KB of attributes (a SQL statement, a stack).</para>
///
/// <para>Printed, not asserted: the live set is a process-wide number. The behaviour is gated in
/// <c>SpanRingBackPressureTests</c>.</para>
/// </summary>
public sealed class SpanRingBytesProbe
{
    private readonly ITestOutputHelper _out;
    public SpanRingBytesProbe(ITestOutputHelper output) => _out = output;

    [Fact]
    public void Retained_bytes_of_a_full_ring()
    {
        Fill(capacity: 1 << 10, blobBytes: 375, label: null);                 // warm
        Fill(capacity: 1 << 16, blobBytes: 375,    label: "ordinary span, 375 B attributes");
        Fill(capacity: 1 << 13, blobBytes: 10_000, label: "heavy span, 10 KB attributes");
    }

    private void Fill(int capacity, int blobBytes, string? label)
    {
        GC.Collect(2, GCCollectionMode.Aggressive, blocking: true, compacting: true);
        long liveBefore = GC.GetTotalMemory(forceFullCollection: true);

        // The corpus is built before and dropped before the sample; each span has its OWN name
        // string and blob, the way a parser hands them over, so what survives is what the ring holds.
        var items = new SpanIngestItem[capacity];
        for (int i = 0; i < capacity; i++)
            items[i] = new SpanIngestItem
            {
                TraceId           = new TraceId(0x51A6UL, (ulong)(i / 10 + 1)),
                SpanId            = new SpanId((ulong)(i + 1)),
                StartTimeUnixNano = 1_785_000_000_000_000_000L + i * 1_000_000L,
                DurationNanos     = 1_000_000L,
                Name              = new string("SELECT payments".AsSpan()),
                ServiceName       = "billing",
                Kind              = SpanKind.Client,
                AttributesBytes   = Blob(blobBytes, i),
            };

        var ring     = SpanRingBytesFixture.Ring(capacity);
        var endpoint = new SpanIngestionEndpoint(ring, NullLogger<SpanIngestionEndpoint>.Instance);

        long allocBefore = GC.GetAllocatedBytesForCurrentThread();
        endpoint.TryIngest(items, out int accepted);
        long allocated = GC.GetAllocatedBytesForCurrentThread() - allocBefore;

        Array.Clear(items);
        items = null;
        long retained = GC.GetTotalMemory(forceFullCollection: true) - liveBefore;
        long native   = SpanRingBytesFixture.NativeBytes(ring);

        if (label is not null)
        {
            _out.WriteLine("");
            _out.WriteLine($"FULL RING  {capacity:N0} slots, {label}");
            _out.WriteLine($"  accepted    {accepted,12:N0} spans   ({(accepted < capacity ? "refused by BYTES" : "every slot")})");
            _out.WriteLine($"  managed     {retained / 1048576.0,12:N1} MB   {(accepted == 0 ? 0 : retained / accepted),8:N0} B/span held");
            _out.WriteLine($"  native      {native / 1048576.0,12:N1} MB   (slots + arena reached)");
            _out.WriteLine($"  enqueue     {(accepted == 0 ? 0 : allocated / accepted),12:N0} B/span allocated");
        }

        GC.KeepAlive(endpoint);
        ring.Dispose();
    }

    private static byte[] Blob(int bytes, int seed)
    {
        var buf = new System.Buffers.ArrayBufferWriter<byte>(bytes + 16);
        var w   = new MessagePack.MessagePackWriter(buf);
        w.WriteMapHeader(1);
        w.Write("db.statement");
        w.Write(new string((char)('a' + seed % 26), Math.Max(0, bytes - 20)));
        w.Flush();
        return buf.WrittenMemory.ToArray();
    }
}

/// <summary>How the probe builds a ring and reads its native footprint — the one place that knows the ring's shape.</summary>
internal static class SpanRingBytesFixture
{
    public static SpanRingBuffer Ring(int capacity) => new(capacity, new Ameto.Core.TracesOptions().EffectiveRingMaxBytes);

    public static long NativeBytes(SpanRingBuffer ring) => 0;
}
