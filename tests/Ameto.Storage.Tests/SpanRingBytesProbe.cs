using System.Diagnostics;
using System.Text;
using Microsoft.Extensions.Logging.Abstractions;
using Ameto.Tracing;
using Ameto.Tracing.Ingestion;
using Ameto.Tracing.Storage;
using Xunit.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// WHAT A FULL SPAN RING HOLDS — the burst the drainer has not caught up with yet, which is exactly
/// what a 512 MB stand has no room for. None existed before this package (TS#9 / TI#3).
///
/// <para>Fill the ring through the ingest endpoint until it refuses, drop every reference the probe
/// itself holds, and read the live set: whatever is still there, the RING is keeping alive. Two
/// shapes, because they are the two regimes a count-based ring cannot tell apart — the ordinary
/// eight-attribute span, and one carrying 10 KB of attributes (a SQL statement, a stack). The raw
/// ring (TI#3) keeps nothing managed, so its footprint is the NATIVE line: the slot array (fixed)
/// plus the arena the backlog reached.</para>
///
/// <para>Printed, not asserted: the live set is a process-wide number. The behaviour is gated in
/// <c>SpanRingBackPressureTests</c> and <c>SpanRingRawTests</c>.</para>
/// </summary>
public sealed class SpanRingBytesProbe : IDisposable
{
    private readonly ITestOutputHelper _out;
    private readonly List<string>      _dirs = [];
    public SpanRingBytesProbe(ITestOutputHelper output) => _out = output;

    public void Dispose()
    {
        foreach (var d in _dirs)
            try { Directory.Delete(d, true); } catch { }
    }

    [Fact]
    public void Retained_bytes_of_a_full_ring()
    {
        Fill(capacity: 1 << 10, blobBytes: 375, label: null);                 // warm
        Fill(capacity: 1 << 16, blobBytes: 375,    label: "ordinary span, 375 B attributes");
        Fill(capacity: 1 << 13, blobBytes: 10_000, label: "heavy span, 10 KB attributes");
        Fill(capacity: 1 << 16, blobBytes: 10_000, label: "heavy span, 10 KB attributes, the 512 MB stand's ring budget",
             budget: new Ameto.Core.TracesOptions().RingMaxBytesFor(Ameto.Core.MemoryBudgets.Derive(384L << 20, 512L << 20)));
    }

    private void Fill(int capacity, int blobBytes, string? label, long budget = 0)
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

        var ring     = budget > 0 ? new SpanRingBuffer(capacity, budget) : SpanRingBytesFixture.Ring(capacity);
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
            _out.WriteLine($"  managed     {retained / 1e6,12:N1} MB   {(accepted == 0 ? 0 : retained / accepted),8:N0} B/span held");
            _out.WriteLine($"  native      {native / 1e6,12:N1} MB   (slots {ring.SlotBytes / 1e6:N1} MB + arena reached)");
            _out.WriteLine($"  enqueue     {(accepted == 0 ? 0 : allocated / accepted),12:N0} B/span allocated (item door: the gRPC receiver's)");
        }

        // THE RESTING LEVEL (review F3): drain the burst, as the drainer would, and let the ring go
        // idle — what the drainer's idle trim leaves behind is what the process keeps.
        var headers = new SpanHeader[512];
        var apart   = new byte[]?[512];
        int n;
        while ((n = ring.TryDequeueMany(headers, apart)) > 0) ring.Release(headers.AsSpan(0, n));
        long given   = ring.TrimIdleArena();
        long resting = SpanRingBytesFixture.NativeBytes(ring);
        if (label is not null)
            _out.WriteLine($"  after idle  {resting / 1e6,12:N1} MB   (the trim gave back {given / 1e6:N1} MB; slots {ring.SlotBytes / 1e6:N1} MB stay)");

        GC.KeepAlive(endpoint);
        ring.Dispose();
    }

    /// <summary>
    /// THE RAW PATH END TO END, per span: what the PRODUCER (a request thread) allocates handing a
    /// span to the sink, what the DRAINER spends taking it into the log and the tier, and what the
    /// tier then retains — 49 000 ordinary spans, the name and service as a parser slices them.
    /// </summary>
    [Fact]
    public void Raw_ingest_cost_end_to_end()
    {
        Raw(2_000, label: null);                                              // warm
        Raw(49_000, label: "49 000 ordinary spans, raw sink -> ring -> drainer -> engine");
    }

    private void Raw(int spans, string? label)
    {
        string dir = Path.Combine(Path.GetTempPath(), "ameto-rawprobe-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        _dirs.Add(dir);

        var blob = TraceHotTierProbe.SqlClientBlob(0);
        var name = Encoding.UTF8.GetBytes("SELECT payments");
        var svc  = Encoding.UTF8.GetBytes("billing");

        GC.Collect(2, GCCollectionMode.Aggressive, blocking: true, compacting: true);
        long liveBefore = GC.GetTotalMemory(forceFullCollection: true);

        var pools   = new SpanStringPools();
        var engine  = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance, false, true, null, pools);
        var ring    = new SpanRingBuffer(1 << 16, 64L * 1024 * 1024, pools);
        var sink    = new SpanIngestionEndpoint(ring, NullLogger<SpanIngestionEndpoint>.Instance);
        var drainer = new SpanDrainer(ring, engine, NullLogger<SpanDrainer>.Instance, startLoop: false);

        long produceAlloc = 0, drainAlloc = 0;
        var  produce = new Stopwatch();
        var  drain   = new Stopwatch();
        long baseNano = 1_785_000_000_000_000_000L;
        for (int first = 0; first < spans; first += 512)
        {
            int n = Math.Min(512, spans - first);

            long a0 = GC.GetAllocatedBytesForCurrentThread();
            produce.Start();
            int idx = sink.InternService(svc);
            for (int i = first; i < first + n; i++)
                sink.TryIngestRaw(new TraceId(0x9E37UL, (ulong)(i / 10 + 1)), new SpanId((ulong)(i + 1)),
                                  i % 10 == 0 ? default : new SpanId((ulong)(i / 10 * 10 + 1)),
                                  baseNano + i * 1_000_000L, 1_000_000L, name, idx, svc,
                                  SpanKind.Client, SpanStatusCode.Unset, 0, blob);
            sink.EndBatch();
            produce.Stop();
            long a1 = GC.GetAllocatedBytesForCurrentThread();

            drain.Start();
            while (drainer.DrainOnce(out _) > 0) { }
            drain.Stop();
            long a2 = GC.GetAllocatedBytesForCurrentThread();

            produceAlloc += a1 - a0;
            drainAlloc   += a2 - a1;
        }

        long retained = GC.GetTotalMemory(forceFullCollection: true) - liveBefore;
        long native   = ring.SlotBytes + ring.ArenaHighWaterBytes;

        if (label is not null)
        {
            _out.WriteLine("");
            _out.WriteLine($"RAW PATH  {label}");
            _out.WriteLine($"  producer    {produce.Elapsed.TotalNanoseconds / spans,10:N0} ns/span   {produceAlloc / (double)spans,8:N1} B/span allocated");
            _out.WriteLine($"  drainer     {drain.Elapsed.TotalNanoseconds / spans,10:N0} ns/span   {drainAlloc / (double)spans,8:N1} B/span allocated");
            _out.WriteLine($"  tier        {retained / 1e6,10:N1} MB       {retained / spans,8:N0} B/span retained (engine + its tier)");
            _out.WriteLine($"  ring native {native / 1e6,10:N1} MB       (slots {ring.SlotBytes / 1e6:N1} MB + arena reached {ring.ArenaHighWaterBytes / 1024.0:N0} KiB)");
        }

        GC.KeepAlive(drainer);
        engine.Dispose();
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

    public static long NativeBytes(SpanRingBuffer ring) => ring.SlotBytes + ring.ArenaHighWaterBytes;
}
