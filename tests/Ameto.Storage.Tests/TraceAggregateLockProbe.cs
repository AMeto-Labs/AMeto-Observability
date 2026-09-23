using System.Diagnostics;
using Microsoft.Extensions.Logging.Abstractions;
using Ameto.Tracing;
using Ameto.Tracing.Storage;
using Xunit.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// WHAT THE ENGINE'S ONE LOCK COSTS THE TWO SIDES THAT SHARE IT. Ingest takes the write side of
/// <c>TraceStorageEngine._lock</c>; every query takes the read side of the same lock over the same
/// tier. A cost moved from one side to the other is only a saving if the other side can afford it,
/// so every figure here is taken with the OTHER side running against it.
///
/// <para><b>The write path (TS#4).</b> The drainer used to call <c>WriteSpan</c> per span — the
/// engine write lock and the log's append lock, four interlocked round trips, per span, and a
/// point at which a reader could interleave per span. <c>WriteSpans</c> takes both once per
/// <see cref="TraceStorageEngine.MaxSpansPerWriteHold"/> spans. The saving is ingest CPU; the price
/// is that a reader arriving behind the drainer waits out a longer hold. The arms below put a
/// number on both: per span (the old shape), one hold per 64, one hold per 128 (the cap), one hold
/// per 512 (the whole drained batch, the uncapped design the cap exists to refuse), and the cap
/// without the hand-off between holds, whose readers starve — each against
/// two readers: a point lookup, whose latency is almost all lock wait, and the span search, whose
/// read-lock hold is a walk of the whole tier and which the writer waits out in turn.</para>
///
/// <para>PRINTED, NOT ASSERTED. Wall-clock figures move with the machine and the configuration
/// (the suite runs in Debug on a two-core CI box); a probe that asserts them is a flaky test. The
/// behaviour the cap and the reader hand-off guarantee is asserted by seams in
/// <c>SpanWalTests</c>.</para>
/// </summary>
public sealed class TraceAggregateLockProbe : IDisposable
{
    private const int SpansPerTrace = 10;
    private const int Prefill       = 10_000;   // what the readers look up
    private const int DrainBatch    = 512;      // SpanDrainer.BatchSize

    // The suite runs this in Debug on every build; the numbers in the commit bodies are Release's,
    // where Prefill + Ingest stays under the 50 000-span flush threshold.
#if DEBUG
    private const int Ingest = 10_000;
    private const int Rounds = 3;
#else
    private const int Ingest = 30_000;
    private const int Rounds = 7;
#endif

    private static readonly DateTimeOffset Base = new(2026, 8, 1, 12, 0, 0, TimeSpan.Zero);

    private readonly List<string>      _dirs = [];
    private readonly ITestOutputHelper _out;

    public TraceAggregateLockProbe(ITestOutputHelper output) => _out = output;

    public void Dispose()
    {
        foreach (var d in _dirs)
            try { Directory.Delete(d, true); } catch { }
    }

    // ── TS#4: the write path against a concurrent reader ──────────────────────

    private enum Reader { None, Lookup, Search }

    private readonly record struct WriteArm(string Label, int CallSize, int PerHold, bool Handoff = true);

    /// <summary>
    /// Five shapes of the same ingest: <c>CallSize</c> spans per engine call, <c>PerHold</c> per lock hold,
    /// and whether the hand-off between holds runs. "per span (old)" is the drainer before this change
    /// — a hold per span, nothing yielded. "no hand-off" is the batch without
    /// <c>LetQueuedReadersIn</c>: the arm that shows why it exists, because its readers starve.
    /// </summary>
    private static readonly WriteArm[] Arms =
    [
        new("per span (old)",            1,          1,   Handoff: false),
        new("128/hold, no hand-off",     DrainBatch, 128, Handoff: false),
        new("64/hold",                   DrainBatch, 64),
        new("128/hold (after)",          DrainBatch, TraceStorageEngine.MaxSpansPerWriteHold),
        new("512/hold",                  DrainBatch, DrainBatch),
    ];

    private readonly record struct WriteResult(
        double NanosPerSpan, double CyclesPerSpan, long AllocPerSpan,
        double ReaderP50Us, double ReaderP99Us, double ReaderMaxUs, int Reads);

    [Fact]
    public void Batched_ingest_against_a_concurrent_reader()
    {
        // Warm every arm once, small, so no measured run is jitting the path it measures.
        foreach (var arm in Arms)
        {
            RunWriteArm(arm, prefill: 500, ingest: 2_000, Reader.Lookup);
            RunWriteArm(arm, prefill: 500, ingest: 2_000, Reader.Search);
        }

        // ROUND-ROBIN. The arms are interleaved round by round rather than run one after another, so a
        // burst of load from anything else on the machine lands on every arm alike. The UNCONTENDED
        // figures are the MINIMUM over the rounds — nothing but the machine adds noise to them, and
        // noise can only add. The CONTENDED ones are the MEDIAN, because there the variation between
        // rounds is the lock doing its job, and a minimum would report the luckiest interleaving.
        var alone  = Grid();
        var lookup = Grid();
        var search = Grid();
        for (int r = 0; r < Rounds; r++)
            for (int a = 0; a < Arms.Length; a++)
            {
                alone[a][r]  = RunWriteArm(Arms[a], Prefill, Ingest, Reader.None);
                lookup[a][r] = RunWriteArm(Arms[a], Prefill, Ingest, Reader.Lookup);
                search[a][r] = RunWriteArm(Arms[a], Prefill, Ingest, Reader.Search);
            }

        _out.WriteLine($"WRITE PATH   {Ingest:N0} spans (8 attributes, {SpansPerTrace}/trace) into a tier "
                     + $"pre-filled with {Prefill:N0}");
        _out.WriteLine($"             {Rounds} interleaved rounds: alone = minimum, contended and reader = median");
        _out.WriteLine("");
        _out.WriteLine($"  {"arm",-26} {"alone ns/span",13} {"cyc/span",9} {"B/span",7}   "
                     + $"{"+lookup ns",10} {"lookup p50",10} {"p99",9} {"n",6}   "
                     + $"{"+search ns",10} {"search p50",10} {"p99",9} {"n",4}");
        for (int a = 0; a < Arms.Length; a++)
        {
            _out.WriteLine($"  {Arms[a].Label,-26} "
                         + $"{Min(alone[a], static x => x.NanosPerSpan),13:N0} "
                         + $"{Min(alone[a], static x => x.CyclesPerSpan),9:N0} "
                         + $"{Median(alone[a], static x => x.AllocPerSpan),7:N0}   "
                         + $"{Median(lookup[a], static x => x.NanosPerSpan),10:N0} "
                         + $"{Median(lookup[a], static x => x.ReaderP50Us),8:N1}us "
                         + $"{Median(lookup[a], static x => x.ReaderP99Us),7:N1}us "
                         + $"{Median(lookup[a], static x => x.Reads),6:N0}   "
                         + $"{Median(search[a], static x => x.NanosPerSpan),10:N0} "
                         + $"{Median(search[a], static x => x.ReaderP50Us),8:N0}us "
                         + $"{Median(search[a], static x => x.ReaderP99Us),7:N0}us "
                         + $"{Median(search[a], static x => x.Reads),4:N0}");
        }
        _out.WriteLine("");
        _out.WriteLine("  alone = no reader. cyc/span = the writer thread's own CPU cycles, alone (Windows).");
        _out.WriteLine("  +lookup / +search = the same ingest with that reader looping on its own thread; its");
        _out.WriteLine("  latency includes the wait for the write lock. lookup = GetTraceAsync of one 10-span hot");
        _out.WriteLine("  trace; search = SearchSpansAsync(limit 20), a walk of the whole tier under the read lock.");
    }

    private static WriteResult[][] Grid()
    {
        var g = new WriteResult[Arms.Length][];
        for (int a = 0; a < g.Length; a++) g[a] = new WriteResult[Rounds];
        return g;
    }

    private static double Min(WriteResult[] runs, Func<WriteResult, double> pick)
    {
        double m = double.MaxValue;
        foreach (var r in runs) m = Math.Min(m, pick(r));
        return m;
    }

    private static double Median(WriteResult[] runs, Func<WriteResult, double> pick)
    {
        var v = new double[runs.Length];
        for (int i = 0; i < runs.Length; i++) v[i] = pick(runs[i]);
        Array.Sort(v);
        return v[v.Length / 2];
    }

    private WriteResult RunWriteArm(WriteArm arm, int prefill, int ingest, Reader readerKind)
    {
        string dir = NewDir();
        using var engine = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance);
        engine._maxSpansPerWriteHold = arm.PerHold;
        if (!arm.Handoff) engine._readerHandoffTicks = 0;

        // The corpus is built before anything is timed: the mapper's cost is not this probe's.
        var pre   = Corpus(0, prefill);
        var items = Corpus(prefill, ingest);
        Assert.Equal(prefill, engine.WriteSpans(pre));

        var  from      = Base.AddMinutes(-1);
        var  to        = Base.AddDays(1);
        var  latencies = new long[1 << 20];
        int  reads     = 0;
        int  stop      = 0;
        int  misses    = 0;
        using var readerUp = new ManualResetEventSlim();
        Thread? reader = null;
        if (readerKind != Reader.None)
        {
            reader = new Thread(() =>
            {
                int traces = prefill / SpansPerTrace;
                readerUp.Set();
                int t = 0;
                while (Volatile.Read(ref stop) == 0 && reads < latencies.Length)
                {
                    long s = Stopwatch.GetTimestamp();
                    int  n = 0;
                    if (readerKind == Reader.Lookup)
                    {
                        var id = pre[(t++ % traces) * SpansPerTrace].TraceId;
                        foreach (var _ in engine.GetTraceAsync(id).ToBlockingEnumerable()) n++;
                        if (n != SpansPerTrace) misses++;   // never throw on a raw thread: it takes the host down
                    }
                    else
                    {
                        foreach (var _ in engine.SearchSpansAsync(from, to, limit: 20).ToBlockingEnumerable()) n++;
                        if (n != 20) misses++;
                    }
                    latencies[reads++] = Stopwatch.GetTimestamp() - s;
                }
            }) { IsBackground = true, Name = "probe-reader" };
            reader.Start();
            readerUp.Wait();
        }

        // ── The writer, on THIS thread, so its allocation and cycle counters are its own.
        long  alloc0 = GC.GetAllocatedBytesForCurrentThread();
        ulong cyc0   = ThreadCycles();
        long  t0     = Stopwatch.GetTimestamp();
        for (int i = 0; i < items.Length; i += arm.CallSize)
        {
            var call = items.AsSpan(i, Math.Min(arm.CallSize, items.Length - i));
            if (engine.WriteSpans(call) != call.Length) throw new InvalidOperationException("refused");
        }
        long  ticks  = Stopwatch.GetTimestamp() - t0;
        ulong cycles = ThreadCycles() - cyc0;
        long  alloc  = GC.GetAllocatedBytesForCurrentThread() - alloc0;

        Volatile.Write(ref stop, 1);
        reader?.Join();
        Assert.Equal(0, misses);   // every read found what it looked for — the reader measured real reads

        double p50 = 0, p99 = 0, max = 0;
        if (reads > 0)
        {
            var sorted = latencies.AsSpan(0, reads);
            sorted.Sort();
            p50 = Us(sorted[(int)(reads * 0.50)]);
            p99 = Us(sorted[Math.Min(reads - 1, (int)(reads * 0.99))]);
            max = Us(sorted[reads - 1]);
        }

        return new WriteResult(
            ticks * 1e9 / Stopwatch.Frequency / ingest, (double)cycles / ingest, alloc / ingest,
            p50, p99, max, reads);
    }

    private static double Us(long ticks) => ticks * 1e6 / Stopwatch.Frequency;

    /// <summary>
    /// CPU cycles THIS thread has run, or 0 off Windows. Wall time on a shared machine carries every
    /// other process's load; a thread's own cycle count only moves while the thread runs, so it is
    /// the figure that separates two arms whose difference is a few tens of nanoseconds a span.
    /// </summary>
    private static ulong ThreadCycles() =>
        OperatingSystem.IsWindows() && QueryThreadCycleTime(GetCurrentThread(), out ulong c) ? c : 0;

    [System.Runtime.InteropServices.DllImport("kernel32.dll")]
    private static extern nint GetCurrentThread();

    [System.Runtime.InteropServices.DllImport("kernel32.dll")]
    [return: System.Runtime.InteropServices.MarshalAs(System.Runtime.InteropServices.UnmanagedType.Bool)]
    private static extern bool QueryThreadCycleTime(nint threadHandle, out ulong cycleTime);

    private string NewDir()
    {
        string dir = Path.Combine(Path.GetTempPath(), "ameto-aggprobe-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        _dirs.Add(dir);
        return dir;
    }

    // ── TS#6: the four aggregate reads against concurrent ingest ──────────────

    private enum Aggregate { Stats, Graph, Volume, List }

    private static readonly Aggregate[] Aggregates = [Aggregate.Stats, Aggregate.Graph, Aggregate.Volume, Aggregate.List];

    /// <summary>
    /// One call of an aggregate. <paramref name="k"/> moves the window's end by k ms so that no call
    /// is served from the memo — the probe prices the pass, not the cache — and the window still
    /// covers every span.
    /// </summary>
    private static void Call(TraceStorageEngine engine, Aggregate which, int k)
    {
        var from = Base.AddMinutes(-1);
        var to   = Base.AddDays(1).AddMilliseconds(k);
        switch (which)
        {
            case Aggregate.Stats:  engine.GetAggregateStatsAsync(from, to).GetAwaiter().GetResult(); break;
            case Aggregate.Graph:  engine.GetServiceGraphAsync(from, to).GetAwaiter().GetResult(); break;
            case Aggregate.Volume: engine.GetTraceVolumeAsync(from, to, 20).GetAwaiter().GetResult(); break;
            case Aggregate.List:
                engine.GetTraceListAsync(from, to, null, null, null, null, null, 200).GetAwaiter().GetResult();
                break;
        }
    }

    /// <summary>
    /// WHAT EACH AGGREGATE COSTS, AND WHAT IT COSTS INGEST. Per aggregate: the calling thread's own
    /// allocation and wall time per call over a tier of <see cref="AggPrefill"/> spans in three
    /// services (the graph has edges to draw); then the same ingest as the TS#4 probe with that
    /// aggregate looping on a reader thread, reporting the writer's ns/span and its slowest
    /// 512-span call — the wait for a reader that holds the lock through its pass. Printed, not
    /// asserted; the behaviour is pinned by <see cref="TraceAggregateLockTests"/>.
    /// </summary>
    [Fact]
    public void Aggregates_against_concurrent_ingest()
    {
        foreach (var a in Aggregates) RunAggregateArm(a, prefill: 1_000, ingest: 1_000, warm: true);

        _out.WriteLine($"AGGREGATES  tier of {AggPrefill:N0} spans (3 services, {SpansPerTrace}/trace, 8 attributes); "
                     + $"ingest of {AggIngest:N0} in {DrainBatch}-span calls; median of {Rounds} rounds");
        _out.WriteLine($"  {"aggregate",-8} {"B/call",12} {"us/call",9}   {"ingest ns/span",14} {"+reader",8} "
                     + $"{"slowest call us",15} {"+reader",8} {"reads",6}");
        foreach (var a in Aggregates)
        {
            var runs = new AggResult[Rounds];
            for (int r = 0; r < Rounds; r++) runs[r] = RunAggregateArm(a, AggPrefill, AggIngest, warm: false);
            _out.WriteLine($"  {a,-8} {MedianOf(runs, static x => x.BytesPerCall),12:N0} {MedianOf(runs, static x => x.UsPerCall),9:N0}   "
                         + $"{MedianOf(runs, static x => x.AloneNsPerSpan),14:N0} {MedianOf(runs, static x => x.ContendedNsPerSpan),8:N0} "
                         + $"{MedianOf(runs, static x => x.AloneWorstUs),15:N0} {MedianOf(runs, static x => x.ContendedWorstUs),8:N0} "
                         + $"{MedianOf(runs, static x => x.Reads),6:N0}");
        }
    }

#if DEBUG
    private const int AggPrefill = 5_000;
    private const int AggIngest  = 5_000;
#else
    private const int AggPrefill = 20_000;
    private const int AggIngest  = 25_000;   // + the prefill stays under the 50 000-span flush threshold
#endif

    private readonly record struct AggResult(
        double BytesPerCall, double UsPerCall, double AloneNsPerSpan, double ContendedNsPerSpan,
        double AloneWorstUs, double ContendedWorstUs, double Reads);

    private static double MedianOf(AggResult[] runs, Func<AggResult, double> pick)
    {
        var v = new double[runs.Length];
        for (int i = 0; i < runs.Length; i++) v[i] = pick(runs[i]);
        Array.Sort(v);
        return v[v.Length / 2];
    }

    private AggResult RunAggregateArm(Aggregate which, int prefill, int ingest, bool warm)
    {
        // ── Alone: the aggregate's own cost, on this thread's counters.
        double bytes, us;
        {
            using var engine = new TraceStorageEngine(NewDir(), NullLogger<TraceStorageEngine>.Instance);
            Assert.Equal(prefill, engine.WriteSpans(Corpus(0, prefill, services: 3)));
            Call(engine, which, 0);
            const int Calls = 10;
            long a0 = GC.GetAllocatedBytesForCurrentThread();
            long t0 = Stopwatch.GetTimestamp();
            for (int k = 1; k <= Calls; k++) Call(engine, which, k);
            us    = Us(Stopwatch.GetTimestamp() - t0) / Calls;
            bytes = (GC.GetAllocatedBytesForCurrentThread() - a0) / (double)Calls;
        }
        if (warm) return default;

        var (aloneNs, aloneWorst, _)   = IngestWith(which, prefill, ingest, reader: false);
        var (contNs, contWorst, reads) = IngestWith(which, prefill, ingest, reader: true);
        return new AggResult(bytes, us, aloneNs, contNs, aloneWorst, contWorst, reads);
    }

    private (double NsPerSpan, double WorstCallUs, int Reads) IngestWith(Aggregate which, int prefill, int ingest, bool reader)
    {
        using var engine = new TraceStorageEngine(NewDir(), NullLogger<TraceStorageEngine>.Instance);
        Assert.Equal(prefill, engine.WriteSpans(Corpus(0, prefill, services: 3)));
        var items = Corpus(prefill, ingest, services: 3);

        int stop = 0, reads = 0;
        using var up = new ManualResetEventSlim();
        Thread? t = null;
        if (reader)
        {
            t = new Thread(() =>
            {
                up.Set();
                int k = 0;
                while (Volatile.Read(ref stop) == 0) { Call(engine, which, ++k); reads++; }
            }) { IsBackground = true, Name = "probe-aggregate" };
            t.Start();
            up.Wait();
        }

        long worst = 0;
        long t0    = Stopwatch.GetTimestamp();
        for (int i = 0; i < items.Length; i += DrainBatch)
        {
            var  call = items.AsSpan(i, Math.Min(DrainBatch, items.Length - i));
            long c0   = Stopwatch.GetTimestamp();
            if (engine.WriteSpans(call) != call.Length) throw new InvalidOperationException("refused");
            worst = Math.Max(worst, Stopwatch.GetTimestamp() - c0);
        }
        long ticks = Stopwatch.GetTimestamp() - t0;
        Volatile.Write(ref stop, 1);
        t?.Join();
        return (ticks * 1e9 / Stopwatch.Frequency / ingest, Us(worst), reads);
    }

    /// <summary>Spans <paramref name="first"/> .. first+count-1: ten to a trace, the SqlClient attribute blob, one service unless <paramref name="services"/> is 3: then the root is "gateway" and its children alternate "billing" and "ledger", every seventh an error.</summary>
    internal static SpanIngestItem[] Corpus(int first, int count, int services = 1)
    {
        long baseNano = Base.ToUnixTimeMilliseconds() * 1_000_000L;
        var items = new SpanIngestItem[count];
        for (int k = 0; k < count; k++)
        {
            int i = first + k;
            items[k] = new SpanIngestItem
            {
                TraceId           = new TraceId(0x9E3779B97F4A7C15UL, (ulong)(i / SpansPerTrace + 1)),
                SpanId            = new SpanId((ulong)(i + 1)),
                ParentSpanId      = i % SpansPerTrace == 0 ? default : new SpanId((ulong)(i / SpansPerTrace * SpansPerTrace + 1)),
                StartTimeUnixNano = baseNano + i * 1_000_000L,
                DurationNanos     = 1_000_000L * (1 + i % 2000),
                Name              = "SELECT payments",
                ServiceName       = services == 1 ? "billing" : i % SpansPerTrace == 0 ? "gateway" : i % 2 == 0 ? "billing" : "ledger",
                Kind              = SpanKind.Client,
                Status            = services != 1 && i % 7 == 0 ? SpanStatusCode.Error : SpanStatusCode.Unset,
                HttpStatusCode    = 0,
                AttributesBytes   = TraceHotTierProbe.SqlClientBlob(i),
            };
        }
        return items;
    }
}

/// <summary>
/// TS#6, PINNED. The four aggregate reads take the read lock only to capture what they will read —
/// the cold array and the unflushed spans as two runs of immutable records — and do their passes
/// after releasing it. Judged at a seam INSIDE each pass: from there a writer must be able to take
/// the write lock at once, and whatever the writer and a flush then do, the pass must count exactly
/// the spans it captured. Plus the memo, and the allocation the pass no longer makes per span.
/// </summary>
public sealed class TraceAggregateLockTests : IDisposable
{
    private static readonly DateTimeOffset Base = new(2026, 8, 1, 12, 0, 0, TimeSpan.Zero);
    private static readonly DateTimeOffset From = Base.AddMinutes(-1);
    private static readonly DateTimeOffset To   = Base.AddDays(1);
    private static readonly TimeSpan HangGuard  = TimeSpan.FromSeconds(30);

    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-agglock-" + Guid.NewGuid().ToString("N"));

    public TraceAggregateLockTests() => Directory.CreateDirectory(_dir);
    public void Dispose() { try { Directory.Delete(_dir, true); } catch { } }

    private TraceStorageEngine NewEngine(int spans)
    {
        var engine = new TraceStorageEngine(_dir, NullLogger<TraceStorageEngine>.Instance);
        Assert.Equal(spans, engine.WriteSpans(TraceAggregateLockProbe.Corpus(0, spans, services: 3)));
        return engine;
    }

    private static long SpanTotal(IReadOnlyList<ServiceSegmentStats> stats)
    {
        long n = 0;
        foreach (var s in stats) n += s.SpanCount;
        return n;
    }

    /// <summary>
    /// A WRITER GETS THE LOCK WHILE AN AGGREGATE IS MID-PASS, and the pass is not disturbed by it.
    /// Each aggregate is parked inside its pass; from there another thread must take the engine's
    /// write lock without waiting (TryEnterWriteLock(0): no timer decides it), then 300 more spans
    /// are ingested and the tier is flushed into a segment underneath the parked pass. Released, the
    /// pass must report exactly what it captured: the appends landed past the captured runs, and the
    /// flush swapped the list rather than clearing it. With the pass back inside the read lock the
    /// writer cannot get in.
    /// </summary>
    [Theory]
    [InlineData("GetAggregateStatsAsync")]
    [InlineData("GetServiceGraphAsync")]
    [InlineData("GetTraceVolumeAsync")]
    [InlineData("GetTraceListAsync")]
    public async Task An_aggregate_pass_runs_with_the_lock_free_and_counts_what_it_captured(string aggregate)
    {
        using var engine = NewEngine(1_000);
        engine._aggregateMemoTicks = 0;

        using var parked  = new ManualResetEventSlim();
        using var release = new ManualResetEventSlim();
        bool readLockHeldInPass = true;
        engine._aggregatePassForTest = name =>
        {
            if (name != aggregate) return;
            readLockHeldInPass = engine.LockForTest.IsReadLockHeld;
            parked.Set();
            release.Wait(HangGuard);
        };

        Task<long> call = aggregate switch
        {
            "GetAggregateStatsAsync" => Task.Run(async () => SpanTotal(await engine.GetAggregateStatsAsync(From, To))),
            "GetServiceGraphAsync"   => Task.Run(async () => (long)(await engine.GetServiceGraphAsync(From, To)).Edges.Length),
            "GetTraceVolumeAsync"    => Task.Run(async () => (long)(await engine.GetTraceVolumeAsync(From, To, 20)).TotalTraces),
            _                        => Task.Run(async () => (long)(await engine.GetTraceListAsync(From, To, null, null, null, null, null, 1_000)).Rows.Count),
        };
        Assert.True(parked.Wait(HangGuard), $"hang guard: {aggregate} never reached its pass");

        bool writerGotIn = false;
        var writer = new Thread(() =>
        {
            if (engine.LockForTest.TryEnterWriteLock(0)) { writerGotIn = true; engine.LockForTest.ExitWriteLock(); }
        });
        writer.Start();
        writer.Join();

        // Ingest and a whole flush while the pass is parked; both need the write lock.
        if (writerGotIn)
        {
            Assert.Equal(300, engine.WriteSpans(TraceAggregateLockProbe.Corpus(1_000, 300, services: 3)));
            engine.FlushHotTier();
        }
        release.Set();
        long result = await call.WaitAsync(HangGuard);

        Assert.False(readLockHeldInPass, $"{aggregate} ran its pass holding the read lock");
        Assert.True(writerGotIn, $"a writer could not take the lock while {aggregate} was mid-pass");
        long expected = aggregate switch
        {
            "GetAggregateStatsAsync" => 1_000,
            "GetServiceGraphAsync"   => 2,                       // gateway->billing, gateway->ledger
            _                        => 100,                     // traces: ten spans each
        };
        Assert.Equal(expected, result);

        // And after the flush the same window sees everything, once: 1 300 spans, cold and hot.
        engine._aggregatePassForTest = null;
        Assert.Equal(1_300, SpanTotal(await engine.GetAggregateStatsAsync(From, To)));
    }

    /// <summary>
    /// A REPEATED AGGREGATE IS SERVED FROM THE MEMO, AND NOT ONE SPAN LATER. The same window twice is
    /// the same result object; one more span, or a flush, is a new key and a recount; an expired memo
    /// recounts. A memo keyed on the window alone fails the second half, none at all the first.
    /// </summary>
    [Fact]
    public async Task A_repeated_aggregate_is_memoised_until_anything_it_read_changes()
    {
        using var engine = NewEngine(1_000);
        engine._aggregateMemoTicks = System.Diagnostics.Stopwatch.Frequency * 60;   // no expiry inside the test

        var first  = await engine.GetAggregateStatsAsync(From, To);
        var second = await engine.GetAggregateStatsAsync(From, To);
        Assert.Same(first, second);
        var graph1 = await engine.GetServiceGraphAsync(From, To);
        Assert.Same(graph1, await engine.GetServiceGraphAsync(From, To));

        Assert.Equal(1, engine.WriteSpans(TraceAggregateLockProbe.Corpus(1_000, 1, services: 3)));
        var afterWrite = await engine.GetAggregateStatsAsync(From, To);
        Assert.NotSame(first, afterWrite);
        Assert.Equal(1_001, SpanTotal(afterWrite));
        Assert.NotSame(graph1, await engine.GetServiceGraphAsync(From, To));

        engine.FlushHotTier();
        var afterFlush = await engine.GetAggregateStatsAsync(From, To);
        Assert.NotSame(afterWrite, afterFlush);
        Assert.Equal(1_001, SpanTotal(afterFlush));                               // now from the sidecar

        engine._aggregateMemoTicks = 0;
        Assert.NotSame(afterFlush, await engine.GetAggregateStatsAsync(From, To));
    }

    /// <summary>
    /// THE STATS PASS ALLOCATES PER SERVICE, NOT PER SPAN. 10 000 hot spans in three services: the
    /// pass used to allocate a uint[19] (100 B) for every one of them, about a megabyte a call,
    /// inside the read lock. Now it is three accumulators and a result list. Counted on this thread
    /// only, and with the memo off so the pass really runs.
    /// </summary>
    [Fact]
    public async Task The_stats_pass_allocates_per_service_not_per_span()
    {
        using var engine = NewEngine(10_000);
        engine._aggregateMemoTicks = 0;
        await engine.GetAggregateStatsAsync(From, To);                              // warm

#pragma warning disable xUnit1031 // synchronous ON PURPOSE: the per-thread counter must see the whole call
        long a0 = GC.GetAllocatedBytesForCurrentThread();
        var stats = engine.GetAggregateStatsAsync(From, To).GetAwaiter().GetResult();
#pragma warning restore xUnit1031
        long allocated = GC.GetAllocatedBytesForCurrentThread() - a0;

        Assert.Equal(10_000, SpanTotal(stats));

        // The same histogram HistogramBuckets.IndexOf would have built, span by span.
        var want = new uint[HistogramBuckets.Count];
        foreach (var s in TraceAggregateLockProbe.Corpus(0, 10_000, services: 3)) want[HistogramBuckets.IndexOf(s.DurationNanos)]++;
        var got = new uint[HistogramBuckets.Count];
        foreach (var s in stats) for (int i = 0; i < got.Length; i++) got[i] += s.Buckets[i];
        Assert.Equal(want, got);
        Assert.True(allocated < 16 * 1024, $"the stats pass allocated {allocated:N0} B over 10 000 spans");
    }
}
