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

    /// <summary>Spans <paramref name="first"/> .. first+count-1: ten to a trace, the SqlClient attribute blob, one service.</summary>
    private static SpanIngestItem[] Corpus(int first, int count)
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
                ServiceName       = "billing",
                Kind              = SpanKind.Client,
                Status            = SpanStatusCode.Unset,
                HttpStatusCode    = 0,
                AttributesBytes   = TraceHotTierProbe.SqlClientBlob(i),
            };
        }
        return items;
    }
}
