using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging.Abstractions;
using Ameto.Tracing;
using Ameto.Tracing.Storage;
using Xunit.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// <c>TraceIndexStore.Lookup</c> keeps its two working lists on the calling thread instead of
/// allocating them per lookup (TS#13) — and that must cost the reader refcount nothing.
///
/// <para>The refcount (<c>TryAcquire</c> / <c>Release</c> / <c>Retire</c>) is the only thing that
/// keeps a reader's bloom — native memory — alive under a lookup. A pooled list can break it in
/// two ways a fresh list never could: by KEEPING its references past the lookup, which pins a
/// retired reader's managed object for the thread's life; and by keeping its COUNT, which hands
/// the next lookup on the thread a reader it never acquired — possibly freed, answering
/// <c>Unreadable</c>, and dropping every segment it covers to a full scan. The seam test below
/// retires a run at the one moment the list holds it and checks both, plus that the hold still
/// did its job. No sleeps, no second thread: the seam is the interleaving.</para>
/// </summary>
public sealed class TraceIndexLookupPoolTests : IDisposable
{
    private readonly string            _dir;
    private readonly ITestOutputHelper _out;

    public TraceIndexLookupPoolTests(ITestOutputHelper output)
    {
        _out = output;
        _dir = Path.Combine(Path.GetTempPath(), "ameto-tixpool-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_dir);
    }

    public void Dispose() { try { Directory.Delete(_dir, true); } catch { } }

    private static TraceId Id(int i) => new(
        unchecked((ulong)(i * 6364136223846793005L + 1442695040888963407L)),
        unchecked((ulong)(i * 2862933555777941757L + 3037000493L)));

    /// <summary>A run big enough that a lookup crosses a block read, for <paramref name="segment"/>.</summary>
    private TraceIndexRun WriteRun(string name, ulong segment, int entries = 10_000)
    {
        var w = new TraceIndexWriter();
        for (int i = 0; i < entries; i++) w.Add(Id(i), segment, [(uint)i, (uint)i + 1, (uint)i + 2]);
        return w.Write(Path.Combine(_dir, name), level: 1, coveredSegments: [segment]);
    }

    // ── The seam test ─────────────────────────────────────────────────────────

    /// <summary>
    /// Runs one lookup during which the run is dropped from the store — at its block read, while
    /// the lookup's pinned-reader list holds it — and hands back only a weak reference, so nothing
    /// in THIS frame keeps the reader alive afterwards (a Debug JIT stretches locals to the end of
    /// their method, which is why this is a method of its own).
    /// </summary>
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static (WeakReference Reader, int SeamFired, TraceIndexAnswer Answer, bool FreedAfter)
        LookupWhileTheRunIsRetired(TraceIndexStore store, string path, TraceId id)
    {
        var reader = store.ReaderForTest(path);
        Assert.NotNull(reader);

        int fired = 0;
        reader._beforeBlockReadForTest = _ =>
        {
            if (fired++ > 0) return;
            store.Remove([path]);   // what a merge or Sync does to a run: out of the map, Retire()
        };

        var answer = store.Lookup(id);

        // The hold did its job: the reader answered on live memory, and the lookup's Release — not
        // the Retire — is what freed it. A freed reader refuses holds and answers Unreadable.
        var probe = new List<TraceIndexHit>();
        bool freed = !reader.TryAcquire()
                  && reader.Lookup(TraceIndexFileTestsAccess.Key(id), probe) == TraceIndexOutcome.Unreadable;

        return (new WeakReference(reader), fired, answer, freed);
    }

    [Fact]
    public void A_run_retired_while_the_pooled_list_holds_it_is_freed_unreachable_and_never_handed_on()
    {
        var store = new TraceIndexStore(NullLogger.Instance);
        var first = WriteRun("pooled-first.tix", segment: 1);
        Assert.True(store.Add(first));

        var (weak, fired, answer, freedAfter) = LookupWhileTheRunIsRetired(store, first.FilePath, Id(4_242));

        _out.WriteLine($"seam fired {fired}x; answer: {answer.Hits.Count} hit(s), "
                     + $"unanswerable {answer.Unanswerable?.Count ?? 0}; freed after the lookup: {freedAfter}");
        Assert.True(fired > 0, "the seam never fired — the lookup read no block");
        Assert.Single(answer.Hits);
        Assert.Equal(3, answer.Hits[0].Offsets.Length);
        Assert.Null(answer.Unanswerable);
        Assert.True(freedAfter, "the retired reader was not freed by the lookup's Release");

        // UNREACHABLE: nothing may still point at the reader — not the store, not the lookup's list.
        // A list returned to the thread uncleared keeps it for as long as the thread lives.
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        Assert.False(weak.IsAlive,
            "a retired reader is still reachable after the lookup that held it returned — the lookup's "
            + "pooled list kept its reference");

        // NEVER HANDED ON: the next lookup on this thread asks only the runs the store has now. A list
        // put back with its entries would ask the freed reader too, which answers Unreadable and
        // un-covers its segment for a request that has nothing to do with it.
        var second = WriteRun("pooled-second.tix", segment: 2);
        Assert.True(store.Add(second));

        var next = store.Lookup(Id(4_242));
        _out.WriteLine($"next lookup: {next.Hits.Count} hit(s) in segment(s) "
                     + $"{string.Join(",", next.Hits.Select(h => h.SegmentId))}, unanswerable "
                     + $"{(next.Unanswerable is null ? "none" : string.Join(",", next.Unanswerable))}");
        Assert.Null(next.Unanswerable);
        Assert.Single(next.Hits);
        Assert.Equal(2UL, next.Hits[0].SegmentId);

        store.Dispose();
    }

    // ── The allocation gate ───────────────────────────────────────────────────

    [Fact]
    public void A_bloom_miss_lookup_allocates_nothing()
    {
        var store = new TraceIndexStore(NullLogger.Instance);
        var runs  = new[] { WriteRun("miss-a.tix", 1), WriteRun("miss-b.tix", 2), WriteRun("miss-c.tix", 3) };
        foreach (var r in runs) Assert.True(store.Add(r));
        var readers = runs.Select(r => store.ReaderForTest(r.FilePath)!).ToArray();

        // Ids EVERY run's bloom rejects, so the lookup reads no block: the path this item is about.
        var misses = new List<TraceId>();
        for (int i = 1_000_000; misses.Count < 200; i++)
            if (readers.All(r => !r.MightContain(TraceIndexFileTestsAccess.Key(Id(i)))))
                misses.Add(Id(i));
        var ids = misses.ToArray();

        for (int w = 0; w < 50; w++) foreach (var id in ids) store.Lookup(id);   // warm, and seed the pool

        long best = long.MaxValue;
        for (int pass = 0; pass < 5; pass++)
        {
            long a0 = GC.GetAllocatedBytesForCurrentThread();
            foreach (var id in ids)
            {
                var a = store.Lookup(id);
                if (a.Hits.Count != 0 || a.Unanswerable is not null) Assert.Fail("a bloom miss answered something");
            }
            best = Math.Min(best, GC.GetAllocatedBytesForCurrentThread() - a0);
        }

        // For the record, not gated: what a HIT costs — the answer's array and the offsets it holds.
        long h0  = GC.GetAllocatedBytesForCurrentThread();
        var  hit = store.Lookup(Id(4_242));
        long hitBytes = GC.GetAllocatedBytesForCurrentThread() - h0;

        _out.WriteLine($"bloom-miss lookups over {runs.Length} runs: {best:N0} B for {ids.Length} "
                     + $"({(double)best / ids.Length:N1} B/lookup, best of 5); a hit in all {hit.Hits.Count} "
                     + $"runs: {hitBytes:N0} B");

        // Before TS#13: a List<TraceIndexReader> sized by the run count and a List<TraceIndexHit> of
        // two on every lookup — 168 B per lookup over three runs, 33 600 B for these 200 (measured).
        Assert.True(best < ids.Length * 8,
            $"{ids.Length} bloom-miss lookups allocated {best:N0} B — the lookup is building its "
            + "working lists per call again");
        store.Dispose();
    }
}
