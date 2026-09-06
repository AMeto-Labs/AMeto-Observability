using Microsoft.Extensions.Logging.Abstractions;
using Ameto.Tracing;
using Ameto.Tracing.Storage;
using Xunit.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// A READER IS NOT FREED WHILE A LOOKUP IS INSIDE IT — the property the refcount was added for,
/// and the one no single-threaded test can express.
///
/// <para>The finding was that the store freed dropped readers immediately while a lookup held a
/// plain reference across bloom probes and a 4 KB read. The bloom lives in <c>NativeMemory</c>, so
/// the reachable outcomes were a read of freed memory and an <c>ObjectDisposedException</c>
/// surfacing as a 500 — not a stale answer.</para>
///
/// <para>WHY THE EXISTING TESTS DID NOT COVER IT, stated because it is the interesting part:
/// <c>TryAcquire</c> and <c>Release</c> run on every indexed lookup and <c>Retire</c> on every
/// retention and compaction test, so a crudely broken refcount would have been caught. What no
/// existing test could see is the ordering — <c>Retire</c> reverting to an unconditional
/// <c>Dispose</c> keeps every counter moving, keeps all 546 tests green, and puts the race back
/// exactly as it was found. So these tests interleave deliberately, through a seam, with no
/// sleeping and no chance of a flake.</para>
/// </summary>
public sealed class TraceIndexLifetimeTests : IDisposable
{
    private readonly string            _dir;
    private readonly ITestOutputHelper _out;

    public TraceIndexLifetimeTests(ITestOutputHelper output)
    {
        _out = output;
        _dir = Path.Combine(Path.GetTempPath(), "ameto-life-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_dir);
    }

    public void Dispose() { try { Directory.Delete(_dir, true); } catch { } }

    private string Path_(string n) => Path.Combine(_dir, n);

    private static TraceId Id(int i) => new(
        unchecked((ulong)(i * 6364136223846793005L + 1442695040888963407L)),
        unchecked((ulong)(i * 2862933555777941757L + 3037000493L)));

    private static uint[] Offsets(int i, int n) => [.. Enumerable.Range(0, n).Select(k => (uint)(i * n + k))];

    /// <summary>A run big enough that a lookup crosses more than one block.</summary>
    private TraceIndexRun WriteRun(string name, int entries = 10_000)
    {
        var w = new TraceIndexWriter();
        for (int i = 0; i < entries; i++) w.Add(Id(i), 1, Offsets(i, 3));
        return w.Write(Path_(name), level: 1, coveredSegments: [1]);
    }

    [Fact]
    public void A_run_retired_mid_lookup_still_answers_on_live_memory()
    {
        // THE RACE, MADE DETERMINISTIC. The store drops the run at the exact instant the lookup is
        // between its bloom probe and its block read — the window that used to end in a read of
        // freed native memory. With the refcount the reader stays alive until the hold goes.
        var run = WriteRun("retire-mid.tix");

        var store = new TraceIndexStore(NullLogger.Instance);
        Assert.True(store.Add(run));

        var reader = TraceIndexReader.Open(run.FilePath);
        Assert.NotNull(reader);
        Assert.True(reader.TryAcquire(), "a live reader refused a hold");

        int retiredAtBlock = -1;
        reader._beforeBlockReadForTest = b =>
        {
            if (retiredAtBlock >= 0) return;
            retiredAtBlock = b;
            // What the store does when a merge takes this run away, or when Sync stops naming it.
            reader.Retire();
        };

        var hits = new List<TraceIndexHit>();
        var outcome = reader.Lookup(TraceIndexFileTestsAccess.Key(Id(4_242)), hits);

        _out.WriteLine($"retired during block {retiredAtBlock} → {outcome}, {hits.Count} hit(s)");
        Assert.True(retiredAtBlock >= 0, "the seam never fired — the lookup read no block");
        Assert.Equal(TraceIndexOutcome.Found, outcome);
        Assert.Single(hits);
        Assert.Equal(3, hits[0].Offsets.Length);

        // The hold is what kept it alive; giving it back is what frees it.
        reader.Release();
        Assert.False(reader.TryAcquire(), "a freed reader handed out a new hold");
        store.Dispose();
    }

    [Fact]
    public void The_last_release_is_what_frees_and_a_second_hold_is_refused_after_it()
    {
        // The counter itself, stated as behaviour rather than read off the field. Two holds, one
        // retire, one release — still alive; second release — gone.
        var run = WriteRun("counting.tix", entries: 2_000);
        var r = TraceIndexReader.Open(run.FilePath);
        Assert.NotNull(r);

        Assert.True(r.TryAcquire());
        Assert.True(r.TryAcquire());
        r.Retire();

        // Retired but held: no new holds, and the ones outstanding still work.
        Assert.False(r.TryAcquire(), "a retired reader handed out a fresh hold");
        var hits = new List<TraceIndexHit>();
        Assert.Equal(TraceIndexOutcome.Found, r.Lookup(TraceIndexFileTestsAccess.Key(Id(7)), hits));

        r.Release();
        hits.Clear();
        _out.WriteLine("one hold left after retire: still answering");
        Assert.Equal(TraceIndexOutcome.Found, r.Lookup(TraceIndexFileTestsAccess.Key(Id(7)), hits));

        r.Release();   // the last one — frees here
        hits.Clear();
        var after = r.Lookup(TraceIndexFileTestsAccess.Key(Id(7)), hits);
        _out.WriteLine($"after the last release: {after}");
        Assert.Equal(TraceIndexOutcome.Unreadable, after);   // never NotPresent
    }

    [Fact]
    public void A_run_dropped_with_deleteFiles_keeps_its_file_until_the_last_hold_goes()
    {
        // THE COMBINATION BUG. Refcounting deliberately keeps a retired reader alive for lookups
        // already inside it — but the reader holds no handle, it REOPENS the file for every block.
        // So unlinking the .tix right after dropping it cancelled exactly that overlap, and the
        // resulting FileNotFoundException is now Unreadable, which makes the store treat EVERY
        // segment the run covered as unanswerable. For an L3 merge that is a thousand segments
        // dropping to a full scan because a file was deleted on purpose a microsecond earlier.
        var run = WriteRun("deferred-delete.tix");

        var store = new TraceIndexStore(NullLogger.Instance);
        Assert.True(store.Add(run));

        // THE STORE'S OWN READER, not a second one opened beside it. Opening the file again gives
        // an instance with its own refcount that the store has never heard of — the first draft of
        // this test did exactly that and "failed", because nothing the store did could possibly
        // respect a hold it did not know existed. The lookup path takes its hold inside the store,
        // so that is the instance the seam has to sit on.
        var reader = store.ReaderForTest(run.FilePath);
        Assert.NotNull(reader);

        bool fileGoneMidLookup = false;
        int  seamFired = 0;
        reader._beforeBlockReadForTest = _ =>
        {
            if (seamFired++ > 0) return;
            store.Remove([run.FilePath], deleteFiles: true);        // what index compaction does
            fileGoneMidLookup |= !File.Exists(run.FilePath);
        };

        var answer = store.Lookup(Id(4_242));

        _out.WriteLine($"file still present while the lookup ran: {!fileGoneMidLookup}; "
                     + $"{answer.Hits.Count} hit(s), unanswerable {answer.Unanswerable?.Count ?? 0}");
        Assert.True(seamFired > 0, "the seam never fired — the lookup read no block");
        Assert.False(fileGoneMidLookup, "the .tix was unlinked while a lookup was still reading it");
        Assert.Single(answer.Hits);
        Assert.Null(answer.Unanswerable);   // nothing dropped to a full scan

        // And it does go, once the lookup's hold is gone — deferred, not leaked.
        Assert.False(File.Exists(run.FilePath), "the run was never deleted");
        store.Dispose();
    }

    [Fact]
    public void A_merge_batch_stops_at_the_entry_cap_not_only_at_the_run_count()
    {
        // MaxEntriesPerMerge, which nothing asserted. Merge holds every surviving entry in one
        // writer until it sorts, so the batch SIZE is the peak — and the engine cannot reach two
        // million entries in a test, which makes calling the selector directly the only practical
        // way to pin the constant. Left unpinned it is the first thing somebody raises when
        // merging looks slow.
        const int cap = TraceIndexCompactor.MaxEntriesPerMerge;

        // Ten runs at one level — enough to merge — each a fifth of the cap, so the batch has to
        // stop at five however many are on offer.
        var runs = new List<TraceIndexRun>();
        for (int i = 0; i < 10; i++)
            runs.Add(new TraceIndexRun(1, $"L1-{i}.tix", 0x1000, 0x9000, cap / 5, [(ulong)(i + 1)]));

        var batch = TraceIndexCompactor.SelectMergeBatch(runs);
        long entries = batch.Sum(r => (long)r.EntryCount);

        _out.WriteLine($"10 runs of {cap / 5:N0} entries → batch of {batch.Count}, {entries:N0} entries "
                     + $"(cap {cap:N0})");
        Assert.True(batch.Count >= 2, "the level was full and nothing was selected");
        Assert.True(entries <= cap, $"a batch of {entries:N0} entries is past the {cap:N0} cap");
        Assert.Equal(5, batch.Count);
    }

    [Fact]
    public void Two_runs_are_taken_even_when_the_pair_alone_exceeds_the_cap()
    {
        // The deliberate exception, which matters more than the cap: a merge of one is not a merge,
        // so refusing outright would wedge the level forever and the run count would grow without
        // bound — the exact failure the compactor exists to prevent.
        const int cap = TraceIndexCompactor.MaxEntriesPerMerge;

        var runs = new List<TraceIndexRun>();
        for (int i = 0; i < 10; i++)
            runs.Add(new TraceIndexRun(1, $"L1-big-{i}.tix", 0x1000, 0x9000, cap, [(ulong)(i + 1)]));

        var batch = TraceIndexCompactor.SelectMergeBatch(runs);
        _out.WriteLine($"10 runs of {cap:N0} entries each → batch of {batch.Count}");
        Assert.Equal(2, batch.Count);
    }
}
