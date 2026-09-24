using Microsoft.Extensions.Logging;

namespace Ameto.Tracing.Storage;

/// <summary>
/// What the open runs, together, could say about one trace.
///
/// <para><see cref="Unanswerable"/> is the half that keeps the coverage rule honest: those
/// segments are covered on paper but no run managed to read for them just now, so for THIS request
/// they must be treated as uncovered and read. Without it a torn block or a file locked for a
/// moment reads as "the trace is not in that segment".</para>
/// </summary>
/// <param name="AnsweredFor">
/// Segments some run ACTUALLY ANSWERED FOR in this lookup — acquired, read, and returning either
/// <c>Found</c> or <c>NotPresent</c>.
///
/// <para>THIS IS THE PROOF, AND IT COMES FROM THE SAME INSTANT AS THE HITS. Without it a skip was
/// inferred from the mere ABSENCE of a hit, checked against a coverage set sampled at a different
/// moment — which meant the decision rested on an argument about ordering rather than on a fact.
/// <see cref="Unanswerable"/> only speaks for a run that was asked and failed; a run already gone
/// from the store is not asked at all, so it cannot report that it could not answer, and its
/// segments looked exactly like segments a healthy run had cleared.</para>
///
/// <para>Null means no run answered for anything, which is the same as an empty set and saves the
/// allocation on the common path where the index is not in use.</para>
/// </param>
internal readonly record struct TraceIndexAnswer(
    TraceIndexHits      Hits,
    HashSet<ulong>?     Unanswerable,
    HashSet<ulong>?     AnsweredFor);

/// <summary>
/// The hits of one lookup: read-only, and EMPTY rather than null by default — so a caller that
/// holds a <c>default(TraceIndexAnswer)</c> (the index switched off) can walk it like any other.
///
/// <para>A struct over an exact-size array rather than the <c>List</c> it replaced, which every
/// lookup used to allocate — a bloom miss included — to hand back nothing. A miss now hands back
/// <c>default</c>, a hit the one array the answer is made of. Read-only because an empty answer
/// is no longer a fresh object: anything a caller could append to would have to be.</para>
/// </summary>
internal readonly struct TraceIndexHits : IReadOnlyList<TraceIndexHit>
{
    private readonly TraceIndexHit[]? _hits;

    public TraceIndexHits(TraceIndexHit[] hits) => _hits = hits;

    public int Count => _hits?.Length ?? 0;

    public TraceIndexHit this[int index] => (_hits ?? [])[index];

    /// <summary>The pattern <c>foreach</c> binds to: a plain struct, no allocation, no ref struct.</summary>
    public Enumerator GetEnumerator() => new(_hits);

    IEnumerator<TraceIndexHit> IEnumerable<TraceIndexHit>.GetEnumerator() =>
        ((IEnumerable<TraceIndexHit>)(_hits ?? [])).GetEnumerator();

    System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() =>
        ((IEnumerable<TraceIndexHit>)this).GetEnumerator();

    public struct Enumerator(TraceIndexHit[]? hits)
    {
        private int _i = -1;

        public bool MoveNext() => hits is not null && ++_i < hits.Length;

        public readonly TraceIndexHit Current => hits![_i];
    }
}

/// <summary>
/// THE OPEN RUNS, AND THE ONE QUESTION THE READ PATH ASKS THEM.
///
/// <para>Holds a <see cref="TraceIndexReader"/> per run named by the manifest — which is to say it
/// holds each run's bloom filter and sparse block index in memory and nothing else. A lookup asks
/// every open run; the ones whose bloom says no cost no disk at all, and the rest cost one block
/// each.</para>
///
/// <para>IT NEVER DECIDES WHETHER A SEGMENT MAY BE SKIPPED. That decision needs two facts and this
/// class only has one: it can say "no run names this trace", but whether that counts as proof
/// depends on whether the manifest says the run covers the segment in question. Keeping the two
/// apart is deliberate — a store that answered "not present" on its own would be one refactor away
/// from being believed about a segment nothing had indexed. See
/// <c>TraceStorageEngine.GetTraceAsync</c>, where the two are put together in one place.</para>
///
/// <para>A run that will not open is simply absent from the store, and a segment whose run is
/// absent is not covered — the manifest is corrected to say so. That is the whole error handling:
/// there is no failure mode here that is not "this segment is read the way it was read before the
/// index existed".</para>
/// </summary>
internal sealed class TraceIndexStore : IDisposable
{
    private readonly ILogger _logger;
    private readonly System.Threading.Lock _gate = new();

    /// <summary>Open runs by their file path. Replaced wholesale, never mutated in place.</summary>
    private volatile Dictionary<string, TraceIndexReader> _open = new(StringComparer.Ordinal);

    /// <summary>
    /// Every segment the currently open runs cover — rebuilt whenever <see cref="_open"/> is
    /// replaced, and handed out by reference to each lookup.
    ///
    /// <para>ON THE MUTATION, NOT ON THE LOOKUP, and that is the whole reason it is a field. A
    /// lookup needs this set to say which segments it actually answered for, and building it there
    /// put a fresh <c>HashSet&lt;ulong&gt;</c> sized by the number of covered cold segments on the
    /// SUCCESS path of every trace lookup — a bloom miss is <c>NotPresent</c>, not
    /// <c>Unreadable</c>, so unlike the unanswerable set it was allocated whether or not anything
    /// went wrong. Runs change on a flush, a merge or a retention pass; lookups happen per request.
    /// Building it on the rare side costs nothing and the replaced check it stands in for was one
    /// hash probe with no allocation at all.</para>
    ///
    /// <para>Published under the same lock and in the same statement as <see cref="_open"/>, so a
    /// lookup that pinned a set of readers is holding the set that describes exactly those.</para>
    /// </summary>
    private volatile HashSet<ulong> _coveredByOpen = new();

    public TraceIndexStore(ILogger logger) => _logger = logger;

    /// <summary>Runs currently open, and the bytes they keep alive.</summary>
    public (int Runs, long RetainedBytes) Stats
    {
        get
        {
            var open = _open;
            long bytes = 0;
            foreach (var r in open.Values) bytes += r.RetainedBytes;
            return (open.Count, bytes);
        }
    }

    /// <summary>
    /// Opens every run the manifest names, and drops any that are no longer named. Returns the
    /// segments whose runs could NOT be opened, so the caller can withdraw their coverage — a
    /// claim the index cannot back must not survive the process that discovered it.
    /// </summary>
    public IReadOnlyList<string> Sync(TraceManifest manifest)
    {
        var wanted  = manifest.Runs;
        var unusable = new List<string>();

        lock (_gate)
        {
            var next  = new Dictionary<string, TraceIndexReader>(wanted.Count, StringComparer.Ordinal);
            var stale = new List<TraceIndexReader>();

            foreach (var run in wanted)
            {
                if (next.ContainsKey(run.FilePath)) continue;

                if (_open.TryGetValue(run.FilePath, out var already))
                {
                    next[run.FilePath] = already;
                    continue;
                }

                var opened = TraceIndexReader.Open(run.FilePath);
                if (opened is not null) opened.CoveredSegments = run.CoveredSegments;
                if (opened is null)
                {
                    // THE RUN, NOT ITS SEGMENTS. Reporting segment ids let the caller withdraw a
                    // merged run's claim one segment at a time and never remove the run itself —
                    // see TraceManifest.DropRuns.
                    unusable.Add(run.FilePath);
                    _logger.LogWarning(
                        "Trace index run {Path} could not be opened — the segment(s) it covered fall "
                      + "back to the full scan", run.FilePath);
                    continue;
                }
                next[run.FilePath] = opened;
            }

            foreach (var (path, reader) in _open)
                if (!next.ContainsKey(path)) stale.Add(reader);

            Publish(next);
            foreach (var r in stale) r.Retire();
        }

        return unusable;
    }

    /// <summary>
    /// Registers a run written just now, without re-opening everything else.
    ///
    /// <para>RETURNS FALSE WHEN IT WILL NOT OPEN, and every caller has to act on that. Coverage is
    /// the claim that lets a segment be skipped; making it and then failing to open the run behind
    /// it leaves the manifest saying "covered" and this store holding nothing, and a lookup racing
    /// that state finds hits, finds coverage, finds no offsets for the segment, and omits its
    /// spans. It heals only at the next restart, when <see cref="Sync"/> reports the run unusable —
    /// so a long-lived process under-reports until somebody bounces it.</para>
    ///
    /// <para>The trigger is mundane on Windows: an antivirus or backup agent holding a handle on
    /// the just-renamed <c>.tix</c> is a sharing violation inside <c>Open</c>, which catches
    /// everything and answers null. So the callers open FIRST and claim coverage only on success.
    /// </para>
    /// </summary>
    public bool Add(TraceIndexRun run)
    {
        var opened = TraceIndexReader.Open(run.FilePath);
        if (opened is not null) opened.CoveredSegments = run.CoveredSegments;
        if (opened is null)
        {
            _logger.LogWarning(
                "Trace index run {Path} was written but will not open — the segment(s) it would "
              + "have covered stay on the scanning path", run.FilePath);
            return false;
        }
        lock (_gate)
        {
            var next = CopyOpen();
            if (next.Remove(run.FilePath, out var old)) old.Retire();
            next[run.FilePath] = opened;
            Publish(next);
        }
        return true;
    }

    /// <summary>Closes and forgets runs by path. The files themselves are the caller's business.</summary>
    /// <param name="deleteFiles">
    /// Unlink each <c>.tix</c> as part of retiring it, rather than leaving that to the caller.
    ///
    /// <para>THE CALLER CANNOT SAFELY DELETE IT ITSELF once a lookup may be holding the reader,
    /// and the reason is not obvious: the reader keeps no handle, it REOPENS the file for every
    /// block it reads. So an unlink right after <c>Remove</c> — which is what index compaction did
    /// — cancels exactly the overlap the refcount exists to provide, and the resulting
    /// FileNotFoundException is now <c>Unreadable</c> rather than a swallowed "no", which drops
    /// every segment the run covered to a full scan. Deleting from the last release keeps both
    /// properties: no lookup ever meets a missing file, and nothing is left on disk.</para>
    /// </param>
    public void Remove(IEnumerable<string> paths, bool deleteFiles = false)
    {
        lock (_gate)
        {
            var next  = CopyOpen();
            var drop  = new List<TraceIndexReader>();
            var unopened = deleteFiles ? new List<string>() : null;
            foreach (var p in paths)
            {
                if (next.Remove(p, out var r)) drop.Add(r);
                else unopened?.Add(p);          // never opened here — nothing can be holding it
            }
            Publish(next);
            foreach (var r in drop) r.Retire(deleteFiles);
            if (unopened is null) return;
            foreach (var p in unopened)
            {
                try { if (File.Exists(p)) File.Delete(p); } catch { /* the startup sweep gets it */ }
            }
        }
    }

    /// <summary>
    /// Every segment any open run places this trace in. An empty result means "no run named it",
    /// which is NOT the same as "it is not there" — see the class docstring.
    /// </summary>
    public TraceIndexAnswer Lookup(TraceId traceId)
    {
        // THE TWO WORKING LISTS ARE THE THREAD'S, NOT THE LOOKUP'S — the pinned readers and the
        // hits being collected, which every lookup used to allocate afresh, a bloom miss included
        // (a List sized by the run count plus a List of two, ~150 B, on every trace detail and on
        // both passes of GetTraceAsync). Taken OUT of the slot for the length of the call, so a
        // lookup that re-entered on this thread would get fresh lists instead of ones in use.
        //
        // THEY NEVER CARRY A READER PAST THIS CALL, and that is the whole safety argument. The
        // refcount — TryAcquire / Release / Retire — is the only thing keeping a reader's bloom
        // (native memory) alive under a lookup; a pooled list that kept its references after
        // Release would pin retired readers' managed objects for the thread's life, and a list that
        // kept its COUNT would hand the next lookup on this thread a reader it never acquired —
        // one that may already be freed, whose Lookup answers Unreadable and drops every segment it
        // covers to a full scan. So the finally releases, then CLEARS (List.Clear on a reference
        // type zeroes the array), then puts back. TraceIndexLookupPoolTests retires a run through
        // the seam while the list holds it and checks all three: freed, unreachable, never asked.
        var held = t_held ?? new List<TraceIndexReader>();
        var hits = t_hits ?? new List<TraceIndexHit>();
        t_held = null;
        t_hits = null;

        HashSet<ulong>? unanswerable = null;
        try
        {
            HashSet<ulong> answeredFor;

            // PINNED UNDER THE GATE, so nothing can retire and free a reader between taking the
            // snapshot and using it. A lookup holds each reader across bloom probes and a 4 KB block
            // read — milliseconds — and the store used to dispose dropped readers immediately, whose
            // bloom is native memory.
            lock (_gate)
            {
                // ROOM FIRST, THEN HOLDS. A hold taken and then lost to an Add that grows the list
                // and throws (out of memory) is a reader the finally cannot see, so cannot Release:
                // its bloom — native memory — is never freed, nor its file deleted. The per-call
                // list this replaced was sized to the run count before the first TryAcquire; the
                // thread's pooled list is sized here, where a failure still holds nothing.
                var open = _open;
                held.EnsureCapacity(open.Count);
                foreach (var r in open.Values) if (r.TryAcquire()) held.Add(r);

                // TAKEN HERE, WITH THE READERS, AND NOT BUILT PER LOOKUP. This is the set the caller
                // uses as its proof, and it describes exactly the runs pinned on the line above,
                // because Publish assigns both in one statement under this lock. Unioning the runs'
                // CoveredSegments here instead allocated a HashSet on the SUCCESS path of every
                // lookup — a bloom miss is NotPresent, not Unreadable, so it happened whether or not
                // anything went wrong — where the check it replaced was one hash probe.
                answeredFor = _coveredByOpen;
            }

            ulong key = TraceIndexFile.KeyOf(traceId);
            foreach (var r in held)
            {
                if (r.Lookup(key, hits) != TraceIndexOutcome.Unreadable) continue;

                // A RUN THAT COULD NOT ANSWER UN-COVERS ITS SEGMENTS FOR THIS REQUEST. Silence
                // from a covered run is what lets the engine skip a segment, and this run proved
                // nothing — so the caller has to read those segments instead. Subtracting these
                // from AnsweredFor is the caller's job and it already does it: the skip requires
                // AnsweredFor AND not Unanswerable, so a run that failed takes its segments back
                // out of the proof.
                (unanswerable ??= new HashSet<ulong>()).UnionWith(r.CoveredSegments);
                _logger.LogDebug("Trace index run {Path} could not answer a lookup; the {Count} "
                              + "segment(s) it covers are read rather than skipped",
                              r.Path, r.CoveredSegments.Length);
            }

            // The answer's own array, exactly sized, and only when there is something in it.
            return new TraceIndexAnswer(
                hits.Count == 0 ? default : new TraceIndexHits(hits.ToArray()),
                unanswerable, answeredFor);
        }
        finally
        {
            foreach (var r in held) r.Release();
            held.Clear();
            hits.Clear();
            if (held.Capacity <= MaxPooledCapacity) t_held = held;
            if (hits.Capacity <= MaxPooledCapacity) t_hits = hits;
        }
    }

    /// <summary>The calling thread's working lists for <see cref="Lookup"/> — see there.</summary>
    [ThreadStatic] private static List<TraceIndexReader>? t_held;
    [ThreadStatic] private static List<TraceIndexHit>?    t_hits;

    /// <summary>
    /// A list grown past this by one unusual lookup (thousands of runs, a trace id shared by
    /// thousands of index entries) is dropped rather than kept for the thread's life.
    /// </summary>
    private const int MaxPooledCapacity = 256;

    /// <summary>True when any run is open at all — the read path skips its work entirely if not.</summary>
    public bool HasRuns => _open.Count > 0;

    /// <summary>
    /// A copy of the open map, named rather than spelled out at each call site — see the same
    /// helpers on TraceManifest.State: "new Dictionary<K,V>(collection)" and
    /// "new Dictionary<K,V>(count)" are one shape to the convention scanner and opposites in
    /// fact, and one arguable claim beats several unarguable ones.
    /// </summary>
    private Dictionary<string, TraceIndexReader> CopyOpen() => new(_open, StringComparer.Ordinal);

    /// <summary>
    /// Publishes a new set of open runs together with the segment set derived from it. The only
    /// place <see cref="_open"/> is assigned, so the two can never disagree — the derived set is
    /// what a lookup calls its proof, and a stale one would be a proof about runs that are gone.
    /// Callers hold <see cref="_gate"/>.
    /// </summary>
    private void Publish(Dictionary<string, TraceIndexReader> next)
    {
        var covered = new HashSet<ulong>();
        foreach (var r in next.Values)
            foreach (ulong sid in r.CoveredSegments) covered.Add(sid);

        _open          = next;
        _coveredByOpen = covered;
    }

    /// <summary>
    /// The open reader for a path, so a test can install a seam on the instance a lookup will
    /// actually take a hold on. Opening the file again would produce a DIFFERENT reader with its
    /// own refcount, which is exactly the reader whose lifetime nothing here manages.
    /// </summary>
    internal TraceIndexReader? ReaderForTest(string path)
        => _open.TryGetValue(path, out var r) ? r : null;

    public void Dispose()
    {
        lock (_gate)
        {
            foreach (var r in _open.Values) r.Retire();
            Publish(new Dictionary<string, TraceIndexReader>(StringComparer.Ordinal));
        }
    }
}
