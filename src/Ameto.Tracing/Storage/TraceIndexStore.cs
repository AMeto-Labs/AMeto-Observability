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
internal readonly record struct TraceIndexAnswer(
    List<TraceIndexHit> Hits,
    HashSet<ulong>?     Unanswerable);

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

            _open = next;
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
            _open = next;
        }
        return true;
    }

    /// <summary>Closes and forgets runs by path. The files themselves are the caller's business.</summary>
    public void Remove(IEnumerable<string> paths)
    {
        lock (_gate)
        {
            var next  = CopyOpen();
            var drop  = new List<TraceIndexReader>();
            foreach (var p in paths)
                if (next.Remove(p, out var r)) drop.Add(r);
            _open = next;
            foreach (var r in drop) r.Retire();
        }
    }

    /// <summary>
    /// Every segment any open run places this trace in. An empty result means "no run named it",
    /// which is NOT the same as "it is not there" — see the class docstring.
    /// </summary>
    public TraceIndexAnswer Lookup(TraceId traceId)
    {
        // PINNED UNDER THE GATE, so nothing can retire and free a reader between taking the
        // snapshot and using it. A lookup holds each reader across bloom probes and a 4 KB block
        // read — milliseconds — and the store used to dispose dropped readers immediately, whose
        // bloom is native memory.
        List<TraceIndexReader> held;
        lock (_gate)
        {
            var open = _open;
            held = new List<TraceIndexReader>(open.Count);
            foreach (var r in open.Values) if (r.TryAcquire()) held.Add(r);
        }

        var hits = new List<TraceIndexHit>(2);
        HashSet<ulong>? unanswerable = null;
        ulong key = TraceIndexFile.KeyOf(traceId);
        try
        {
            foreach (var r in held)
            {
                if (r.Lookup(key, hits) != TraceIndexOutcome.Unreadable) continue;

                // A RUN THAT COULD NOT ANSWER UN-COVERS ITS SEGMENTS FOR THIS REQUEST. Silence
                // from a covered run is what lets the engine skip a segment, and this run proved
                // nothing — so the caller has to read those segments instead.
                (unanswerable ??= new HashSet<ulong>()).UnionWith(r.CoveredSegments);
                _logger.LogDebug("Trace index run {Path} could not answer a lookup; the {Count} "
                              + "segment(s) it covers are read rather than skipped",
                              r.Path, r.CoveredSegments.Length);
            }
        }
        finally { foreach (var r in held) r.Release(); }

        return new TraceIndexAnswer(hits, unanswerable);
    }

    /// <summary>True when any run is open at all — the read path skips its work entirely if not.</summary>
    public bool HasRuns => _open.Count > 0;

    /// <summary>
    /// A copy of the open map, named rather than spelled out at each call site — see the same
    /// helpers on TraceManifest.State: "new Dictionary<K,V>(collection)" and
    /// "new Dictionary<K,V>(count)" are one shape to the convention scanner and opposites in
    /// fact, and one arguable claim beats several unarguable ones.
    /// </summary>
    private Dictionary<string, TraceIndexReader> CopyOpen() => new(_open, StringComparer.Ordinal);

    public void Dispose()
    {
        lock (_gate)
        {
            foreach (var r in _open.Values) r.Retire();
            _open = new Dictionary<string, TraceIndexReader>(StringComparer.Ordinal);
        }
    }
}
