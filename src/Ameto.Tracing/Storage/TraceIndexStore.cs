using Microsoft.Extensions.Logging;

namespace Ameto.Tracing.Storage;

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
    public IReadOnlyList<ulong> Sync(TraceManifest manifest)
    {
        var wanted  = manifest.Runs;
        var unusable = new List<ulong>();

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
                if (opened is null)
                {
                    if (run.CoversSegment is { } sid) unusable.Add(sid);
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
            foreach (var r in stale) r.Dispose();
        }

        return unusable;
    }

    /// <summary>Registers a run written just now, without re-opening everything else.</summary>
    public void Add(TraceIndexRun run)
    {
        var opened = TraceIndexReader.Open(run.FilePath);
        if (opened is null)
        {
            _logger.LogWarning("Trace index run {Path} was written but will not open", run.FilePath);
            return;
        }
        lock (_gate)
        {
            var next = CopyOpen();
            if (next.Remove(run.FilePath, out var old)) old.Dispose();
            next[run.FilePath] = opened;
            _open = next;
        }
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
            foreach (var r in drop) r.Dispose();
        }
    }

    /// <summary>
    /// Every segment any open run places this trace in. An empty result means "no run named it",
    /// which is NOT the same as "it is not there" — see the class docstring.
    /// </summary>
    public List<TraceIndexHit> Lookup(TraceId traceId)
    {
        var hits = new List<TraceIndexHit>(2);
        ulong key = TraceIndexFile.KeyOf(traceId);
        foreach (var r in _open.Values) r.Lookup(key, hits);
        return hits;
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
            foreach (var r in _open.Values) r.Dispose();
            _open = new Dictionary<string, TraceIndexReader>(StringComparer.Ordinal);
        }
    }
}
