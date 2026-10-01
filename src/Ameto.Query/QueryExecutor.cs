using Microsoft.Extensions.Logging;
using Microsoft.Win32.SafeHandles;
using Ameto.Core;
using Ameto.Indexing;
using Ameto.Query.Filtering;
using Ameto.Storage;

namespace Ameto.Query;

/// <summary>
/// Executes a <see cref="QueryRequest"/> against the storage engine.
///
/// Execution pipeline for cold-tier segments:
///   0. Pin: every segment of the window snapshot gets a file handle straight away, and every
///      later open maps from it — a merge or retention under the query cannot take a file the
///      query has yet to open (#114; see <see cref="ExecuteAsync"/>).
///   1. Time-range filter on <see cref="SegmentInfo"/> (skip segments outside window).
///   2. Index fast-skip, in the order the merge consumes segments and only as far as the page
///      reaches (a batch at a time, see <see cref="MergeSourcesAsync"/>): open each group's
///      <see cref="SegmentIndexView"/> — the cached memo of what earlier queries worked out,
///      reading a section only for a new question — and call
///      <see cref="ISegmentIndex.MightContain"/>; skip groups where the index says no match.
///   3. Candidate narrowing: trigram (<see cref="ISegmentIndex.LookupTrigram"/>) and
///      inverted (<see cref="ISegmentIndex.LookupIntersect"/>) posting lists yield
///      candidate event ordinals (file order, v5 segments) — the reader skips blocks
///      without candidates and materialises only candidate rows.
///   4. Block decode: LZ4 decompress via <see cref="SegmentReader"/>.
///   5. Per-event AST evaluation via <see cref="FilterEvaluator"/>.
///
/// Hot-tier events are scanned directly (no index) and merged with the cold stream on
/// the same (Timestamp, EventId) key — one globally ordered stream feeds the limit and
/// the pagination cursor, whatever tier an event happens to live in.
/// Results are emitted in the requested <see cref="QueryDirection"/> order.
/// </summary>
public sealed class QueryExecutor : IQueryExecutor
{
    private readonly ISegmentProvider         _segments;
    private readonly SegmentIndexReaderFactory _indexFactory;
    private readonly ILogger<QueryExecutor>   _logger;
    private readonly SegmentIndexCache?       _indexCache;

    public QueryExecutor(
        ISegmentProvider         segments,
        SegmentIndexReaderFactory indexFactory,
        ILogger<QueryExecutor>   logger,
        SegmentIndexCache?       indexCache = null)
    {
        _segments     = segments;
        _indexFactory = indexFactory;
        _logger       = logger;
        _indexCache   = indexCache is { Enabled: true } ? indexCache : null;
    }

    /// <summary>
    /// How much SYNCHRONOUS scanning a query may do before it hands its consumer a pending step
    /// (<see cref="ScanPace"/>). Settable only so a test can make every clock read a yield and see
    /// the yields on a fixture far too small to take the default's 50 ms.
    /// </summary>
    internal TimeSpan ScanYieldInterval { get; init; } = ScanPace.DefaultInterval;

    // ── IQueryExecutor ────────────────────────────────────────────────────────

    public async IAsyncEnumerable<LogEvent> ExecuteAsync(
        QueryRequest request,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        // A caller that runs the same request shape over and over (the live tail, once per
        // poll) compiles the filter once and carries it on the request; it is trusted only
        // when it was compiled from this request's own text, so a mismatched pair costs a
        // compile, never a wrong filter.
        var filter = request.Prepared is CompiledFilter prepared
                     && string.Equals(prepared.Expression, request.Filter, StringComparison.Ordinal)
            ? prepared
            : CompiledFilter.Compile(request.Filter);
        int limit  = request.Count;
        int count  = 0;

        // A page of nothing asks for nothing: no snapshot, no pins, no prefilter. The loop below
        // checks its limit AFTER each row now, so without this a count of 0 would get a row; and
        // when the check ran before each row, the same request planned the window and primed a
        // prefilter batch (8 opens over the 40-segment fixture) for a first row it then dropped.
        if (limit <= 0) yield break;

        bool forward  = request.Direction == QueryDirection.Forward;
        var  from     = request.FromUtc;
        var  to       = request.ToUtc;
        var  afterId  = request.AfterEventId;
        var  afterTs  = request.AfterTimestampTicks;
        var  levels   = request.Levels;

        // The cursor is the tightest time bound the request carries: everything strictly
        // past it in the query direction was already served, so page k must not re-open
        // and re-decode the segments and blocks page k-1 already walked. Clamp INCLUSIVE
        // at the cursor tick — events sharing the timestamp are tie-broken by id inside
        // the cursor check, and the window checks are themselves inclusive. Guarded to
        // the DateTime range: the cursor comes off the wire, and a garbage value should
        // keep matching nothing (as before) rather than throw from the ctor.
        if (afterTs is long cursorTicks && cursorTicks >= 0 && cursorTicks <= DateTime.MaxValue.Ticks)
        {
            if (forward)
            {
                if (from is null || from.Value.UtcTicks < cursorTicks)
                    from = new DateTimeOffset(cursorTicks, TimeSpan.Zero);
            }
            else
            {
                if (to is null || to.Value.UtcTicks > cursorTicks)
                    to = new DateTimeOffset(cursorTicks, TimeSpan.Zero);
            }
        }

        // @t predicates from the filter's AND-chain are exact bounds too — folded into the
        // window so the catalog filter, zone map and hot header scan prune with them (a
        // `@t >= '...'` filter used to be evaluated per event over the whole catalog). The
        // evaluator re-checks every event regardless, so this can only skip work; the
        // DateTime-range guard mirrors the cursor's.
        if (filter.MinTimestampTicks is long boundMin && boundMin >= 0 && boundMin <= DateTime.MaxValue.Ticks
            && (from is null || from.Value.UtcTicks < boundMin))
            from = new DateTimeOffset(boundMin, TimeSpan.Zero);
        if (filter.MaxTimestampTicks is long boundMax && boundMax >= 0 && boundMax <= DateTime.MaxValue.Ticks
            && (to is null || to.Value.UtcTicks > boundMax))
            to = new DateTimeOffset(boundMax, TimeSpan.Zero);

        // The level set the filter's AND-chain admits is a `levels` parameter the caller
        // did not spell out — the same exactness (it is computed with the evaluator's own
        // comparison), the same pruning: level-split cold segments drop through their
        // posting lists, hot headers through the mask. The evaluator still re-checks every
        // event, so like the bounds above this can only skip work. A caller's explicit set
        // is kept as given.
        levels ??= filter.DerivedLevels;

        // ── Hot tier ──────────────────────────────────────────────────────────
        // Window/cursor/level filtering and the (@t, id) sort happen at HEADER level
        // inside the reader (HotTierScan) — events are materialised lazily in result
        // order, so a page query allocates ~limit events, not the whole tier.
        //
        // The hot stream is ONE MORE ENTRANT of the k-way merge below, not a prefix
        // of the response. Emitting it wholesale before the cold tier violated the
        // global (ts, id) order whenever the tiers interleave in time (late-arriving
        // events, replicated peer segments), and the violation was not cosmetic: the
        // client's next-page keyset cursor is the last emitted (ts, id), so every
        // not-yet-served cold event on the wrong side of a mis-ordered boundary
        // failed the cursor on all subsequent pages — silently unreachable rows.
        //
        // ── Cold-tier segments (k-way merge) ─────────────────────────────────
        // After Variant B, every segment's blocks are individually sorted by @t,
        // but two segments can overlap in [MinTs..MaxTs] (e.g. a flush that captured
        // some late-arriving events whose @t falls inside a previously flushed
        // segment's window). To preserve a global @t order across segments we run a
        // small k-way merge over per-segment iterators, keyed on (Timestamp, EventId).
        // Segments whose events were already served via the hot reader's frozen
        // tiers are excluded by `covered` to prevent duplicates during the flush
        // window (registered cold segment + still-frozen hot tier overlap).
        long fromTicksGlobal = from?.UtcTicks ?? long.MinValue;
        long toTicksGlobal   = to?.UtcTicks   ?? long.MaxValue;

        // ── The plan: the hot snapshot, the catalog snapshot, and a PIN on every segment ──
        // The merge opens its segments LAZILY, a batch at a time as its front reaches them, and
        // the catalog does not stand still meanwhile: a merge can swap a batch of the window's
        // segments out for its output and unlink them between this snapshot and their open
        // (#114). Opened by path, such a source was simply gone — the prefilter fell back, the
        // scan's own open failed and skipped it without a word — and the output holding its rows
        // was not in this snapshot either. Measured over 40 segments with a merge after the first
        // row: 50 of 252 rows for a filtered query and 25 of 1008 unfiltered, each a stream that
        // ended normally. Until the lazy prefilter (#109) a filtered query mapped every window
        // segment before its first row, which kept the files by accident; an unfiltered one never
        // had even that.
        //
        // So every segment of the window is pinned straight after the snapshot, with a bare
        // handle — an open and a close, ~13 µs a segment measured on Windows, no mapping and no
        // read — and every later open maps FROM the pin rather than resolving the path again. The
        // file then outlives a merge or a retention delete under the query: on Linux the inode
        // survives the unlink; on Windows the unlink fails (see PinShare) and is parked for the
        // retry, which deletes the file once this query lets go — the road a held mapping always
        // took.
        //
        // The pins are THIS method's: released in the finally below, after the merge's own
        // finally has closed every reader mapped from them — the await foreach disposes the merge
        // before control gets there, on every exit: a full page, a consumer stopping early,
        // cancellation, a throw.
        IHotTierReader?   hotReader = null;
        List<SegmentInfo> window    = [];
        SafeFileHandle?[] pins      = [];
        try
        {
            for (int attempt = 1; ; attempt++)
            {
                hotReader = _segments.OpenHotTierReader();
                window    = SnapshotWindow(hotReader.CoveredSegmentKeys, from, to, fromTicksGlobal, toTicksGlobal);
                pins      = window.Count == 0 ? [] : new SafeFileHandle?[window.Count];
                if (BeforePinForTest is { } beforePin) await beforePin(window);

                // A segment that left the catalog before its pin was retired on purpose, by a merge
                // or by retention, and the catalog says which of the two by what it lists NOW: the
                // output, or nothing. So the plan is taken again — the hot snapshot with it, which
                // is what keeps the retake from double-reading a tier flushed in between: a cold
                // re-read alone could list the segments of the tier this hot snapshot still holds
                // as current, which no `covered` set names. Nothing has been read or yielded yet,
                // so a retake costs a catalog read and the pins, and is invisible to the consumer.
                if (!PinWindow(window, pins, from, to, mayRetake: attempt < MaxPlanAttempts)) break;

                ReleasePins(pins);
                hotReader.Dispose();
                hotReader = null;
            }

            // ONE pace for every source below, the hot tier and each segment scan: the scan hands its
            // consumer a pending step at least every ScanYieldInterval of synchronous work, wherever
            // in the merge that work is spent. Without it no step of a real scan is ever pending, and
            // a consumer that sends while the scan works never gets the chance (see ScanPace).
            var pace      = new ScanPace(ScanYieldInterval);
            var hotStream = HotEventsAsync(hotReader, filter, from, to, afterTs, afterId, forward, levels, pace, ct);

            await foreach (var ev in MergeSourcesAsync(hotStream, window, pins, filter, levels, from, to, afterTs, afterId, forward, pace, ct))
            {
                if (ct.IsCancellationRequested) yield break;
                yield return ev;
                // Stop AT the limit, not when the row after it arrives. Checked before each row,
                // the limit made a consumer that reads the stream to its end — the list endpoint,
                // the SSE writer — wait while the merge produced row limit+1, only to drop it, and
                // at a prefilter batch edge that one row costs the whole next batch, doubled: 24
                // segments opened instead of 8 for a page of 44 over the 40-segment fixture (#114).
                if (++count >= limit) yield break;
            }
        }
        finally
        {
            ReleasePins(pins);
            hotReader?.Dispose();
        }
    }

    // ── Window snapshot and pins (#114) ───────────────────────────────────────

    /// <summary>
    /// How a pin shares its file: <see cref="FileShare.Read"/>, exactly what a by-path
    /// <see cref="SegmentReader.Open(string, bool)"/> takes (its <c>CreateFromFile</c> opens the
    /// path for read, sharing read), so a pin is no more and no less permissive than the mapping it
    /// stands in for, and the merge planner's and the header scan's own by-path opens go on working
    /// beside it.
    ///
    /// <para>NOT <see cref="FileShare.Delete"/>, deliberately. Without it a Windows unlink of a
    /// pinned file fails with a sharing violation, and the storage engine already has the road for
    /// that: the delete is parked and retried until the query lets go (StorageEngine
    /// ParkSegmentDelete, SettleMergedSources), the merge keeps its manifest for the recovery sweep.
    /// With it the unlink would SUCCEED, and what that leaves depends on the volume — measured on
    /// NTFS here, POSIX semantics take the name at once, even while the file is mapped; under
    /// legacy semantics (FAT, many SMB shares) the file turns delete-pending, keeping its name, failing
    /// every new open and answering <c>File.Exists</c> true — a state the merge's manifest check, the
    /// recovery sweep and the boot scan do not model. Linux ignores the flag either way: the unlink
    /// succeeds and the inode lives as long as the pin.</para>
    ///
    /// <para>NOT <see cref="FileShare.Write"/>: nothing writes a published segment, and a pin
    /// should not be what lets something start.</para>
    /// </summary>
    private const FileShare PinShare = FileShare.Read;

    /// <summary>
    /// How many times a query takes its plan — the hot snapshot, the catalog snapshot, the pins —
    /// for a segment that left the catalog between the snapshot and its pin
    /// (<see cref="PinWindow"/>). One retake answers a merge commit or a retention delete landing in
    /// that window, which is as long as pinning the window takes; a third attempt means it happened
    /// twice in a row, and the last plan goes ahead with what it could pin.
    /// </summary>
    private const int MaxPlanAttempts = 3;

    /// <summary>
    /// Test seam: called with the window's catalog snapshot after it is taken and before any of it
    /// is pinned — the window in which a merge or a retention delete can take a segment out from
    /// under the plan. Called again for every retake. Null in production.
    /// </summary>
    internal Func<IReadOnlyList<SegmentInfo>, ValueTask>? BeforePinForTest { get; set; }

    /// <summary>
    /// Process-wide count of segment files pinned by a query (<see cref="PinWindow"/>), with the
    /// caveats of <see cref="SegmentReader.Opens"/>: a test that reads it needs the assembly's
    /// parallelisation switched off.
    /// </summary>
    internal static long Pins;

    /// <summary>
    /// The other half of <see cref="Pins"/>: pins released, each exactly once
    /// (<see cref="ReleasePins"/> takes the handle out of its slot before closing it). A pin is a
    /// handle with no mapping behind it, so a leaked one is invisible to the reader counters —
    /// and on Windows it is a segment file that can never be deleted while the process lives.
    /// </summary>
    internal static long Unpins;

    /// <summary>
    /// The catalog snapshot of the window: segments overlapping it, minus those the hot snapshot's
    /// frozen tiers still cover. A loop, not the LINQ chain it replaces — two iterators and a
    /// growing list per query, for a list whose size the catalog already gave.
    /// </summary>
    private List<SegmentInfo> SnapshotWindow(
        IReadOnlySet<SegmentKey> covered, DateTimeOffset? from, DateTimeOffset? to, long fromTicks, long toTicks)
    {
        var listed = _segments.GetSegments(from, to);
        var window = new List<SegmentInfo>(listed.Count);
        for (int i = 0; i < listed.Count; i++)
        {
            var s = listed[i];
            if (covered.Contains(SegmentKey.Of(s))) continue;
            if (s.MaxTimestampTicks < fromTicks || s.MinTimestampTicks > toTicks) continue;
            window.Add(s);
        }
        return window;
    }

    /// <summary>
    /// Pins every segment of <paramref name="window"/> into the slot of <paramref name="pins"/> with
    /// the same index. True when the plan has to be taken again: a segment's file was gone and the
    /// catalog no longer lists it.
    ///
    /// <para>A missing file is sorted the way the header aggregation sorts one it meets mid-scan
    /// (StorageEngine.OnHeaderSegmentUnreadable): by whether the catalog STILL SERVES the segment
    /// under its key and at its path.</para>
    /// <list type="bullet">
    /// <item><b>It does not</b>: a merge swapped it out for its output, or retention removed it, and
    /// either way unlinked the file after the entry went. The two mean opposite things for the rows
    /// — in an output this snapshot does not list, or gone — and the header scan has to tell them
    /// apart after the fact, to call a count a floor. Here nothing has been read yet, so the plan is
    /// simply taken again: the next snapshot lists the output, or nothing, and either answer is
    /// whole. Debug, as a race: nothing is damaged.</item>
    /// <item><b>It does</b>: the file went behind the catalog's back, or an import has published its
    /// entry and not landed the file yet (ImportSegment publishes before its move). The segment stays
    /// in the plan unpinned, and its open BY PATH when the merge front reaches it is the retry —
    /// late enough for an import to have landed, and warned by name if it fails too
    /// (<see cref="ReportUnopenable"/>): the header scan's "unreadable", never a silent skip.</item>
    /// </list>
    /// <para>On the last attempt a retired segment stays in the plan unpinned as well, and meets the
    /// same Warning if the query reaches it.</para>
    ///
    /// <para>A file that is there but refuses the open — a sharing violation from another process,
    /// an ACL, the descriptor limit — stays in the plan unpinned too, for the same retry.</para>
    /// </summary>
    private bool PinWindow(
        List<SegmentInfo> window, SafeFileHandle?[] pins, DateTimeOffset? from, DateTimeOffset? to, bool mayRetake)
    {
        List<int>? missing = null;
        for (int i = 0; i < window.Count; i++)
        {
            try
            {
                pins[i] = File.OpenHandle(window[i].FilePath, FileMode.Open, FileAccess.Read, PinShare);
                Interlocked.Increment(ref Pins);
            }
            catch (Exception ex) when (ex is FileNotFoundException or DirectoryNotFoundException)
            {
                (missing ??= new List<int>(1)).Add(i);
            }
            catch (Exception ex)
            {
                // Anything else, never the whole query's failure: one segment's open never was.
                // Debug: if the open by path fails as well, ReportUnopenable names the file at
                // Warning, and one refusal logged twice at that level is noise.
                _logger.LogDebug(ex,
                    "Query could not pin segment {File}; it is opened by path if the query reaches it",
                    window[i].FilePath);
            }
        }
        if (missing is null) return false;

        // ONE read of the catalog answers for every missing file, and only a query that met one
        // pays for it.
        var  catalog = _segments.GetSegments(from, to);
        bool retired = false;
        foreach (int i in missing)
        {
            var info = window[i];
            if (Lists(catalog, info))
            {
                _logger.LogDebug(
                    "Segment {File} is in the catalog but its file was not there to pin; it is opened by path if the query reaches it",
                    info.FilePath);
            }
            else
            {
                retired = true;
                _logger.LogDebug(
                    "Segment {File} left the catalog between the query's snapshot and its pin (a merge or retention) — {Action}",
                    info.FilePath, mayRetake
                        ? "taking the plan again"
                        : "out of plan attempts; it is opened by path if the query reaches it");
            }
        }
        return retired && mayRetake;
    }

    /// <summary>
    /// Whether <paramref name="catalog"/> holds <paramref name="info"/>'s segment under its key AND
    /// at its path — the test StorageEngine.CatalogServes makes, for the same reason: a key served
    /// from another path is another file.
    /// </summary>
    private static bool Lists(IReadOnlyList<SegmentInfo> catalog, SegmentInfo info)
    {
        var key = SegmentKey.Of(info);
        for (int i = 0; i < catalog.Count; i++)
        {
            var c = catalog[i];
            if (SegmentKey.Of(c) == key && string.Equals(c.FilePath, info.FilePath, StringComparison.OrdinalIgnoreCase))
                return true;
        }
        return false;
    }

    /// <summary>
    /// Releases every pin still in <paramref name="pins"/>, emptying each slot first, so a second
    /// call — the retake path, then the finally — closes and counts nothing twice.
    /// </summary>
    private static void ReleasePins(SafeFileHandle?[] pins)
    {
        for (int i = 0; i < pins.Length; i++)
        {
            if (pins[i] is not { } pin) continue;
            pins[i] = null;
            try { pin.Dispose(); } catch { /* best-effort */ }
            Interlocked.Increment(ref Unpins);
        }
    }

    /// <summary>
    /// Opens a window segment for the prefilter or the scan: mapped from its pin when the plan
    /// holds one, by path when it could not take one (see <see cref="PinWindow"/>).
    /// </summary>
    private static SegmentReader OpenSegment(SegmentInfo info, SafeFileHandle? pin) =>
        pin is not null ? SegmentReader.Open(pin, info.FilePath) : SegmentReader.Open(info.FilePath);

    // ── Segments that cannot be opened ────────────────────────────────────────

    /// <summary>Places under the warn-once rule of <see cref="ReportUnopenable"/>; past it, Debug only.</summary>
    private const int WarnedUnopenableCap = 1024;

    /// <summary>
    /// Files <see cref="ReportUnopenable"/> has already named at Warning, by file and by site (the
    /// prefilter's fallback and the scan's skip say different things). Never cleared to make room,
    /// for the reason the header aggregation's set is not: clearing a full set made every file past
    /// the cap warn again on every poll.
    /// </summary>
    private readonly HashSet<(string File, bool Scan)> _warnedUnopenable = new();
    private readonly System.Threading.Lock _warnedUnopenableGate = new();

    /// <summary>
    /// A window segment that could not be opened, named at Warning with its file — ONCE per file and
    /// site, Debug after that.
    ///
    /// <para>It used to be Debug in the prefilter and nothing at all in the scan, which then skipped
    /// the segment: its rows were missing and the stream ended as if whole. Opened by path, a
    /// failure there could be a merge or retention racing the query, and a race is not damage — but
    /// that silence is also exactly how a merge took rows from queries unseen (#114). A PINNED file
    /// cannot have been unlinked under the query, so failing to map it is no race: the bytes are
    /// torn, or the mapping itself failed. And a segment that could not be pinned is one the catalog
    /// still served with no file behind it — the header aggregation's "unreadable", which that path
    /// names at Warning too.</para>
    ///
    /// <para>Once, and not per query, for the header aggregation's reason: the live tail polls, alert
    /// rules tick every 15 s, and a torn file stays in the catalog until the next start quarantines
    /// it — a Warning per query would bury the log.</para>
    /// </summary>
    private void ReportUnopenable(Exception ex, SegmentInfo info, bool pinned, bool scan)
    {
        bool first;
        lock (_warnedUnopenableGate)
            first = _warnedUnopenable.Count < WarnedUnopenableCap && _warnedUnopenable.Add((info.FilePath, scan));

        var level = first ? Microsoft.Extensions.Logging.LogLevel.Warning : Microsoft.Extensions.Logging.LogLevel.Debug;
        if (scan)
            _logger.Log(level, ex,
                "Segment {File} could not be opened (pinned by the query: {Pinned}) — its rows are missing from this query's result",
                info.FilePath, pinned);
        else
            _logger.Log(level, ex,
                "Index prefilter could not open segment {File} (pinned by the query: {Pinned}) — falling back to a full scan of it",
                info.FilePath, pinned);
    }

    /// <summary>
    /// The hot tier as a (ts, id)-sorted async source for the merge. ReadSorted already
    /// applies window, cursor and level filtering at header level, plus whatever of the
    /// filter the header can answer (<see cref="CompiledFilter.HeaderPredicate"/>: level,
    /// trace / span id, service); the compiled filter then runs here, per materialised
    /// event, as the correctness gate — the same division of labour the cold scan uses.
    /// </summary>
    private static async IAsyncEnumerable<LogEvent> HotEventsAsync(
        IHotTierReader                hotReader,
        CompiledFilter                filter,
        DateTimeOffset?               from,
        DateTimeOffset?               to,
        long?                         afterTs,
        Ameto.Core.EventId?           afterId,
        bool                          forward,
        HashSet<Ameto.Core.LogLevel>? levels,
        ScanPace                      pace,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        foreach (var ev in hotReader.ReadSorted(
                     from?.UtcTicks ?? long.MinValue, to?.UtcTicks ?? long.MaxValue,
                     afterTs, afterId?.RawValue, forward, levels, filter.HeaderPredicate))
        {
            if (ct.IsCancellationRequested) yield break;
            if (pace.Due()) await ScanPace.Yield();
            if (!filter.Matches(ev)) continue;
            yield return ev;
        }
    }

    // ── Cold-tier k-way merge ─────────────────────────────────────────────────

    private static readonly Comparer<(long ts, ulong id)> MergeAsc =
        Comparer<(long ts, ulong id)>.Create(static (a, b) =>
            a.ts != b.ts ? a.ts.CompareTo(b.ts) : a.id.CompareTo(b.id));

    private static readonly Comparer<(long ts, ulong id)> MergeDesc =
        Comparer<(long ts, ulong id)>.Create(static (a, b) =>
            a.ts != b.ts ? b.ts.CompareTo(a.ts) : b.id.CompareTo(a.id));

    /// <summary>
    /// Merges the hot-tier stream and events from multiple cold-tier segments preserving
    /// a global (Timestamp, EventId) order. For sorted segments (file format v2+) we
    /// stream blocks lazily; for unsorted legacy segments (v1) we materialise the whole
    /// segment, sort it once, then merge with the rest.
    /// </summary>
    /// <param name="pins">
    /// Index for index with <paramref name="segInfos"/>: the handle pinning each segment's file
    /// since the snapshot, or null where none could be taken (see <see cref="PinWindow"/>). BORROWED
    /// — the caller owns and releases them, after this method's finally has closed every reader
    /// mapped from one.
    /// </param>
    private async IAsyncEnumerable<LogEvent> MergeSourcesAsync(
        IAsyncEnumerable<LogEvent>           hotStream,
        IReadOnlyList<SegmentInfo>           segInfos,
        SafeFileHandle?[]                    pins,
        CompiledFilter                       filter,
        HashSet<Ameto.Core.LogLevel>?       levels,
        DateTimeOffset?                      from,
        DateTimeOffset?                      to,
        long?                                afterTs,
        Ameto.Core.EventId?                 afterId,
        bool                                 forward,
        ScanPace                             pace,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
    {
        long fromTicks = from?.UtcTicks ?? long.MinValue;
        long toTicks   = to?.UtcTicks   ?? long.MaxValue;

        // Priming order, for EVERY segment of the window and before any of them is opened: the
        // merge front moves one way through time, so segments are consumed in that order too —
        // newest MaxTs first going backward, oldest MinTs first going forward. The prefilter
        // walks the same order (see PrimeAsync), which is what lets it stop where the page does.
        // Stable like the OrderBy it replaces (ties keep catalog order): the key carries the
        // input index, so an unstable Array.Sort cannot reorder ties.
        var keyed = new PrimeEntry[segInfos.Count];
        for (int i = 0; i < keyed.Length; i++)
        {
            var s = segInfos[i];
            keyed[i] = new PrimeEntry(forward ? s.MinTimestampTicks : s.MaxTimestampTicks, i, s);
        }
        keyed.AsSpan().Sort(new PrimeOrder(descending: !forward));
        // Each segment travels with its pin from here on — through the prefilter, the priming
        // queue and the scan — so that whichever of them opens it maps the file the snapshot
        // named, not whatever the path holds by then (#114).
        var order = new WindowSegment[keyed.Length];
        for (int i = 0; i < order.Length; i++) order[i] = new WindowSegment(keyed[i].Info, pins[keyed[i].Index]);

        // The priming queue: the segments that survived the prefilter, in priming order, each
        // with the reader the prefilter opened. It is also the only OWNER of those readers — the
        // finally closes every one of them, primed or not. A filter with nothing to ask an index
        // has every segment survive without opening any, so it skips the prefilter outright.
        bool prefilter   = NeedsPrefilter(filter, levels);
        var  survivors   = new List<PrefilterResult>(prefilter ? Math.Min(order.Length, PrefilterParallelism) : order.Length);
        int  checkedUpTo = 0;                    // order[..checkedUpTo] has been through the prefilter
        int  batch       = PrefilterParallelism; // the next prefilter batch; doubles each time
        if (!prefilter)
        {
            foreach (var s in order) survivors.Add(new PrefilterResult(s.Info, null, null, s.Pin));
            checkedUpTo = order.Length;
        }

        var iterators = new List<IAsyncEnumerator<LogEvent>>();
        try
        {
            // PriorityQueue ordered by (ts, id). For backward (newest-first) we invert
            // the comparer; .NET's PriorityQueue is a min-heap.
            var comparer = forward ? MergeAsc : MergeDesc;
            var heap = new PriorityQueue<IAsyncEnumerator<LogEvent>, (long ts, ulong id)>(comparer);
            int next = 0;

            // The hot tier primes unconditionally: it is RAM-resident (priming costs a
            // header walk, no I/O) and its time bounds are not in the catalog, so it
            // cannot take part in the could-beat check that gates the cold segments.
            var hotIt = hotStream.GetAsyncEnumerator(ct);
            if (await hotIt.MoveNextAsync())
            {
                iterators.Add(hotIt);
                heap.Enqueue(hotIt, (hotIt.Current.Timestamp.UtcTicks, hotIt.Current.Id.RawValue));
            }
            else
            {
                await hotIt.DisposeAsync();
            }

            // A segment can only matter while it could still beat the merge front: going
            // backward its MaxTs is an upper bound on anything it can produce, so once the
            // heap's best is newer than that, neither it nor any later segment (they are
            // ordered) can contribute. Ties prime, so equal timestamps are never dropped. An
            // empty heap has no front yet, and anything could beat it.
            bool CouldBeat(SegmentInfo info) =>
                !heap.TryPeek(out _, out var best)
                || (forward ? info.MinTimestampTicks <= best.ts : info.MaxTimestampTicks >= best.ts);

            // Open segments LAZILY. Priming every surviving segment up front is what made a
            // page cost the whole catalog: an unfiltered `count=50` takes GetSegments(null,
            // null) — every segment there is — memory-maps all of them and decompresses a
            // block from each, only to serve 50 events off the top of the heap. On the
            // sandbox stand that is 291 opens for one page.
            //
            // PREFILTER them lazily too, for the same reason one step earlier. The prefilter
            // used to run over every segment of the window before the first row — open the
            // file, read each group's bloom, copy its inverted section — and a 50-row page then
            // primed the newest handful of them. A filtered page cost the WINDOW, not the page:
            // on a copy of the sandbox stand, in a 512 MB two-core container with a cold page
            // cache, `['service.name'] = 'Axiom.API'` took 0.10 s over a day and 0.95 s over 90
            // days for the same 50 rows, nearly all of it prefilter. The queue is now filled a
            // batch of the priming order at a time — only when it runs dry, and only while the
            // next unchecked segment could still beat the front: the bound priming already uses,
            // and the order makes that one check answer for every segment after it.
            //
            // Batches double from one wave of the prefilter's parallelism. A page the newest k
            // segments can fill checks fewer than 2k + PrefilterParallelism of them; a filter
            // that has to see the whole window — a rare value, or one that is not there — still
            // gets through it in parallel, in log2(n) rounds instead of one.
            async ValueTask PrimeAsync()
            {
                while (true)
                {
                    if (next == survivors.Count)
                    {
                        if (checkedUpTo == order.Length || !CouldBeat(order[checkedUpTo].Info)) return;

                        int take   = Math.Min(batch, order.Length - checkedUpTo);
                        var passed = await PrefilterSegmentsAsync(
                            new ArraySegment<WindowSegment>(order, checkedUpTo, take),
                            filter, levels, fromTicks, toTicks, ct);
                        // Survivors come back in input order and the input is the next slice
                        // of the priming order, so appending keeps the queue ordered.
                        survivors.AddRange(passed);
                        checkedUpTo += take;
                        batch        = Math.Min(batch * 2, order.Length);
                        continue;
                    }

                    if (!CouldBeat(survivors[next].Info)) return;

                    var (segInfo, candidateOffsets, segReader, segPin) = survivors[next++];
                    // The reader is BORROWED — the finally below owns every one of them,
                    // primed or not, so the scan must not dispose what it did not open. So is
                    // the pin, which the scan maps its own reader from when there is none.
                    var stream = ScanSegmentAsync(segInfo, filter, levels, candidateOffsets, segReader, segPin,
                                                  from, to, afterTs, afterId, !forward, pace, ct);
                    var newIt = stream.GetAsyncEnumerator(ct);
                    if (await newIt.MoveNextAsync())
                    {
                        iterators.Add(newIt);
                        heap.Enqueue(newIt, (newIt.Current.Timestamp.UtcTicks, newIt.Current.Id.RawValue));
                    }
                    else
                    {
                        await newIt.DisposeAsync();
                    }
                }
            }

            await PrimeAsync();

            while (heap.Count > 0)
            {
                if (ct.IsCancellationRequested) yield break;

                var it = heap.Dequeue();
                yield return it.Current;

                if (await it.MoveNextAsync())
                    heap.Enqueue(it, (it.Current.Timestamp.UtcTicks, it.Current.Id.RawValue));
                else
                    await it.DisposeAsync();

                // The front just moved; a segment that could not contribute before may be
                // able to now.
                await PrimeAsync();
            }
        }
        finally
        {
            // Anything still in the heap was already disposed when drained, but if the
            // consumer broke out early we must release the remaining mmap handles.
            foreach (var it in iterators)
            {
                try { await it.DisposeAsync(); } catch { /* best-effort */ }
            }

            // …and every reader the prefilter carried, INCLUDING survivors that never primed —
            // the rest of the last batch, for a small page. This is the only owner: the scan
            // borrows, the iterator above closes only what it opened itself, and a batch that
            // failed closed its own before throwing. The PINS are not released here but by
            // ExecuteAsync, which took them, once this has run: a pin outlives every reader
            // mapped from it. Until both have gone those files cannot be deleted on Windows; the
            // merge already handles a source held open by an in-flight query (the delete parked
            // and retried, the manifest kept for the recovery sweep), and the hold is bounded by
            // this query either way.
            foreach (var p in survivors)
            {
                if (p.Reader is { } r)
                {
                    try { r.Dispose(); } catch { /* best-effort */ }
                }
            }
        }
    }

    // ── Index fast-skip + trigram pre-filter (combined, parallel) ────────────

    /// <summary>
    /// Result of the per-segment prefilter: the segment to scan, optional candidate block
    /// offsets from the trigram index (null = scan all blocks), and the reader the prefilter
    /// already opened.
    ///
    /// <para>OWNERSHIP: <paramref name="Reader"/> belongs to <see cref="MergeSourcesAsync"/>,
    /// which disposes every one of them in its finally — including the segments that never
    /// prime. The scan borrows it and must not dispose it. Null means the prefilter opened
    /// nothing (the no-hint passthrough, or a segment that failed to open) and the scan opens
    /// and closes its own.</para>
    ///
    /// <para>LIFETIME, and the reason this is not a cache: a mapped file cannot be deleted on
    /// Windows, and retention and the merge delete segments while queries run. Bounded by the
    /// QUERY, a held reader is the same hazard the merge already documents and handles — the
    /// catalog entry goes, <c>File.Delete</c> fails, the manifest survives and the recovery
    /// sweep finishes the job once the reader closes. What changes is which segments are held:
    /// the survivors rather than only the primed ones. Anything longer-lived than a query would
    /// need refcounting against the catalog, which is deliberately not attempted here.</para>
    ///
    /// <para><paramref name="Pin"/> is the segment's pin, carried for the scan to map its own
    /// reader from when <paramref name="Reader"/> is null; borrowed like everything else here
    /// (see <see cref="WindowSegment"/>).</para>
    /// </summary>
    private readonly record struct PrefilterResult(
        SegmentInfo Info, uint[]? CandidateOffsets, SegmentReader? Reader, SafeFileHandle? Pin);

    /// <summary>
    /// A window segment and the handle pinning its file since the catalog snapshot (#114), or null
    /// where none could be taken. The pin belongs to <see cref="ExecuteAsync"/>, which releases it
    /// after every reader mapped from it is closed; everything downstream only borrows it.
    /// </summary>
    private readonly record struct WindowSegment(SegmentInfo Info, SafeFileHandle? Pin);

    /// <summary>A window segment keyed for the priming order (see <see cref="PrimeOrder"/>).</summary>
    private readonly record struct PrimeEntry(long Key, int Index, SegmentInfo Info);

    /// <summary>
    /// The priming order — MinTs ascending going forward, MaxTs descending going backward
    /// — with the input index as the tiebreak, so the sort is stable like the LINQ OrderBy
    /// it replaced without the keyed comparer, the iterator chain and the list per query.
    /// </summary>
    private readonly struct PrimeOrder(bool descending) : IComparer<PrimeEntry>
    {
        public int Compare(PrimeEntry a, PrimeEntry b)
        {
            int c = descending ? b.Key.CompareTo(a.Key) : a.Key.CompareTo(b.Key);
            return c != 0 ? c : a.Index.CompareTo(b.Index);
        }
    }

    /// <summary>
    /// How many segments the prefilter works on at once: one per core, at most 8 — each one in
    /// flight holds index sections of several MB (see <see cref="PrefilterSegmentsAsync"/>). It is
    /// also the merge's FIRST prefilter batch, one wave of this parallelism, so a page the newest
    /// few segments can fill pays for no more of them than that.
    /// </summary>
    internal static readonly int PrefilterParallelism = Math.Min(Environment.ProcessorCount, 8);

    /// <summary>
    /// Whether <see cref="PrefilterSegmentsAsync"/> has anything to ask an index: an equality or
    /// inverted hint, a substring predicate, or a level set. Without one every segment passes
    /// through unopened, and the merge does that without calling it.
    /// </summary>
    private static bool NeedsPrefilter(CompiledFilter filter, HashSet<Ameto.Core.LogLevel>? levels) =>
        (!filter.IsMatchAll && filter.TryGetIndexHint(out _, out _))
        || filter.GetInvertedHints().Count > 0
        || filter.GetTrigramHints().Count > 0
        || levels is { Count: > 0 };

    /// <summary>
    /// Runs bloom/inverted fast-skip and trigram offset lookup for one batch of cold
    /// segments in parallel, opening each segment's mmap exactly once. Survivors come
    /// back in <paramref name="segInfos"/> order — results are written into a slot per
    /// input index, so the parallel completion order does not leak out — and the merge
    /// RELIES on that: it hands over consecutive slices of its priming order and appends
    /// what survives to its priming queue as is (<see cref="MergeSourcesAsync"/>).
    ///
    /// <para>The unit of prefiltering is the INDEX GROUP, not the file. A single bloom
    /// stretched over 24 h answers "maybe" to everything, so a day-scale segment would
    /// survive every query and the fast-skip would stop being a skip; per group the filter
    /// keeps the ~10 bits/term it is sized for. A rejected group costs one bloom read and
    /// never touches its multi-MB inverted/trigram sections. v4-v6 segments expose exactly
    /// one group, so they take the same path with the same result as before.</para>
    /// </summary>
    private async Task<List<PrefilterResult>> PrefilterSegmentsAsync(
        IReadOnlyList<WindowSegment>  segInfos,
        CompiledFilter                filter,
        HashSet<Ameto.Core.LogLevel>? levels,
        long                          fromTicks,
        long                          toTicks,
        CancellationToken             ct)
    {
        // Fast path: nothing to prefilter — pass every segment through. The merge never asks
        // (it primes such a filter straight from the catalog); this keeps the method whole.
        if (!NeedsPrefilter(filter, levels))
        {
            var passthrough = new List<PrefilterResult>(segInfos.Count);
            foreach (var seg in segInfos)
                passthrough.Add(new PrefilterResult(seg.Info, null, null, seg.Pin));
            return passthrough;
        }

        // GetTrigramHints() returns a pre-computed list — no .ToList() allocation needed.
        var trigramHints   = filter.GetTrigramHints();
        var invertedHints  = filter.GetInvertedHints();
        bool hasIndexHint  = !filter.IsMatchAll && filter.TryGetIndexHint(out _, out _);
        bool hasInvHints   = invertedHints.Count > 0;

        // The levels PARAMETER prunes through the same index the `@l = '...'` FILTER
        // uses. It used to prune nothing: an errors-only dashboard query decoded every
        // block of every surviving segment only for the per-event level check to reject
        // nearly all of it. Per group, each level of the set either has a posting list,
        // is provably absent, or answers "no information" (a pre-index segment) — the
        // union across the set is definitive unless any level answers the latter.
        (string, object?)[][]? levelHints = null;
        if (levels is { Count: > 0 })
        {
            levelHints = new (string, object?)[levels.Count][];
            int li = 0;
            foreach (var l in levels)
                levelHints[li++] = [(ClefFields.Level, l.ToSeqString())];
        }

        var results = new PrefilterResult?[segInfos.Count];

        // Bound parallelism conservatively — each in-flight prefilter holds
        // index byte arrays (inverted + trigram can be several MB per segment),
        // so a high degree of parallelism over hundreds of segments blows
        // working-set memory into the gigabytes. ProcessorCount, capped at 8,
        // is a good balance between throughput and RAM.
        int degree = Math.Max(1, Math.Min(PrefilterParallelism, segInfos.Count));

        try
        {
            await Parallel.ForEachAsync(
                Enumerable.Range(0, segInfos.Count),
                new ParallelOptions { MaxDegreeOfParallelism = degree, CancellationToken = ct },
                (i, innerCt) =>
                {
                    var seg  = segInfos[i];
                    var info = seg.Info;
                    // NOT a `using`: a surviving segment hands its reader to the scan through
                    // PrefilterResult (see the record's ownership note) and the merge disposes it.
                    // Every other exit from this body disposes it here — `keep` is the one flag
                    // that decides which, and it is set exactly where the result is stored.
                    SegmentReader? reader = null;
                    bool keep = false;
                    try
                    {
                        // From the pin: the file the snapshot named, whatever a merge or retention
                        // has done to the path since (#114).
                        reader = OpenSegment(info, seg.Pin);

                        // Accumulated across the surviving groups. Posting offsets are FILE
                        // ordinals in every group (SegmentIndexBuilder.Build writes
                        // firstOrdinal + pos), so the groups' candidate arrays simply
                        // concatenate — no rebasing.
                        //
                        // The ORDER of what comes out is STILL not a promise this method makes.
                        // Each group's array is ascending — TryNarrowWithIndex merges sorted
                        // posting lists now, where it used to drain a HashSet whose enumeration
                        // order is unspecified by contract — but groups are appended in file
                        // order and a later group's ordinals are all above an earlier one's only
                        // because SegmentIndexBuilder.Build writes firstOrdinal + pos. Nothing
                        // downstream may assume more than "the reader will handle it".
                        //
                        // SegmentReader.ReadEventsAsync verifies the order before its two-pointer
                        // walk and sorts a copy when it has to, and that check is the contract,
                        // not a belt-and-braces extra: the walk advances a single cursor through
                        // the candidates alongside ascending row ordinals, so one descending pair
                        // makes it step past a candidate it will never come back to and the query
                        // silently loses rows the index proved it should return. Do not delete it
                        // on the strength of a comment here.
                        List<uint>? candidates = null;
                        bool anyGroupSurvived  = false;
                        // A surviving group that could not narrow (its index sections are absent —
                        // e.g. a WAL-recovery flush that ran before the builder was wired) is NO
                        // information about its rows. Candidates would then silently exclude them,
                        // so the whole segment falls back to a full scan.
                        bool unnarrowedGroup   = false;
                        // THE THIRD NARROWING STATE: groups that contribute EVERY one of their
                        // rows (see TryNarrowWithIndex — the level union that IS the group). Their
                        // ordinals are the contiguous range [FirstOrdinal, +EventCount), so they
                        // are not materialised here at all; they are generated only if some OTHER
                        // group genuinely narrows and the segment therefore has to name ordinals.
                        List<(uint First, uint Count)>? everyRowGroups = null;
                        // Groups this segment could contribute rows from, and groups that survived.
                        // When they agree, every row of the segment is a candidate, and the honest
                        // way to say so is to hand the scan no candidate list at all.
                        int groupsConsidered = 0, groupsAccepted = 0;

                        // Appends the pending whole-group ranges. Called before a narrowed group's
                        // own ordinals go in, so the list stays ascending: groups are visited in
                        // ordinal order and everything pending came from an earlier one.
                        void FlushEveryRowGroups()
                        {
                            if (everyRowGroups is null || candidates is null) return;
                            for (int r = 0; r < everyRowGroups.Count; r++)
                            {
                                var (first, count) = everyRowGroups[r];
                                for (uint o = 0; o < count; o++) candidates.Add(first + o);
                            }
                            everyRowGroups.Clear();
                        }

                        void Accept(uint[]? groupCandidates, bool everyRow, uint firstOrdinal, uint eventCount)
                        {
                            anyGroupSurvived = true;
                            groupsAccepted++;

                            if (everyRow)
                            {
                                (everyRowGroups ??= new List<(uint, uint)>(4)).Add((firstOrdinal, eventCount));
                            }
                            else if (groupCandidates is null)
                            {
                                unnarrowedGroup = true;
                            }
                            else
                            {
                                candidates ??= new List<uint>(groupCandidates.Length);
                                FlushEveryRowGroups();
                                candidates.AddRange(groupCandidates);
                            }
                        }

                        var groups = reader.Groups;
                        for (int g = 0; g < groups.Length; g++)
                        {
                            ref readonly var grp = ref groups[g];
                            if (grp.EventCount == 0) continue;
                            // Group time bounds are exact, so this drops a group's index sections
                            // without reading them. The reader's per-event window check remains
                            // the correctness gate. NOT counted as a dropped group below: the
                            // reader prunes by the same window itself, so leaving such a group out
                            // of an explicit candidate list buys nothing.
                            if (grp.MaxTs < fromTicks || grp.MinTs > toTicks) continue;

                            groupsConsidered++;

                            // The group's index as this query sees it: the cached memo for (file,
                            // group) when there is a cache — found, or created empty — else a
                            // throwaway one. It answers from what earlier queries worked out and
                            // reads a section of the group, out of the reader opened above, only for
                            // a question it has not been asked before; then it decodes the one
                            // bucket or trigram asked for, never the section (#80). Disposed at the
                            // end of this iteration, `continue` included: the rented sections go
                            // back to the pool, a bloom it had to deserialise is freed, and the
                            // cache learns whether this group was a hit (no section read) and what
                            // the memo grew by.
                            using var index = _indexFactory.OpenGroup(_indexCache, info.FilePath, g, reader);

                            // Phase 1: the bloom gate on the equality hint — and on the level set —
                            // before any inverted or trigram lookup. The verdict is per probed text
                            // and remembered, so a repeated filter pays nothing here; a new value
                            // reads this group's bloom section once.
                            //
                            // "Cheap" is relative: MEASURED by BloomSizingProbe, bloom is 15.6 % of
                            // a prop-dense group's three sections and 26.6 % of a thin one's. What
                            // keeps it on the path is not its size but its answer: it is keyless, so
                            // it rejects a group where the value appears under no property at all —
                            // including groups where the inverted index would only have said "this
                            // property is unknown here, scan". The gate lives in PassesBloomGate
                            // rather than inline: it has to probe every value form the scan would
                            // accept, and an inline copy of that decision is exactly what once let
                            // the index prune rows a scan would have matched.
                            if (hasIndexHint && !PassesBloomGate(filter, index))
                                continue;
                            // No level of the set can be present in this group (no false negatives
                            // in the bloom) — skip it before the inverted lookups.
                            if (levelHints is not null && !AnyLevelMaybePresent(levelHints, index))
                                continue;

                            // Phase 2: trigram offsets and the inverted-index narrowing. The fast
                            // path above already returned for a filter with no hint of any kind, so
                            // every group reaching here has something to look up. A group that is
                            // provably empty for this filter is skipped, not the whole segment —
                            // the next group may still hold matches.
                            if (!TryNarrowWithIndex(filter, index, levelHints, grp.EventCount,
                                                    out var groupCandidates, out bool groupEveryRow))
                                continue;

                            Accept(groupCandidates, groupEveryRow, grp.FirstOrdinal, grp.EventCount);
                        }

                        // Every group rejected ⇒ the segment holds nothing this query can match.
                        if (!anyGroupSurvived) return ValueTask.CompletedTask;

                        uint[]? candidateOffsets;
                        if (unnarrowedGroup)
                        {
                            // One group with no information sends the whole segment to a full scan,
                            // its narrowed groups included — a candidate list would exclude rows
                            // nothing proved absent.
                            candidateOffsets = null;
                        }
                        else if (candidates is not null)
                        {
                            // Something genuinely narrowed, so this segment has to name its rows —
                            // and a whole-group contributor names its contiguous range, generated
                            // directly rather than unioned out of its posting lists.
                            FlushEveryRowGroups();
                            candidateOffsets = candidates.ToArray();
                        }
                        else if (everyRowGroups is null)
                        {
                            candidateOffsets = null;                      // nothing narrowed at all
                        }
                        else if (groupsAccepted == groupsConsidered)
                        {
                            // EVERY group of this segment contributes EVERY one of its rows: the
                            // candidate list would be 0,1,2,…,n-1. Saying so by handing back no
                            // list is the same scan over the same rows, and it is the difference
                            // between a plain block walk and a group-sized uint[] built by a
                            // posting-list union, copied into a List, copied out again, and then
                            // resolved by a binary search per block and a cursor step per row —
                            // to select all of them. This is the level-only filter's normal shape
                            // against a level-split store.
                            candidateOffsets = null;
                        }
                        else
                        {
                            // Some group WAS rejected, so the survivors' rows still have to be
                            // named — but as their plain ranges, with no posting lists involved.
                            candidateOffsets = ContiguousOrdinals(everyRowGroups);
                        }

                        results[i] = new PrefilterResult(info, candidateOffsets, reader, seg.Pin);
                        keep = true;
                    }
                    catch (Exception ex)
                    {
                        // On error, don't skip the segment — fall back to a full scan
                        // so we never silently lose data due to a transient I/O hiccup. The
                        // reader is NOT carried over: whatever went wrong may be the mapping
                        // itself, and the scan's own Open is the retry.
                        //
                        // An OPEN that failed is not that hiccup any more: a pinned file cannot
                        // have been unlinked under the query, and an unpinned one is a file the
                        // catalog served without one behind it (see PinWindow). Named at Warning,
                        // once per file. A failure past the open — reading the index — keeps its
                        // Debug: the full scan reads the blocks without it, and loses nothing.
                        if (reader is null)
                            ReportUnopenable(ex, info, pinned: seg.Pin is not null, scan: false);
                        else
                            _logger.LogDebug(ex, "Index prefilter failed for segment {Id}, falling back to full scan", info.Id);
                        results[i] = new PrefilterResult(info, null, null, seg.Pin);
                    }
                    finally { if (!keep) reader?.Dispose(); }
                    return ValueTask.CompletedTask;
                }).ConfigureAwait(false);
        }
        catch
        {
            // Cancellation, or a fault Parallel.ForEachAsync surfaces after some bodies have
            // already stored their reader. Nothing downstream will ever see those results, so
            // this is the only place that can close them.
            DisposeReaders(results);
            throw;
        }

        var surviving = new List<PrefilterResult>(segInfos.Count);
        for (int i = 0; i < results.Length; i++)
            if (results[i] is { } r)
                surviving.Add(r);
        return surviving;
    }

    /// <summary>Closes every reader a prefilter result still owns. Best-effort and idempotent
    /// per slot — a double dispose on <see cref="SegmentReader"/> is harmless, an unclosed
    /// mapping is a file that cannot be deleted.</summary>
    private static void DisposeReaders(PrefilterResult?[] results)
    {
        for (int i = 0; i < results.Length; i++)
        {
            if (results[i] is { Reader: { } r })
            {
                try { r.Dispose(); } catch { /* best-effort */ }
                results[i] = null;
            }
        }
    }

    /// <summary>
    /// Phase 1 of the prefilter: the bloom gate on the equality hint, through the group's view —
    /// every value form the scan would accept is probed, each verdict answered from the group's
    /// memo when an earlier query already asked and from its bloom section when not. False drops
    /// the group before any inverted or trigram lookup.
    ///
    /// <para>Factored out of the parallel body together with <see cref="TryNarrowWithIndex"/>
    /// so the tests that assert "the index never costs a query its rows" can run the decision
    /// this method makes instead of a copy of it. A copy passed while the original was
    /// reverted, which is the one thing those tests exist to catch — and so is a test that runs a
    /// DIFFERENT overload than production does, which is why there is only this one.</para>
    /// </summary>
    internal static bool PassesBloomGate(CompiledFilter filter, SegmentIndexView index)
    {
        if (!filter.TryGetIndexHint(out _, out object? hintVal)) return true;
        return index.MightContainValue(hintVal);
    }

    /// <summary>
    /// Phase 2: the definitive <c>MightContain</c> gate, trigram offset lookup and inverted
    /// posting-list narrowing, against an already-loaded index.
    ///
    /// <para>Returns false when the segment is provably empty for this filter — the caller
    /// then never reads it. <paramref name="candidates"/> is null for "scan every block" and
    /// otherwise the exact local offsets worth deserialising.</para>
    /// </summary>
    internal static bool TryNarrowWithIndex(CompiledFilter filter, ISegmentIndex idx, out uint[]? candidates)
        => TryNarrowWithIndex(filter, idx, levelHints: null, out candidates);

    /// <summary>True when at least one level of the set might be present per the group's bloom.</summary>
    private static bool AnyLevelMaybePresent((string, object?)[][] levelHints, SegmentIndexView index)
    {
        for (int i = 0; i < levelHints.Length; i++)
            if (index.MightContainValue(levelHints[i][0].Item2))
                return true;
        return false;
    }

    internal static bool TryNarrowWithIndex(
        CompiledFilter filter, ISegmentIndex idx, (string, object?)[][]? levelHints, out uint[]? candidates)
        => TryNarrowWithIndex(filter, idx, levelHints, groupEventCount: 0, out candidates);

    internal static bool TryNarrowWithIndex(
        CompiledFilter filter, ISegmentIndex idx, (string, object?)[][]? levelHints,
        uint groupEventCount, out uint[]? candidates)
        => TryNarrowWithIndex(filter, idx, levelHints, groupEventCount, out candidates, out _);

    /// <param name="groupEventCount">
    /// Events in the group being narrowed, or 0 for "unknown". Used for ONE decision: a level
    /// union whose posting lists already account for every event in the group is the identity,
    /// and intersecting with the identity is work with no result. See below.
    /// </param>
    /// <param name="everyRow">
    /// True when the group contributes EVERY one of its rows — the third narrowing state,
    /// distinct from both "these ordinals" (<paramref name="candidates"/>) and "no information"
    /// (a null <paramref name="candidates"/> with this false). <paramref name="candidates"/> is
    /// left null, because the answer is the contiguous range the caller already knows from the
    /// group directory; materialising it is what this state exists to avoid.
    /// </param>
    internal static bool TryNarrowWithIndex(
        CompiledFilter filter, ISegmentIndex idx, (string, object?)[][]? levelHints,
        uint groupEventCount, out uint[]? candidates, out bool everyRow)
    {
        candidates = null;
        everyRow   = false;

        var trigramHints  = filter.GetTrigramHints();
        var invertedHints = filter.GetInvertedHints();
        bool hasIndexHint = !filter.IsMatchAll && filter.TryGetIndexHint(out _, out _);

        // Definitive inverted-index check (bloom can have false positives)
        if (hasIndexHint
            && filter.TryGetIndexHint(out string prop, out object? val)
            && !idx.MightContain(prop, val))
            return false;

        // EVERY posting list below arrives ASCENDING and DISTINCT — SegmentBitmapCodec's
        // delta encoding cannot decode backwards, and both LookupTrigram and LookupIntersect
        // now hand back merged sorted arrays. So every combination here is a two-pointer merge.
        // It used to be a HashSet per step: `new HashSet<uint>(offsets)` for the trigram seed,
        // one for the inverted result and one for the level union — and for a level-split Error
        // segment that last one is the WHOLE group, ~2 MB of buckets per group per query,
        // built to hash data that was already in order. The sorted-array result is also what
        // lets SegmentReader.ReadEventsAsync replace its clone-and-sort with one scan.
        if (trigramHints.Count > 0)
        {
            uint[]? acc = null;
            foreach (var (_, text) in trigramHints)
            {
                var offsets = idx.LookupTrigram(text);
                if (offsets is null) continue;              // no information from this hint
                acc = acc is null ? offsets : IntersectSorted(acc, offsets);
                if (acc.Length == 0) return false;
            }
            candidates = acc;
        }

        // Inverted-index event-level narrowing: AND posting lists for all equality
        // predicates. This gives exact event offsets within the segment — the reader will
        // only deserialise those events.
        if (invertedHints.Count > 0)
        {
            var invOffsets = idx.LookupIntersect(invertedHints);
            if (invOffsets is not null)
            {
                if (invOffsets.Length == 0) return false;

                candidates = candidates is null ? invOffsets : IntersectSorted(candidates, invOffsets);
                if (candidates.Length == 0) return false;
            }
        }

        // The levels parameter, through the same posting lists a `@l = '...'` filter
        // uses. Union across the set — an event matches ANY allowed level — and only
        // definitive when every level answered (null = the group predates the index or
        // the bucket is unknown: no information, scan). An empty union proves the group
        // holds none of the allowed levels. The per-event level re-check in the scan
        // stays the correctness gate, as with every other narrowing here.
        if (levelHints is not null)
        {
            uint[]? union     = null;
            long    totalOffs = 0;
            bool    definitive = true;
            foreach (var hint in levelHints)
            {
                var offs = idx.LookupIntersect(hint);
                if (offs is null) { definitive = false; break; }
                totalOffs += offs.Length;
                if (offs.Length > 0) union = union is null ? offs : UnionSorted(union, offs);
            }
            if (definitive)
            {
                if (union is null) return false;

                // LEVEL-PURE GROUP: an event has exactly one level, so a group's level posting
                // lists are disjoint and their lengths sum to at most its event count. Equality
                // therefore proves the union IS the group — the normal case for a level-split
                // Error segment, where `@l = Error` names all ~100k of its rows. Intersecting
                // an existing candidate set with the identity cannot remove anything, so the
                // merge over the whole group is skipped and the candidates stand.
                //
                // Only when something else already narrowed. With NO other hint the union is
                // still the whole answer this group contributes — and the honest way to say so
                // is `everyRow`, not the array. Returning the array made a level-only filter
                // (`@l != 'Error'`, which the compiled filter now derives a level set from even
                // when the request names none) pay, per group and per query, for a group-sized
                // uint[] built by a posting-list union, copied into a List and copied out again
                // — three copies of the group — which the reader then resolved with two binary
                // searches per block and a cursor step per row, to select 100 % of them.
                // Returning null instead is not available here: null means UNNARROWED, and one
                // unnarrowed group sends the whole segment, its narrowed groups included, to a
                // full scan. Hence the third state.
                bool identity = groupEventCount != 0 && totalOffs == groupEventCount;
                if (candidates is null)
                {
                    if (identity) everyRow = true;
                    else          candidates = union;
                }
                else if (!identity)
                {
                    candidates = IntersectSorted(candidates, union);
                    if (candidates.Length == 0) return false;
                }
            }
        }

        return true;
    }

    /// <summary>
    /// The ordinals of whole-group contributors, as plain ascending ranges. Groups are visited
    /// in ordinal order, so concatenating their ranges is already sorted — no posting list is
    /// read and no merge runs to produce what the group directory already states.
    /// </summary>
    private static uint[] ContiguousOrdinals(List<(uint First, uint Count)> ranges)
    {
        long total = 0;
        for (int r = 0; r < ranges.Count; r++) total += ranges[r].Count;

        var outp = new uint[total];
        int k = 0;
        for (int r = 0; r < ranges.Count; r++)
        {
            var (first, count) = ranges[r];
            for (uint o = 0; o < count; o++) outp[k++] = first + o;
        }
        return outp;
    }

    /// <summary>Intersects two ascending, distinct arrays into a new ascending array.</summary>
    private static uint[] IntersectSorted(uint[] a, uint[] b)
    {
        var outp = new uint[Math.Min(a.Length, b.Length)];
        int i = 0, j = 0, k = 0;
        while (i < a.Length && j < b.Length)
        {
            uint x = a[i], y = b[j];
            if      (x < y) i++;
            else if (x > y) j++;
            else { outp[k++] = x; i++; j++; }
        }
        return k == outp.Length ? outp : outp[..k];
    }

    /// <summary>Unions two ascending, distinct arrays into a new ascending array.</summary>
    private static uint[] UnionSorted(uint[] a, uint[] b)
    {
        var outp = new uint[a.Length + b.Length];
        int i = 0, j = 0, k = 0;
        while (i < a.Length && j < b.Length)
        {
            uint x = a[i], y = b[j];
            if      (x < y) outp[k++] = a[i++];
            else if (x > y) outp[k++] = b[j++];
            else { outp[k++] = x; i++; j++; }
        }
        while (i < a.Length) outp[k++] = a[i++];
        while (j < b.Length) outp[k++] = b[j++];
        return k == outp.Length ? outp : outp[..k];
    }

    // ── Segment scan ──────────────────────────────────────────────────────────

    /// <param name="borrowed">
    /// The reader the prefilter already opened for this segment, or null. When non-null this
    /// method does NOT dispose it — <see cref="MergeSourcesAsync"/> owns every prefilter reader
    /// and closes them all in one place, because a segment that never primes has no iterator to
    /// close it. Opening the file twice per segment per query was the cost being removed: a
    /// FileInfo stat, a CreateFromFile, a CreateViewAccessor over the whole file and a
    /// block-index parse, 40 of them for a 20-segment query.
    /// </param>
    /// <param name="pin">
    /// The handle pinning the segment's file since the snapshot, or null; what this method maps its
    /// own reader from when <paramref name="borrowed"/> is null. Borrowed as well: the query
    /// releases it, after this iterator's reader has gone.
    /// </param>
    private async IAsyncEnumerable<LogEvent> ScanSegmentAsync(
        SegmentInfo info,
        CompiledFilter filter,
        HashSet<Ameto.Core.LogLevel>? levels,
        uint[]? candidateOffsets,
        SegmentReader? borrowed,
        SafeFileHandle? pin,
        DateTimeOffset? from,
        DateTimeOffset? to,
        long? afterTs,
        Ameto.Core.EventId? afterId,
        bool reversed,
        ScanPace pace,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
    {
        SegmentReader? opened = null;
        if (borrowed is null)
        {
            try
            {
                opened = OpenSegment(info, pin);
            }
            catch (Exception ex)
            {
                // The segment's rows are lost to this query. This used to happen without a word —
                // a bare `catch { yield break; }` — which is how a merge under the query could take
                // 202 of 252 rows from a filtered search and leave nothing in the log (#114). The
                // pin closes that race; whatever still fails here is named (ReportUnopenable).
                ReportUnopenable(ex, info, pinned: pin is not null, scan: true);
                yield break;
            }
        }

        var reader = borrowed ?? opened!;
        try
        {
            // Every segment is v2+: events inside each block are sorted by @t and
            // blocks themselves are sorted, so we can stream lazily without buffering.
            await foreach (var ev in reader.ReadEventsAsync(candidateOffsets, from, to, reversed, ct))
            {
                if (pace.Due()) await ScanPace.Yield();
                if (!InWindow(ev, from, to)) continue;
                if (!AfterCursor(ev, afterTs, afterId, !reversed)) continue;
                if (levels != null && !levels.Contains(ev.Level)) continue;
                if (!filter.Matches(ev)) continue;
                yield return ev;
            }
        }
        finally { opened?.Dispose(); }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static bool InWindow(LogEvent ev, DateTimeOffset? from, DateTimeOffset? to)
    {
        if (from.HasValue && ev.Timestamp < from.Value) return false;
        if (to.HasValue   && ev.Timestamp > to.Value)   return false;
        return true;
    }

    /// <summary>(timestamp, eventId) pagination cursor — see <see cref="QueryCursor.After"/>.</summary>
    private static bool AfterCursor(LogEvent ev, long? afterTs, Ameto.Core.EventId? afterId, bool forward)
        => QueryCursor.After(ev.Timestamp.UtcTicks, ev.Id.RawValue, afterTs, afterId?.RawValue, forward);
}
