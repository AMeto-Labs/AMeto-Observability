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
///   3. Candidate narrowing (<see cref="Narrow"/>), per index group and in two parts. What the
///      posting lists' COUNTS decide is decided with step 2: a group that holds no match is
///      dropped, one whose lists name nearly all of it contributes every row, and a short list
///      is named there. A LONG list — trigram (<see cref="ISegmentIndex.LookupTrigram"/>) aside,
///      which is still built with step 2 — is, for the first batch of a query, built only when
///      the scan reaches its group (<see cref="SegmentPlan"/>): the candidate ordinals of that
///      group, by which the reader skips blocks without candidates and materialises only
///      candidate rows. Later batches build theirs as they go (<see cref="MergeSourcesAsync"/>).
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
    ///
    /// <para>A materialised hot row carries its exception as a view into the tier's arena,
    /// valid while <paramref name="hotReader"/> is — which is the whole of this enumeration,
    /// and is what lets the filter read <c>@x</c> on every candidate without a copy. A row that
    /// passes leaves the snapshot: the page is serialised after <see cref="ExecuteAsync"/>'s
    /// finally has released the reader, by when the tier may have flushed and freed its blocks,
    /// so the row makes its bytes its own here, once, for the rows kept alone.</para>
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
            ev.OwnRawBytes();   // the row outlives the snapshot; its exception was a view into the tier until now
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
        // Once for the query: every prefilter batch asks with them, and so does a group whose
        // rows are named only when the scan reaches it.
        var  levelHints  = LevelHints(levels);
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

                        // THE FIRST BATCH LEAVES LONG POSTING LISTS FOR THE SCAN; THE REST DO NOT.
                        // A page is usually filled out of the first batch — that is why it is one
                        // wave of the prefilter's parallelism and no more — and for that batch a
                        // list built up front is, for all but the group or two the page reads,
                        // built to be thrown away (see SegmentPlan for what that cost).
                        //
                        // A page that comes back for a SECOND batch has said something about
                        // itself: its rows are thin on the ground, or there are none, and the
                        // prefilter is going to walk on through the window. From there the lists
                        // are built as each batch is checked, eight segments at a time, as every
                        // batch's used to be. Left to the scan they would be built one group at a
                        // time on the consumer's thread — and what two lists prove only together,
                        // an intersection that comes out empty, would be learned a segment at a
                        // time, by a search whose whole cost is finding that out.
                        int nameAtMost = checkedUpTo == 0 ? NameUpFront : int.MaxValue;

                        var passed = await PrefilterSegmentsAsync(
                            new ArraySegment<WindowSegment>(order, checkedUpTo, take),
                            filter, levels, levelHints, nameAtMost, fromTicks, toTicks, ct);
                        // Survivors come back in input order and the input is the next slice
                        // of the priming order, so appending keeps the queue ordered.
                        survivors.AddRange(passed);
                        checkedUpTo += take;
                        batch        = Math.Min(batch * 2, order.Length);
                        continue;
                    }

                    if (!CouldBeat(survivors[next].Info)) return;

                    var (segInfo, segPlan, segReader, segPin) = survivors[next++];
                    // The reader is BORROWED — the finally below owns every one of them,
                    // primed or not, so the scan must not dispose what it did not open. So is
                    // the pin, which the scan maps its own reader from when there is none.
                    var stream = ScanSegmentAsync(segInfo, filter, levels, segPlan, segReader, segPin,
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
    /// Result of the per-segment prefilter: the segment to scan, what the index decided about
    /// each of its groups (<paramref name="Plan"/>; null = read every row of the segment), and
    /// the reader the prefilter already opened.
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
        SegmentInfo Info, SegmentPlan? Plan, SegmentReader? Reader, SafeFileHandle? Pin);

    /// <summary>
    /// What the index decided about each group of ONE surviving segment, and what it has left
    /// undecided: the plan the scan reads the segment by (<see cref="SegmentReader.ReadGroupsAsync"/>),
    /// one group at a time, in the order it walks them.
    ///
    /// <para>A group is in one of four states. Three are answers the prefilter gave while it had
    /// the group's index open: nothing of it is read (no match is possible, it lies outside the
    /// window, or it is empty — the state a slot starts in), all of it is read, or the rows named
    /// are. The fourth is a question put off, by a query's first prefilter batch only: the
    /// group's rows have to be named, the posting lists that name them are long, and they are
    /// not built until the scan asks for this group (<see cref="NameLate"/>).</para>
    ///
    /// <para><b>Why put it off.</b> The prefilter works a batch of segments before the first row
    /// is returned, and a page is mostly filled by the newest one or two. Named up front, a list
    /// cost twelve to sixteen bytes for every row it named — decoded, copied out of the index,
    /// gathered into one list for the segment and copied out of that — in every group of every
    /// segment of the batch: 0.4-1.0 MB for a page of fifty rows over eight groups of 10 000,
    /// 1.4-3.9 MB over eight of 40 000 (<c>FirstPageIndexCostProbe</c>), and growing with the
    /// group. Left to the scan, a list is built for the groups the page reads, and a segment
    /// with many groups pays for the one the page ends in rather than for all of them.</para>
    ///
    /// <para><b>Why per group.</b> One candidate list for a whole segment can only name rows. A
    /// group that contributes every row then has to be spelled as each of its ordinals, and a
    /// group the index knows nothing about cannot be spelled at all — it used to send the whole
    /// segment, narrowed groups included, to a full scan. Here each group keeps the answer its
    /// own index gave.</para>
    ///
    /// <para>Filled by one prefilter worker, then read by the scan's thread, never both at once:
    /// the prefilter batch has completed before any of its survivors is primed. Each slot is
    /// read once and emptied, so a candidate list — or the memo a cacheless query carries to the
    /// second asking — is not kept for the rest of the query by a group already read.</para>
    /// </summary>
    private sealed class SegmentPlan(
        QueryExecutor owner, SegmentInfo info, SegmentReader reader,
        CompiledFilter filter, (string, object?)[][]? levelHints) : ISegmentGroupPlan
    {
        private enum Part : byte { None, Whole, Named, Late }

        private struct Slot
        {
            public Part                 Part;
            public ReadOnlyMemory<uint> Candidates;   // Named
            public SegmentIndexReader?  Memo;         // Late, and only without an index cache
        }

        private readonly Slot[] _groups = new Slot[reader.Groups.Length];

        /// <summary>Every row of group <paramref name="group"/> is read: the index said so, or said nothing.</summary>
        public void ReadWhole(int group) => _groups[group].Part = Part.Whole;

        /// <summary>
        /// Only the rows at <paramref name="candidates"/> are read. The list may be the index
        /// cache's own (<see cref="ISegmentIndex.TryLookupIntersect"/>), which is why it is
        /// read-only memory from here to the reader's row walk.
        /// </summary>
        public void ReadOnly(int group, ReadOnlyMemory<uint> candidates) =>
            _groups[group] = new Slot { Part = Part.Named, Candidates = candidates };

        /// <summary>
        /// The group's rows are named when the scan reaches it. <paramref name="memo"/> is what
        /// the prefilter's view of the group learned, when no cache is there to keep it
        /// (<see cref="SegmentIndexView.UncachedMemo"/>).
        /// </summary>
        public void NameLater(int group, SegmentIndexReader? memo) =>
            _groups[group] = new Slot { Part = Part.Late, Memo = memo };

        GroupRead ISegmentGroupPlan.Read(int group)
        {
            var slot = _groups[group];
            _groups[group] = default;

            return slot.Part switch
            {
                Part.Whole => GroupRead.Whole,
                Part.Named => GroupRead.Only(slot.Candidates),
                Part.Late  => owner.NameLate(info, reader, group, filter, levelHints, slot.Memo),
                _          => GroupRead.Skip,
            };
        }
    }

    /// <summary>
    /// Index groups whose rows the prefilter left to be named by the scan
    /// (<see cref="SegmentPlan.NameLater"/>), counted for the plans a scan reads — not for one
    /// the prefilter then dropped, whose groups are read whole — process-wide, with the caveats
    /// of <see cref="Pins"/>: a test that reads it needs the assembly's parallelisation off.
    /// </summary>
    internal static long GroupsLeftForLater;

    /// <summary>
    /// The other half of <see cref="GroupsLeftForLater"/>: groups a scan reached and named
    /// (<see cref="NameLate"/>). The difference is what a query did not pay for — the lists of
    /// the groups its page never got to — and is the number a test states that saving by.
    /// </summary>
    internal static long GroupsNamedLate;

    /// <summary>
    /// The second asking of a group the prefilter left for later: everything <see cref="Narrow"/>
    /// has to say about it, with no limit on the lists it builds — the scan is here, and needs
    /// them now. The answers already taken are taken again, off the memo this time: the group's
    /// catalog, the lists' counts and where the lists lie, so that what is read of the file is
    /// the lists themselves (<see cref="SegmentIndexReader"/>).
    ///
    /// <para>A failure is the prefilter's kind of failure and gets its answer: the group is read
    /// whole. The index may cost a query its acceleration, never its rows.</para>
    /// </summary>
    private GroupRead NameLate(
        SegmentInfo info, SegmentReader reader, int group,
        CompiledFilter filter, (string, object?)[][]? levelHints, SegmentIndexReader? carried)
    {
        Interlocked.Increment(ref GroupsNamedLate);
        try
        {
            using var index = _indexFactory.OpenGroup(_indexCache, info.FilePath, group, reader, carried);
            return Narrow(filter, index, levelHints, reader.Groups[group].EventCount, int.MaxValue, out var candidates) switch
            {
                GroupVerdict.Dropped => GroupRead.Skip,
                GroupVerdict.Named   => GroupRead.Only(candidates),
                _                    => GroupRead.Whole,
            };
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex,
                "Index narrowing failed for group {Group} of segment {Id}, reading the group whole", group, info.Id);
            return GroupRead.Whole;
        }
    }

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
    /// <param name="levelHints">
    /// The level set as index hints (<see cref="LevelHints"/>), or null when the query has none.
    /// </param>
    /// <param name="nameAtMost">
    /// The longest posting list this batch builds for a group while it has the group open; a
    /// group that needs a longer one is left for the scan to name (<see cref="SegmentPlan"/>).
    /// <see cref="NameUpFront"/> for a query's first batch and no limit after it — see
    /// <see cref="MergeSourcesAsync"/> for why the two differ.
    /// </param>
    private async Task<List<PrefilterResult>> PrefilterSegmentsAsync(
        IReadOnlyList<WindowSegment>  segInfos,
        CompiledFilter                filter,
        HashSet<Ameto.Core.LogLevel>? levels,
        (string, object?)[][]?        levelHints,
        int                           nameAtMost,
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

        bool hasIndexHint = !filter.IsMatchAll && filter.TryGetIndexHint(out _, out _);

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

                        // What the index says about each group of this segment, kept PER GROUP:
                        // the plan the scan reads the segment by (see SegmentPlan). Made at the
                        // first group that survives — so a null plan below is a segment with no
                        // survivor — and dropped again if all it turns out to say is "read
                        // everything".
                        //
                        // It replaced one candidate list for the whole segment, gathered here from
                        // the groups' posting lists: a List the groups were appended to and the
                        // array copied out of it, eight bytes a named row on top of the lists
                        // themselves, for every segment of the batch, before the first row was
                        // returned — and a group that contributed every row had to be written
                        // into it ordinal by ordinal once any other group named its own.
                        SegmentPlan? plan = null;
                        // True once some group is NOT simply read whole: dropped, named, or left
                        // to be named. Until then the segment's answer is "every row".
                        bool narrowed = false;
                        // Groups the plan leaves for the scan to name (GroupsLeftForLater).
                        int leftForLater = 0;

                        var groups = reader.Groups;
                        for (int g = 0; g < groups.Length; g++)
                        {
                            ref readonly var grp = ref groups[g];
                            if (grp.EventCount == 0) continue;
                            // Group time bounds are exact, so this drops a group's index sections
                            // without reading them. The reader's per-event window check remains
                            // the correctness gate. NOT counted as narrowing: the reader prunes by
                            // the same window itself, so a plan made only to leave such a group
                            // out would buy nothing. In a plan that exists for another reason the
                            // group's slot stays empty and it is not read.
                            if (grp.MaxTs < fromTicks || grp.MinTs > toTicks) continue;

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
                            {
                                narrowed = true;
                                continue;
                            }
                            // No level of the set can be present in this group (no false negatives
                            // in the bloom) — skip it before the inverted lookups.
                            if (levelHints is not null && !AnyLevelMaybePresent(levelHints, index))
                            {
                                narrowed = true;
                                continue;
                            }

                            // Phase 2: what the group's posting lists say — by their counts first,
                            // and by the lists themselves only as far as this batch may build them
                            // (nameAtMost: short ones in a query's first batch, any after it).
                            // The fast path above already returned for a filter with no hint of any
                            // kind, so every group reaching here has something to ask. A group that
                            // is provably empty for this filter is skipped, not the whole segment —
                            // the next group may still hold matches.
                            var verdict = Narrow(filter, index, levelHints, grp.EventCount, nameAtMost,
                                                 out var groupCandidates);
                            if (verdict == GroupVerdict.Dropped)
                            {
                                narrowed = true;
                                continue;
                            }

                            plan ??= new SegmentPlan(this, info, reader, filter, levelHints);
                            switch (verdict)
                            {
                                // The group's own ordinals, ascending as Narrow merges them — which
                                // is NOT a promise the reader takes on trust: it verifies the order
                                // before its two-pointer walk and sorts a copy when it has to
                                // (SegmentReader.InWalkOrder). That check is the contract. The walk
                                // advances a single cursor through the candidates alongside
                                // ascending row ordinals, so one descending pair makes it step past
                                // a candidate it will never come back to, and the query silently
                                // loses rows the index proved it should return.
                                case GroupVerdict.Named:
                                    plan.ReadOnly(g, groupCandidates);
                                    narrowed = true;
                                    break;

                                // The rows have to be named and the lists are long: left for the
                                // scan, which names them if it gets to this group. With no cache to
                                // keep what this view has just learned of the group, the plan
                                // carries it to that second asking.
                                case GroupVerdict.Deferred:
                                    plan.NameLater(g, index.UncachedMemo);
                                    leftForLater++;
                                    narrowed = true;
                                    break;

                                // Every row — or NO INFORMATION: a group whose index sections are
                                // absent (a WAL-recovery flush that ran before the builder was
                                // wired), or whose index does not know the property. Nothing was
                                // proved about its rows, so all of them are read; and that is all
                                // it means. It used to send the whole SEGMENT to a full scan,
                                // because one list for the segment could not say "all of this
                                // group" without naming every row of it.
                                default:
                                    plan.ReadWhole(g);
                                    break;
                            }
                        }

                        // Every group rejected ⇒ the segment holds nothing this query can match.
                        if (plan is null) return ValueTask.CompletedTask;

                        // EVERY group that could contribute contributes EVERY one of its rows:
                        // the plan would say "whole" for each. Saying so with no plan is the
                        // same scan over the same rows — the plain block walk — and it is the
                        // level-only filter's normal shape against a level-split store.
                        //
                        // And a plan is walked group by group, which reads every row only where
                        // the groups ARE the file (SegmentReader.GroupsCoverBlocks). Every file
                        // this build writes is such a file; for one that is not, nothing is
                        // assumed, and every row of it is read.
                        var scanPlan = narrowed && reader.GroupsCoverBlocks ? plan : null;
                        // Groups left for later are counted once the plan that leaves them is
                        // the one the scan will read: a plan dropped here, or by a failure below,
                        // has its groups read whole, never named.
                        if (scanPlan is not null && leftForLater > 0)
                            Interlocked.Add(ref GroupsLeftForLater, leftForLater);
                        results[i] = new PrefilterResult(info, scanPlan, reader, seg.Pin);
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

    /// <summary>True when at least one level of the set might be present per the group's bloom.</summary>
    private static bool AnyLevelMaybePresent((string, object?)[][] levelHints, SegmentIndexView index)
    {
        for (int i = 0; i < levelHints.Length; i++)
            if (index.MightContainValue(levelHints[i][0].Item2))
                return true;
        return false;
    }

    /// <summary>
    /// The level set as index hints, one single-predicate list per level, or null when the query
    /// names no level.
    ///
    /// <para>The levels PARAMETER prunes through the same index the <c>@l = '...'</c> FILTER
    /// uses. It used to prune nothing: an errors-only dashboard query decoded every block of
    /// every surviving segment only for the per-event level check to reject nearly all of it.
    /// Per group, each level of the set either has a posting list, is provably absent, or
    /// answers "no information" (a pre-index segment) — the union across the set is definitive
    /// unless any level answers the latter.</para>
    /// </summary>
    private static (string, object?)[][]? LevelHints(HashSet<Ameto.Core.LogLevel>? levels)
    {
        if (levels is not { Count: > 0 }) return null;

        var hints = new (string, object?)[levels.Count][];
        int i = 0;
        foreach (var level in levels)
            hints[i++] = [(ClefFields.Level, level.ToSeqString())];
        return hints;
    }

    /// <summary>What one index group's index says about a filter: the answer <see cref="Narrow"/> gives.</summary>
    internal enum GroupVerdict : byte
    {
        /// <summary>No row of the group can match. It is not read.</summary>
        Dropped,

        /// <summary>
        /// The group contributes EVERY one of its rows: its posting lists name all of it, or so
        /// nearly all that naming the rows is not worth a list (<see cref="NamesNearlyAllOf"/>).
        /// A state and not a list, because the answer is the contiguous range the group directory
        /// already states; materialising it is what the state exists to avoid.
        /// </summary>
        EveryRow,

        /// <summary>Exactly the rows named are candidates.</summary>
        Named,

        /// <summary>
        /// The index has nothing to say about the group's rows — it predates the index, or does
        /// not know the property. Read like <see cref="EveryRow"/>, and a different statement:
        /// that one is a fact about the group, this one is the absence of any.
        /// </summary>
        NoInformation,

        /// <summary>
        /// The rows have to be named, and a list that names them is longer than the caller let
        /// be built now. Nothing more is settled by it: asked again without the limit, the group
        /// may turn out to be any of the others — an intersection of two long lists can be empty.
        /// </summary>
        Deferred,
    }

    /// <summary>
    /// The longest posting list a query's FIRST prefilter batch builds while it has a group
    /// open; a longer one waits for the scan to reach the group (<see cref="SegmentPlan"/>).
    /// Later batches build whatever they need (see <see cref="MergeSourcesAsync"/>).
    ///
    /// <para>It is the index's own line between a list its counting walk decodes and one it only
    /// counts, so that what is named up front is what the count has already decoded — see
    /// <see cref="SegmentIndexReader.ShortListPostings"/>, which says what a list this short
    /// costs and why that is not worth a second visit to the group.</para>
    /// </summary>
    internal const int NameUpFront = SegmentIndexReader.ShortListPostings;

    /// <summary>
    /// <see cref="Narrow"/> with no limit on the lists it builds, as the three answers it had
    /// before there was one to set: false when the group is provably empty for this filter — the
    /// caller then never reads it — and otherwise <paramref name="candidates"/>, null for "read
    /// every row" and otherwise the exact local offsets worth deserialising.
    /// </summary>
    internal static bool TryNarrowWithIndex(CompiledFilter filter, ISegmentIndex idx, out uint[]? candidates)
        => TryNarrowWithIndex(filter, idx, levelHints: null, out candidates);

    internal static bool TryNarrowWithIndex(
        CompiledFilter filter, ISegmentIndex idx, (string, object?)[][]? levelHints, out uint[]? candidates)
        => TryNarrowWithIndex(filter, idx, levelHints, groupEventCount: 0, out candidates);

    internal static bool TryNarrowWithIndex(
        CompiledFilter filter, ISegmentIndex idx, (string, object?)[][]? levelHints,
        uint groupEventCount, out uint[]? candidates)
        => TryNarrowWithIndex(filter, idx, levelHints, groupEventCount, out candidates, out _);

    /// <param name="everyRow">
    /// True for <see cref="GroupVerdict.EveryRow"/>, which a null <paramref name="candidates"/>
    /// alone does not tell from <see cref="GroupVerdict.NoInformation"/>.
    /// </param>
    internal static bool TryNarrowWithIndex(
        CompiledFilter filter, ISegmentIndex idx, (string, object?)[][]? levelHints,
        uint groupEventCount, out uint[]? candidates, out bool everyRow)
    {
        var verdict = Narrow(filter, idx, levelHints, groupEventCount, int.MaxValue, out var rows);
        candidates = verdict == GroupVerdict.Named ? rows.ToArray() : null;
        everyRow   = verdict == GroupVerdict.EveryRow;
        return verdict != GroupVerdict.Dropped;
    }

    /// <summary>
    /// Phase 2 of the prefilter: what one group's index says about the filter — the definitive
    /// <c>MightContain</c> gate, the posting lists' counts, and then, only where still needed and
    /// allowed, the lists: trigram offsets, the inverted hints' intersection, the level union.
    ///
    /// <para><b>Counted before named.</b> Every equality hint and every level of the set is first
    /// asked how many rows its list holds (<see cref="ISegmentIndex.CountAtMost"/>), which the
    /// index answers without building the list. Three things are settled by the numbers alone:
    /// a list that holds no row drops the group; a list that names nearly all of the group
    /// (<see cref="NamesNearlyAllOf"/>) narrows nothing worth having and is left out, so that
    /// the group either contributes every row or is narrowed by the other hints alone; and a list
    /// longer than <paramref name="nameAtMost"/> is not built by this call at all
    /// (<see cref="GroupVerdict.Deferred"/>).</para>
    ///
    /// <para>All three are about what a list COSTS when it buys little. Naming a row costs four
    /// bytes to decode and four to hand over, and the lists that cost most are the ones that say
    /// least: <c>@l = 'Error'</c> over the Error segments of a level-split store is every row of
    /// every group, the rows that carry an exception are nearly every row of the same groups, and
    /// both were decoded, copied and compared with the group's size to learn that. A full read
    /// hides it, since the rows it returns dwarf the lists. A PAGE does not: the prefilter works
    /// a batch of segments before the first row is returned, so fifty rows paid for the lists of
    /// every group in the batch (<c>FirstPageIndexCostProbe</c>). Hence the limit — the
    /// prefilter passes <see cref="NameUpFront"/> for a query's first batch, and the scan, when
    /// it reaches a group left that way, asks again with none.</para>
    ///
    /// <para><b>One function, two callers.</b> The limit is the only difference between the two
    /// askings, deliberately: what drops a group, what makes it whole and which rows it names are
    /// decided by the same lines whichever asks, so the answer cannot depend on when it was
    /// given. A filter with a SUBSTRING predicate is the exception to the limit and not to the
    /// rest: its trigram lists have no count to ask for, and the group they prove empty must be
    /// dropped here, in the batch, rather than one segment at a time by the scan — so such a
    /// filter is narrowed whole in one go, as every filter used to be.</para>
    ///
    /// <para>SAFE in every direction it errs: "every row" and "no information" are supersets, a
    /// deferred group is asked again, and the scan re-checks each row it is handed against the
    /// whole filter. Only <see cref="GroupVerdict.Dropped"/> and <see cref="GroupVerdict.Named"/>
    /// can cost a row, and both rest on what they always did — a posting list (or its count of
    /// zero) from an index that knows the property.</para>
    /// </summary>
    /// <param name="groupEventCount">
    /// Events in the group being narrowed, or 0 for "unknown" — in which case no list is ever
    /// "nearly all" and every one is named.
    /// </param>
    /// <param name="nameAtMost">
    /// The longest posting list this call may build; <see cref="int.MaxValue"/> for no limit.
    /// </param>
    /// <param name="candidates">
    /// The rows named, for <see cref="GroupVerdict.Named"/>; empty for every other verdict.
    /// READ-ONLY, and not for the look of it: for a single equality whose value the group files
    /// once — one bucket, one encoding, the usual case — it is the posting list as the index
    /// keeps it, shared with every query that asks the same
    /// (<see cref="ISegmentIndex.TryLookupIntersect"/>); the copy that used to stand between
    /// them was four bytes for every row named, in every group a query named rows in. A value
    /// filed several ways (a property and its alias, several encodings of a literal) is their
    /// union, made for the call.
    /// </param>
    internal static GroupVerdict Narrow(
        CompiledFilter filter, ISegmentIndex idx, (string, object?)[][]? levelHints,
        uint groupEventCount, int nameAtMost, out ReadOnlyMemory<uint> candidates)
    {
        candidates = default;

        var trigramHints  = filter.GetTrigramHints();
        var invertedHints = filter.GetInvertedHints();

        // Definitive inverted-index check (bloom can have false positives)
        if (!filter.IsMatchAll
            && filter.TryGetIndexHint(out string prop, out object? val)
            && !idx.MightContain(prop, val))
            return GroupVerdict.Dropped;

        // ── What the counts decide ───────────────────────────────────────────────
        //
        // The inverted hints are ANDed, so each one's list bounds the answer by itself: one that
        // holds no row proves the group empty, and one that names nearly all of the group can be
        // left out of the intersection, which without it is a superset the scan trims. A hint
        // whose property the index does not know (null) has no bucket to count and none to
        // intersect: LookupIntersect passes over it below, as it always has.
        ulong nearlyAll = 0;       // by position in invertedHints: lists not worth building
        bool  anyToName = false;   // some hint is left whose list has to be built
        int   longest   = 0;       // the longest list that would be
        for (int i = 0; i < invertedHints.Count; i++)
        {
            var (property, value) = invertedHints[i];
            if (idx.CountAtMost(property, value) is not int listed) continue;
            if (listed == 0) return GroupVerdict.Dropped;

            // The mask has sixty-four positions; a hint past them is named like any other.
            if (i < 64 && NamesNearlyAllOf(groupEventCount, listed))
            {
                nearlyAll |= 1UL << i;
                continue;
            }
            anyToName = true;
            longest   = Math.Max(longest, listed);
        }

        // The levels parameter, through the same posting lists a `@l = '...'` filter uses. An
        // event matches ANY allowed level, so the set is a union — and only definitive when
        // every level answered (null = the group predates the index or the bucket is unknown:
        // no information, scan).
        //
        // LEVEL-PURE GROUP: an event has exactly one level, so a group's level posting lists
        // are disjoint and their counts sum to the rows the union names. A sum that reaches the
        // group — the normal case for a level-split Error segment, where `@l = Error` names all
        // ~100k of its rows — means the union IS the group, and it is never built: intersecting
        // with the identity removes nothing, and the union alone would be a group-sized array
        // to select all of it. A sum of zero proves the group holds none of the allowed levels.
        bool levelsNearlyAll = false, levelsToName = false;
        if (levelHints is not null)
        {
            long total      = 0;
            int  longestOne = 0;
            bool definitive = true;
            foreach (var hint in levelHints)
            {
                if (idx.CountAtMost(hint[0].Item1, hint[0].Item2) is not int listed)
                {
                    definitive = false;
                    break;
                }
                total     += listed;
                longestOne = Math.Max(longestOne, listed);
            }
            if (definitive)
            {
                if (total == 0) return GroupVerdict.Dropped;

                levelsNearlyAll = NamesNearlyAllOf(groupEventCount, (int)Math.Min(total, int.MaxValue));
                levelsToName    = !levelsNearlyAll;
                if (levelsToName) longest = Math.Max(longest, longestOne);
            }
        }

        // ── Not now ──────────────────────────────────────────────────────────────
        //
        // Everything above was answered from counts. What is left needs a list, and the caller
        // has said how long a list it will have built at this point. Not for a filter with a
        // substring predicate — see the summary.
        if (trigramHints.Count == 0 && longest > nameAtMost) return GroupVerdict.Deferred;

        // ── The lists ────────────────────────────────────────────────────────────
        //
        // EVERY posting list below arrives ASCENDING and DISTINCT — SegmentBitmapCodec's
        // delta encoding cannot decode backwards, and both LookupTrigram and LookupIntersect
        // now hand back merged sorted arrays. So every combination here is a two-pointer merge.
        // It used to be a HashSet per step: `new HashSet<uint>(offsets)` for the trigram seed,
        // one for the inverted result and one for the level union — and for a level-split Error
        // segment that last one is the WHOLE group, ~2 MB of buckets per group per query,
        // built to hash data that was already in order. The sorted-array result is also what
        // lets the segment reader replace its clone-and-sort with one scan.
        //
        // `rows` is the answer so far and `named` whether there is one: a list that names no
        // row never gets this far (it drops the group), so the two cannot be told apart by the
        // list being empty.
        ReadOnlyMemory<uint> rows  = default;
        bool                 named = false;

        if (trigramHints.Count > 0)
        {
            uint[]? acc = null;
            foreach (var (_, text) in trigramHints)
            {
                var offsets = idx.LookupTrigram(text);
                if (offsets is null) continue;              // no information from this hint
                acc = acc is null ? offsets : IntersectSorted(acc, offsets);
                if (acc.Length == 0) return GroupVerdict.Dropped;
            }
            if (acc is not null)
            {
                rows  = acc;
                named = true;
            }
        }

        // Inverted-index event-level narrowing: AND posting lists for the equality predicates
        // whose lists are worth having. This gives exact event offsets within the group — the
        // reader will only deserialise those events.
        if (anyToName)
        {
            var toName = nearlyAll == 0 ? invertedHints : filter.GetInvertedHintsWithout(nearlyAll);
            if (idx.TryLookupIntersect(toName, out var invOffsets))
            {
                if (invOffsets.IsEmpty) return GroupVerdict.Dropped;

                if (named)
                {
                    rows = IntersectSorted(rows.Span, invOffsets.Span);
                    if (rows.IsEmpty) return GroupVerdict.Dropped;
                }
                else
                {
                    rows  = invOffsets;
                    named = true;
                }
            }
        }

        // The level union, where the allowed levels are a genuine part of the group. The
        // per-event level re-check in the scan stays the correctness gate, as with every other
        // narrowing here.
        if (levelsToName)
        {
            ReadOnlyMemory<uint> union = default;
            bool                 any   = false;
            foreach (var hint in levelHints!)
            {
                if (!idx.TryLookupIntersect(hint, out var offs))
                {
                    // The count said this bucket was there. An index that gives no list for it
                    // has no information after all, and the levels narrow nothing.
                    levelsToName = false;
                    break;
                }
                if (offs.IsEmpty) continue;
                union = any ? UnionSorted(union.Span, offs.Span) : offs;
                any   = true;
            }
            if (levelsToName)
            {
                if (!any) return GroupVerdict.Dropped;

                if (named)
                {
                    rows = IntersectSorted(rows.Span, union.Span);
                    if (rows.IsEmpty) return GroupVerdict.Dropped;
                }
                else
                {
                    rows  = union;
                    named = true;
                }
            }
        }

        if (named)
        {
            candidates = rows;
            return GroupVerdict.Named;
        }

        // Nothing named. If that is because the lists that could have named rows name the group
        // — an inverted hint by its count, the level set by its sum — the group contributes all
        // of its rows, which is a statement about it; otherwise the index had nothing to say.
        return nearlyAll != 0 || levelsNearlyAll ? GroupVerdict.EveryRow : GroupVerdict.NoInformation;
    }

    /// <summary>
    /// Whether a posting list of at most <paramref name="rows"/> rows names so much of a group
    /// of <paramref name="groupEventCount"/> that the group is carried as "every row" instead of
    /// the list being built: fifteen rows in sixteen, or more.
    ///
    /// <para>SAFE at any threshold — "every row" is a superset, and the scan re-checks each row
    /// against the whole filter — so the number is about cost alone. Naming a row costs four
    /// bytes, to decode the list, the first time a group is asked (the index keeps the decoded
    /// list when there is a cache, and lends it uncopied after that). Not naming it costs a
    /// decoded row the filter then rejects, and only for the rows the list left out.</para>
    ///
    /// <para>MEASURED (<c>ExceptionPresencePageProbe</c>, Release, eight groups of 10 000 Error
    /// rows). The line was drawn when a list cost twelve to sixteen bytes a row and was built in
    /// every group of a page's batch: there a full read was indifferent at it, and a page just
    /// under it cost 1.0-1.4 MB against 0.15 MB just over. Lists are cheaper now and a page
    /// builds one for the group it reads, so the page no longer feels the line; a full read
    /// feels it by under one per cent — just under it (93 rows in a hundred) the list saves the
    /// read 0.9 % of what it allocates, the rows it leaves out, and just over it the read costs
    /// what the plain scan costs. The line stays where it was: what it gives up on a full read
    /// is less than the runs differ by, and what it spares is the largest list a group has,
    /// decoded the first time each group is asked for it.</para>
    ///
    /// <para>The count is an UPPER bound and may exceed the group — a row filed under two
    /// spellings of a value is counted twice — which only ever errs towards "every row". An
    /// unknown group size (0) never qualifies, and a group of fewer than sixteen rows qualifies
    /// only when the list is the whole of it.</para>
    /// </summary>
    internal static bool NamesNearlyAllOf(uint groupEventCount, int rows) =>
        groupEventCount != 0 && rows >= groupEventCount - groupEventCount / WholeGroupDivisor;

    /// <summary>A list may leave out one row in this many and still stand for its group.</summary>
    private const uint WholeGroupDivisor = 16;

    /// <summary>Intersects two ascending, distinct lists into a new ascending array. Neither is written.</summary>
    private static uint[] IntersectSorted(ReadOnlySpan<uint> a, ReadOnlySpan<uint> b)
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

    /// <summary>Unions two ascending, distinct lists into a new ascending array. Neither is written.</summary>
    private static uint[] UnionSorted(ReadOnlySpan<uint> a, ReadOnlySpan<uint> b)
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
    /// file open, a CreateFromFile, a CreateViewAccessor over the whole file and a
    /// block-index parse, 40 of them for a 20-segment query.
    /// </param>
    /// <param name="pin">
    /// The handle pinning the segment's file since the snapshot, or null; what this method maps its
    /// own reader from when <paramref name="borrowed"/> is null. Borrowed as well: the query
    /// releases it, after this iterator's reader has gone.
    /// </param>
    /// <param name="plan">
    /// What the prefilter decided about each index group of the segment, and what it left for
    /// this scan to decide when it reaches the group (<see cref="SegmentPlan"/>); null to read
    /// every row. A plan is made over <paramref name="borrowed"/> and is never there without it.
    /// </param>
    private async IAsyncEnumerable<LogEvent> ScanSegmentAsync(
        SegmentInfo info,
        CompiledFilter filter,
        HashSet<Ameto.Core.LogLevel>? levels,
        SegmentPlan? plan,
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
            //
            // With a plan the file is walked a group at a time, and each group is asked about as
            // the walk reaches it — which is where a group left for later has its rows named.
            // The plan belongs to the reader the prefilter opened; a scan that opened its own
            // has none, and reads everything.
            var events = plan is not null && borrowed is not null
                ? reader.ReadGroupsAsync(plan, from, to, reversed, ct)
                : reader.ReadEventsAsync(null, from, to, reversed, ct);
            await foreach (var ev in events)
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
