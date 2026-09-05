using System.Buffers.Binary;
using System.Text;
using Ameto.Core;
using Microsoft.Extensions.Logging;

namespace Ameto.Tracing.Storage;

/// <summary>
/// What the trace-id index is doing right now, for the diagnostics endpoint.
///
/// <para>Exists because these numbers were invisible, and their invisibility is most of why the
/// problem lasted: nobody could see how many cold segments an install had or what their indexes
/// weighed, so nobody could see that a trace lookup was reading all of them.</para>
/// </summary>
public sealed class TraceIndexReport
{
    /// <summary>Cold segments in the current snapshot — the fan-out width before the index.</summary>
    public int  ColdSegments       { get; init; }
    /// <summary>How many of them the index answers for. Equal to the above means the migration is done.</summary>
    public int  CoveredSegments    { get; init; }
    public long ColdSpans          { get; init; }
    /// <summary>Index runs currently open.</summary>
    public int  OpenRuns           { get; init; }
    /// <summary>What the runs weigh on disk.</summary>
    public long IndexBytesOnDisk   { get; init; }
    /// <summary>What they keep in RAM — the bloom bits and the sparse block maps, and the number
    /// that decides when per-segment runs stop scaling and levelled compaction has to start.</summary>
    public long IndexBytesInMemory { get; init; }
    /// <summary>Catalog generation, so two samples can be told apart.</summary>
    public ulong CatalogGeneration { get; init; }
}

/// <summary>One trace segment as the catalog knows it — the identity a file path cannot provide.</summary>
internal readonly record struct TraceSegmentEntry(
    ulong  SegmentId,
    string FilePath,
    long   MinStartNano,
    long   MaxStartNano,
    int    SpanCount);

/// <summary>
/// One sorted run of the trace-id index. Carried by the manifest from the first version so that
/// adding the index later is not a format change — an engine that writes no runs simply writes
/// none, and one that reads a manifest without them gets an empty list.
/// </summary>
/// <param name="CoveredSegments">
/// Every segment whose traces this run indexes — one for a run written beside a segment, many for
/// a run produced by index compaction.
///
/// <para>A LIST RATHER THAN A SINGLE ID, and the reason is a hole that only opens once runs merge.
/// Coverage is the claim that lets a segment be SKIPPED, so it must be withdrawable the instant a
/// run turns out not to open. With one id per run that works; with a merged run the engine would
/// know a claim had failed and not know whose — and a coverage claim nobody can withdraw is
/// exactly the silent under-report this design exists to prevent. So the run says who it speaks
/// for, and every path that has to take a claim back has something to take back.</para>
///
/// <para>THIS IS ALSO WHY DELETION NEEDS NO TOMBSTONES. A run is dropped only when EVERY segment
/// it covers is gone; a merged run outlives the loss of one of them, because it still holds live
/// entries for the others. The departing segment's entries simply become garbage — filtered on
/// read by "is this segment id still in the catalog?" and removed physically at the next index
/// compaction. Matching runs to segments by FILE PATH instead looked equivalent and was not: a
/// run's path is its own <c>.tix</c>, never the <c>.trc</c> it describes, so nothing ever matched
/// and every run outlived its segment.</para>
/// </param>
internal readonly record struct TraceIndexRun(
    int     Level,
    string  FilePath,
    ulong   MinKey,
    ulong   MaxKey,
    int     EntryCount,
    ulong[] CoveredSegments);

/// <summary>
/// THE CATALOG OF TRACE SEGMENTS, AND THE ONLY PLACE THAT SAYS WHAT AN INDEX MAY ANSWER FOR.
///
/// <para>Two jobs, and the second is why this exists at all.</para>
///
/// <para>THE FIRST IS IDENTITY. A cold trace segment is known today only by its file path, and a
/// path is not an identity: compaction writes a merged file and unlinks its sources, so the thing
/// a reader was holding a reference to stops existing under that name. Anything that wants to say
/// "trace X is in segment Y" needs Y to survive that, which means an id allocated once and carried
/// here. The log engine already reached this conclusion — see <c>SegmentId</c> / <c>SegmentKey</c>
/// in <c>Ameto.Storage.StorageEngine</c>; this is the same idea on the trace side.</para>
///
/// <para>THE SECOND IS COVERAGE, and it is a safety property rather than a feature. An index that
/// answers "there is no such trace" and is wrong is a silent data loss — the exact class this
/// engine's readers have spent every review round closing (see <c>VanishedRegionLog</c>). So the
/// index is never trusted globally. <see cref="Covered"/> names the segments it vouches for, a
/// negative answer counts only inside that set, and every segment outside it is read exactly as it
/// is read today. That single rule is what makes the whole feature deployable: coverage starts
/// empty, grows as a background backfill writes one index run at a time, and can be dropped back
/// to empty at any moment with no worse consequence than today's speed.</para>
///
/// <para>DEGRADATION IS THE DEFAULT, NOT THE EXCEPTION. Today the truth about which segments exist
/// is recovered by scanning the directory — crude, and unkillable. A manifest that becomes a single
/// point of truth would be a downgrade, so <see cref="Load"/> answers EVERY failure — missing,
/// truncated, wrong magic, bad CRC, unreadable — with an empty catalog and a log line. An empty
/// catalog means no ids and no coverage, which means the engine behaves exactly as it does now.
/// There is no state of this file that can make a query wrong; the worst it can do is make one
/// slow.</para>
///
/// <para>WRITES ARE WHOLE OR NOT AT ALL. Each mutation builds a complete new state, writes it to a
/// temp file, fsyncs, and renames over the old one — so a reader gets either the previous
/// generation or the next, never a half. The in-memory state is swapped only after the rename
/// returns, so a failed save leaves memory and disk agreeing on the old generation rather than
/// diverging. Directory entries themselves cannot be fsynced portably from .NET, which is the same
/// gap <c>RecoverOrSweepTempFiles</c> documents for segment renames; the consequence is bounded the
/// same way — a lost rename is a lost generation, and a lost generation is coverage that has to be
/// rebuilt, never a wrong answer.</para>
/// </summary>
internal sealed class TraceManifest
{
    private const uint   Magic       = 0x464D_4452;   // "RDMF" little-endian
    private const ushort Version     = 1;
    internal const string FileName   = "traces.manifest";
    private const string TempName    = "traces.manifest.tmp";

    /// <summary>
    /// Longest path this format will read back. Paths here are written by the engine itself, so
    /// this bounds damage rather than legitimate content — a torn length prefix is what it is for.
    /// </summary>
    private const int MaxPathChars = 4096;

    private readonly string                     _dir;
    private readonly ILogger                    _logger;
    private readonly System.Threading.Lock      _gate = new();

    /// <summary>
    /// The current state. Replaced wholesale, never mutated, so a reader can take the reference
    /// once and use it without a lock — the same discipline <c>_coldSegments</c> follows.
    /// </summary>
    private volatile State _state;

    private TraceManifest(string dir, ILogger logger, State state)
    {
        _dir    = dir;
        _logger = logger;
        _state  = state;
    }

    /// <summary>An immutable snapshot. Every field is fully built before the reference is published.</summary>
    private sealed record State(
        ulong                                Generation,
        ulong                                NextSegmentId,
        Dictionary<ulong, TraceSegmentEntry> Segments,
        List<TraceIndexRun>                  Runs,
        HashSet<ulong>                       Covered)
    {
        public static State Empty => new(0, 1, new(), new(), new());

        // THE COPIES A MUTATION STARTS FROM, named rather than spelled out at each call site.
        // "new List<T>(collection)" and "new List<T>(count)" are one shape to the convention
        // scanner in FileBoundsConventionTests and opposites in fact: one copies memory this
        // process owns, the other reserves for a number a file may have supplied — which is the
        // whole thing that rule exists to police. Three helpers make the distinction once, where
        // a reviewer can argue with it, instead of seven times where they cannot.
        public Dictionary<ulong, TraceSegmentEntry> CopySegments() => new(Segments);
        public List<TraceIndexRun>                  CopyRuns()     => new(Runs);
        public HashSet<ulong>                       CopyCovered()  => new(Covered);
    }

    // ── Reading ────────────────────────────────────────────────────────────────

    /// <summary>Generation of the state currently in memory. 0 means nothing has been written.</summary>
    public ulong Generation => _state.Generation;

    /// <summary>Segments the catalog knows, by id.</summary>
    public IReadOnlyDictionary<ulong, TraceSegmentEntry> Segments => _state.Segments;

    /// <summary>Index runs the catalog knows.</summary>
    public IReadOnlyList<TraceIndexRun> Runs => _state.Runs;

    /// <summary>
    /// Whether the trace-id index vouches for this segment. A negative answer from the index is
    /// only usable when this is true; everything else falls back to the full scan.
    /// </summary>
    public bool IsCovered(ulong segmentId) => _state.Covered.Contains(segmentId);

    /// <summary>
    /// The coverage set as one instant, for a caller that must compare it against something else
    /// taken at the same moment.
    ///
    /// <para>Free: the set is copy-on-write, so this hands back the live reference and never
    /// copies. The read path needs it because asking <see cref="IsCovered"/> per segment samples
    /// coverage LATER than the index lookup it is being compared with, and a segment that became
    /// covered in between is then skipped on the strength of a run the lookup never saw.</para>
    /// </summary>
    public IReadOnlySet<ulong> CoverageSnapshot() => _state.Covered;

    /// <summary>How many segments the index currently vouches for.</summary>
    public int CoveredCount => _state.Covered.Count;

    /// <summary>The segment id for a path, or null when the catalog does not know it.</summary>
    public ulong? IdForPath(string filePath)
    {
        foreach (var (id, seg) in _state.Segments)
            if (string.Equals(seg.FilePath, filePath, StringComparison.Ordinal))
                return id;
        return null;
    }

    // ── Mutation ───────────────────────────────────────────────────────────────

    /// <summary>
    /// The next segment id, reserved and persisted before it is used.
    ///
    /// <para>Persisted rather than handed out from a counter, because a crash between allocating an
    /// id and writing the segment that carries it must not let the id be reused: an index entry
    /// pointing at id 7 has to mean one file for the life of the install. Reserving costs one
    /// manifest write, and ids are allocated once per flush — an hour apart at the quiet end.</para>
    /// </summary>
    public ulong AllocateSegmentId()
    {
        lock (_gate)
        {
            var s  = _state;
            ulong id = s.NextSegmentId;
            Commit(s with { NextSegmentId = id + 1 });
            return id;
        }
    }

    /// <summary>
    /// Registers a freshly written segment, optionally with the index run that covers it. Passing a
    /// run is what marks the segment covered, and the two are written in the same generation on
    /// purpose: a covered segment whose run is missing would be a segment the index is trusted to
    /// answer for and cannot.
    /// </summary>
    public void AddSegment(TraceSegmentEntry segment, TraceIndexRun? run = null)
    {
        lock (_gate)
        {
            var s    = _state;
            var segs = s.CopySegments();
            segs[segment.SegmentId] = segment;

            var runs = s.Runs;
            var cov  = s.Covered;
            if (run is { } r)
            {
                runs = s.CopyRuns();    runs.Add(r);
                cov  = s.CopyCovered(); cov.Add(segment.SegmentId);
            }
            Commit(s with { Segments = segs, Runs = runs, Covered = cov });
        }
    }

    /// <summary>
    /// A compaction, as one generation: the sources leave the catalog and the coverage set, the
    /// merged segment joins them. Their runs go with them — a run naming a segment that no longer
    /// exists is garbage the read path would have to filter forever.
    /// </summary>
    public void ReplaceSegments(
        IReadOnlyCollection<ulong> removedIds, TraceSegmentEntry added, TraceIndexRun? run = null)
    {
        lock (_gate)
        {
            var s     = _state;
            var segs  = s.CopySegments();
            var cov   = s.CopyCovered();
            var gone  = new HashSet<ulong>();
            foreach (var id in removedIds)
            {
                if (segs.Remove(id)) gone.Add(id);
                cov.Remove(id);
            }
            segs[added.SegmentId] = added;

            var runs = KeepRuns(s.Runs, gone);
            if (run is { } r) { runs.Add(r); cov.Add(added.SegmentId); }

            Commit(s with { Segments = segs, Runs = runs, Covered = cov });
        }
    }

    /// <summary>Retention: the segments are gone from disk, so they leave the catalog with them.</summary>
    public void RemoveSegments(IReadOnlyCollection<ulong> removedIds)
    {
        if (removedIds.Count == 0) return;
        lock (_gate)
        {
            var s    = _state;
            var segs = s.CopySegments();
            var cov  = s.CopyCovered();
            var gone = new HashSet<ulong>();
            foreach (var id in removedIds)
            {
                if (segs.Remove(id)) gone.Add(id);
                cov.Remove(id);
            }
            Commit(s with
            {
                Segments = segs,
                Runs     = KeepRuns(s.Runs, gone),
                Covered  = cov,
            });
        }
    }

    /// <summary>
    /// The backfill's one operation: an index run now exists for a segment that was already in the
    /// catalog, so the index may start answering for it. Deliberately the LAST step of a backfill —
    /// the run file is written, fsynced and renamed first, so a crash here costs a rebuild of one
    /// run rather than a segment the index wrongly vouches for.
    /// </summary>
    public void MarkCovered(ulong segmentId, TraceIndexRun run)
    {
        lock (_gate)
        {
            var s = _state;
            if (!s.Segments.ContainsKey(segmentId)) return;   // gone while we were building it
            Commit(s with
            {
                Runs    = Added(s.CopyRuns(), run),
                Covered = Added(s.CopyCovered(), segmentId),
            });
        }
    }

    /// <summary>
    /// Index compaction: some runs are replaced by others over exactly the same keys. It does NOT
    /// touch the coverage set, and that is what makes it interruptible — the same segments are
    /// vouched for before and after, so a crash mid-compaction loses work and nothing else.
    /// </summary>
    public void ReplaceRuns(IReadOnlyCollection<string> removedPaths, IReadOnlyCollection<TraceIndexRun> added)
    {
        lock (_gate)
        {
            var s    = _state;
            var drop = new HashSet<string>(removedPaths, StringComparer.Ordinal);
            var runs = s.Runs.Where(r => !drop.Contains(r.FilePath)).ToList();
            runs.AddRange(added);
            Commit(s with { Runs = runs });
        }
    }

    /// <summary>
    /// Takes back the claim that the index answers for one segment, and drops the per-segment run
    /// that made it. For the discovery that a run will not open: the segment is still there and
    /// still readable, it simply goes back to being scanned.
    /// </summary>
    public void WithdrawCoverage(ulong segmentId)
    {
        lock (_gate)
        {
            var s = _state;
            if (!s.Covered.Contains(segmentId)) return;
            var cov = s.CopyCovered();
            cov.Remove(segmentId);
            Commit(s with
            {
                Runs    = KeepRuns(s.Runs, new HashSet<ulong> { segmentId }),
                Covered = cov,
            });
        }
    }

    /// <summary>
    /// Drops every claim of coverage, keeping the catalog. The switch that turns the fast path off
    /// without turning identity off — for an operator who suspects the index, and for the config
    /// flag that disables it.
    /// </summary>
    public void ClearCoverage()
    {
        lock (_gate)
        {
            var s = _state;
            if (s.Covered.Count == 0 && s.Runs.Count == 0) return;
            Commit(s with { Runs = new List<TraceIndexRun>(), Covered = new HashSet<ulong>() });
        }
    }

    /// <summary>
    /// The runs that survive the loss of <paramref name="gone"/>: everything except runs whose
    /// EVERY covered segment has left.
    ///
    /// <para>All, not any, and that is the whole no-tombstone argument in one predicate. A
    /// per-segment run has exactly one covered segment, so it dies with it — the old behaviour,
    /// unchanged. A merged run keeps live entries for every OTHER segment in it, so dropping it
    /// because one of them expired would silently un-index all the rest. The departing
    /// segment's entries stay in the file as garbage, are filtered on read against the catalog,
    /// and go away at the next index compaction.</para>
    /// </summary>
    private static List<TraceIndexRun> KeepRuns(List<TraceIndexRun> runs, HashSet<ulong> gone)
    {
        var kept = new List<TraceIndexRun>(runs.Count);
        foreach (var r in runs)
        {
            bool anyAlive = false;
            foreach (ulong sid in r.CoveredSegments) if (!gone.Contains(sid)) { anyAlive = true; break; }
            if (anyAlive || r.CoveredSegments.Length == 0) kept.Add(r);
        }
        return kept;
    }

    /// <summary>Adds one item and hands the collection back, so a <c>with</c> can use it inline.</summary>
    private static List<T>    Added<T>(List<T> list, T item)   { list.Add(item); return list; }
    private static HashSet<T> Added<T>(HashSet<T> set, T item) { set.Add(item);  return set;  }

    /// <summary>
    /// Saves, then publishes. In that order: a save that throws leaves memory describing what is
    /// still on disk, so the next attempt starts from a state that is true rather than from one
    /// that only this process believes.
    /// </summary>
    private void Commit(State next)
    {
        var withGen = next with { Generation = next.Generation + 1 };
        Save(withGen);
        _state = withGen;
    }

    // ── Persistence ────────────────────────────────────────────────────────────

    /// <summary>
    /// Reads the manifest, or hands back an empty one. NEVER throws: see the class docstring — an
    /// empty catalog is a correct state that costs speed, and anything this method could throw
    /// would cost the engine its startup.
    /// </summary>
    public static TraceManifest Load(string dir, ILogger logger)
    {
        string path = Path.Combine(dir, FileName);
        try
        {
            if (!File.Exists(path)) return new TraceManifest(dir, logger, State.Empty);

            byte[] raw = File.ReadAllBytes(path);
            var    st  = Parse(raw, path);
            logger.LogInformation(
                "Trace manifest generation {Gen}: {Segments} segment(s), {Runs} index run(s), " +
                "{Covered} covered by the trace-id index",
                st.Generation, st.Segments.Count, st.Runs.Count, st.Covered.Count);
            return new TraceManifest(dir, logger, st);
        }
        catch (Exception ex)
        {
            // EVERY failure lands here on purpose. The engine keeps working without a catalog: no
            // segment ids, no coverage, every read the full scan it does today. Loud, because a
            // manifest that stopped parsing is worth an operator's attention even though nothing
            // is broken by it.
            logger.LogError(ex,
                "Trace manifest {Path} could not be read and is being ignored — segment identity and "
              + "the trace-id index are off for this run, and every trace lookup falls back to "
              + "scanning every segment. Delete the file to have it rebuilt", path);
            return new TraceManifest(dir, logger, State.Empty);
        }
    }

    private static State Parse(ReadOnlySpan<byte> raw, string path)
    {
        if (raw.Length < 4 + 2 + 8 + 8 + 4 + 4 + 4 + 4)
            throw new InvalidDataException($"Trace manifest {path} is {raw.Length} bytes — too short to hold a header");

        // The CRC covers everything before it, so it is checked BEFORE a single length is believed.
        uint stored   = BinaryPrimitives.ReadUInt32LittleEndian(raw[^4..]);
        uint computed = Crc32c.Append(0, raw[..^4]);
        if (stored != computed)
            throw new InvalidDataException(
                $"Trace manifest {path} fails its checksum (stored {stored:X8}, computed {computed:X8})");

        var r = new Cursor(raw[..^4], path);

        if (r.U32() != Magic)  throw new InvalidDataException($"Trace manifest {path} has the wrong magic");
        ushort ver = r.U16();
        if (ver != Version)    throw new InvalidDataException($"Trace manifest {path} is version {ver}, expected {Version}");

        ulong generation = r.U64();
        ulong nextId     = r.U64();

        int segCount = r.Count(fileBytesPerElement: 34, "Manifest segments");   // 8+8+8+4+2 + at least one path byte
        var segments = new Dictionary<ulong, TraceSegmentEntry>(FileBounds.PreallocFor(segCount, 64));
        ulong maxSeen = 0;
        for (int i = 0; i < segCount; i++)
        {
            ulong id   = r.U64();
            long  min  = r.I64();
            long  max  = r.I64();
            int   span = r.I32();
            string p   = r.Str();
            segments[id] = new TraceSegmentEntry(id, p, min, max, span);
            if (id > maxSeen) maxSeen = id;
        }

        int runCount = r.Count(fileBytesPerElement: 29, "Manifest runs");       // 2+8+8+4+4 + at least one path byte
        var runs = new List<TraceIndexRun>(FileBounds.PreallocFor(runCount, 48));
        for (int i = 0; i < runCount; i++)
        {
            int    level = r.U16();
            ulong  minK  = r.U64();
            ulong  maxK  = r.U64();
            int    n      = r.I32();
            int    covers = r.Count(fileBytesPerElement: 8, "Manifest run coverage");
            // Grown rather than sized outright, like the two lists above it: PreallocFor caps what
            // is reserved up front while the list still takes everything the file honestly holds,
            // and it puts the bound on the line that allocates instead of inside Cursor.Count
            // where nothing reviewing this can see it.
            var    ids    = new List<ulong>(FileBounds.PreallocFor(covers, sizeof(ulong)));
            for (int c = 0; c < covers; c++) ids.Add(r.U64());
            string p      = r.Str();
            runs.Add(new TraceIndexRun(level, p, minK, maxK, n, [.. ids]));
        }

        int covCount = r.Count(fileBytesPerElement: 8, "Manifest coverage");
        var covered  = new HashSet<ulong>(FileBounds.PreallocFor(covCount, 16));
        for (int i = 0; i < covCount; i++) covered.Add(r.U64());

        // A COVERED ID THAT NAMES NO SEGMENT IS DROPPED, not trusted. It can only arrive through
        // damage this CRC did not catch or a bug on the write side, and either way the read path
        // must not be told the index vouches for something the catalog has never heard of.
        covered.RemoveWhere(id => !segments.ContainsKey(id));

        // The allocator floor is derived, never merely read. A nextSegmentId that came back below
        // an id already in use would hand the same id to two different files, and an index entry
        // is only meaningful while one id means one segment.
        if (nextId <= maxSeen) nextId = maxSeen + 1;

        return new State(generation, nextId, segments, runs, covered);
    }

    private void Save(State s)
    {
        string tmp   = Path.Combine(_dir, TempName);
        string final = Path.Combine(_dir, FileName);

        var body = new ArrayBufferWriter();
        body.U32(Magic);
        body.U16(Version);
        body.U64(s.Generation);
        body.U64(s.NextSegmentId);

        body.I32(s.Segments.Count);
        foreach (var seg in s.Segments.Values)
        {
            body.U64(seg.SegmentId);
            body.I64(seg.MinStartNano);
            body.I64(seg.MaxStartNano);
            body.I32(seg.SpanCount);
            body.Str(seg.FilePath);
        }

        body.I32(s.Runs.Count);
        foreach (var run in s.Runs)
        {
            body.U16((ushort)run.Level);
            body.U64(run.MinKey);
            body.U64(run.MaxKey);
            body.I32(run.EntryCount);
            body.I32(run.CoveredSegments.Length);
            foreach (ulong sid in run.CoveredSegments) body.U64(sid);
            body.Str(run.FilePath);
        }

        body.I32(s.Covered.Count);
        foreach (ulong id in s.Covered) body.U64(id);

        body.U32(Crc32c.Append(0, body.Written));

        Directory.CreateDirectory(_dir);
        using (var fs = new FileStream(tmp, FileMode.Create, FileAccess.Write, FileShare.None, 4096))
        {
            fs.Write(body.Written);
            fs.Flush(flushToDisk: true);   // the bytes, before the name that makes them the truth
        }
        File.Move(tmp, final, overwrite: true);
    }

    // ── Tiny readers/writers, so no length from the file is believed ───────────

    /// <summary>A forward cursor that refuses to read past what the file actually holds.</summary>
    private ref struct Cursor(ReadOnlySpan<byte> data, string path)
    {
        private readonly ReadOnlySpan<byte> _d = data;
        private readonly string             _p = path;
        private int                         _i = 0;

        private ReadOnlySpan<byte> Take(int n)
        {
            if (n < 0 || _i + n > _d.Length)
                throw new EndOfStreamException($"Trace manifest {_p} ends inside a field");
            var s = _d.Slice(_i, n);
            _i += n;
            return s;
        }

        public uint   U32() => BinaryPrimitives.ReadUInt32LittleEndian(Take(4));
        public ushort U16() => BinaryPrimitives.ReadUInt16LittleEndian(Take(2));
        public ulong  U64() => BinaryPrimitives.ReadUInt64LittleEndian(Take(8));
        public long   I64() => BinaryPrimitives.ReadInt64LittleEndian(Take(8));
        public int    I32() => BinaryPrimitives.ReadInt32LittleEndian(Take(4));

        /// <summary>A count, bounded by what the bytes left could possibly describe.</summary>
        public int Count(int fileBytesPerElement, string what)
        {
            int n = I32();
            FileBounds.RequireCountFits(n, _d.Length - _i, fileBytesPerElement, what, _p);
            return n;
        }

        public string Str()
        {
            int len = U16();
            if (len > MaxPathChars)
                throw new InvalidDataException($"Trace manifest {_p} declares a {len}-byte path");
            return Encoding.UTF8.GetString(Take(len));
        }
    }

    /// <summary>A growable little-endian writer. Nothing here is hot: it runs once per flush.</summary>
    private sealed class ArrayBufferWriter
    {
        private byte[] _buf = new byte[4096];
        private int    _n;

        public ReadOnlySpan<byte> Written => _buf.AsSpan(0, _n);

        private Span<byte> Room(int n)
        {
            if (_n + n > _buf.Length) Array.Resize(ref _buf, Math.Max(_buf.Length * 2, _n + n));
            var s = _buf.AsSpan(_n, n);
            _n += n;
            return s;
        }

        public void U32(uint v)   => BinaryPrimitives.WriteUInt32LittleEndian(Room(4), v);
        public void U16(ushort v) => BinaryPrimitives.WriteUInt16LittleEndian(Room(2), v);
        public void U64(ulong v)  => BinaryPrimitives.WriteUInt64LittleEndian(Room(8), v);
        public void I64(long v)   => BinaryPrimitives.WriteInt64LittleEndian(Room(8), v);
        public void I32(int v)    => BinaryPrimitives.WriteInt32LittleEndian(Room(4), v);

        public void Str(string s)
        {
            int n = Encoding.UTF8.GetByteCount(s);
            if (n > MaxPathChars)
                throw new InvalidDataException($"Path is {n} bytes, over the {MaxPathChars} this format holds");
            U16((ushort)n);
            Encoding.UTF8.GetBytes(s, Room(n));
        }
    }
}
