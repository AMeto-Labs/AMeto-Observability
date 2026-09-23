using Ameto.Core;
using System.Buffers;
using System.Buffers.Binary;
using System.Text;
using K4os.Compression.LZ4;

namespace Ameto.Tracing.Storage;

/// <summary>
/// One pre-aggregated row per trace, derived at flush time. Lets the trace-list and
/// trace-stats endpoints answer without deserialising a single span (analogous to the
/// <c>.stats</c> / <c>.svcgraph</c> sidecars).
/// </summary>
public sealed class TraceSummary
{
    public TraceId        TraceId        { get; init; }
    public SpanId         RootSpanId     { get; init; }
    public long           RootStartNano  { get; init; }
    public long           DurationNanos  { get; init; }
    public uint           SpanCount      { get; init; }
    public bool           HasRoot        { get; init; }
    public bool           HasError       { get; init; }
    public SpanStatusCode RootStatus     { get; init; }
    public short          HttpStatusCode { get; init; }
    public string         Name           { get; init; } = string.Empty;
    public string         ServiceName    { get; init; } = string.Empty;
    public string         HttpMethod     { get; init; } = string.Empty;
    public string         HttpPath       { get; init; } = string.Empty;
    /// <summary>Union of service names across the trace's spans in this segment.</summary>
    public string[]       Services       { get; init; } = [];
}

/// <summary>Sparse trace-volume bucket on a fixed <see cref="TraceSummarySidecar.GridNanos"/> grid.</summary>
public readonly record struct TraceVolumeEntry(long GridIndex, uint TraceCount, uint ErrorCount);

/// <summary>Header-only view of a <c>.tracesum</c> file — enough for volume/sparkline, no body read.</summary>
public sealed class TraceVolumeSegment
{
    public long                    MinStartNano { get; init; }
    public long                    MaxStartNano { get; init; }
    public List<TraceVolumeEntry>  Buckets      { get; init; } = [];
}

/// <summary>
/// Builds and reads the <c>.tracesum</c> companion sidecar.
///
/// <para>Binary format "RDTV":</para>
/// <code>
///   Magic uint32 | Version uint16
///   MinStartNano int64 | MaxStartNano int64
///   [Volume header — uncompressed, tiny]
///     volCount uint32
///     per bucket: gridIndex int64 | traceCount uint32 | errorCount uint32   (16 B each)
///   [Body — LZ4-pickled]
///     bodyUncompSize uint32 | bodyCompSize uint32 | LZ4 bytes of:
///       serviceCount uint32 | per service: nameLen uint16 | UTF-8
///       traceCount   uint32 | per trace: fixed prefix + name/method/path + service indices
/// </code>
/// </summary>
internal static class TraceSummarySidecar
{
    private const uint   Magic     = 0x52_44_54_56; // "RDTV"
    private const ushort Version   = 1;

    /// <summary>
    /// The largest decompressed body this reader will build. Every number below is copied out of a
    /// file, and a length prefix that has been torn is a request for that many bytes BEFORE anything
    /// discovers the file is shorter — <c>BinaryReader.ReadBytes</c> allocates `new byte[count]` and
    /// only then copies whatever it actually found. Measured on this reader before it was bounded: a
    /// 359-byte sidecar with its compressed-size field overwritten allocated 700 294 448 bytes, the
    /// read SUCCEEDED, the page came back rows=20 with no fault of any kind — and the same 668 MB was
    /// paid again on every page of every stream, because this runs once per segment per page.
    ///
    /// <para>256 MB is far above any body this writer produces (a segment's summaries compress to
    /// kilobytes) and far below the point where a torn field costs the process.</para>
    /// </summary>
    private const int MaxBodyBytes = 256 * 1024 * 1024;

    /// <summary>Volume grid resolution — 10 s. Sparse, so idle gaps cost nothing.</summary>
    public const long GridNanos = 10_000_000_000L;

    // ── Writer ──────────────────────────────────────────────────────────────────

    public static void Write(string baseTrcPath, IList<SpanRecord> spans, string? outputPath = null)
    {
        var batch = new OrderedSpans(spans);
        WriteOrdered(baseTrcPath, in batch, outputPath);
    }

    /// <summary>
    /// The same, for a batch the flush has already put in order — see <see cref="OrderedSpans"/>.
    /// The order decides the file: <c>traces</c>, <c>vol</c> and the service pool are all written
    /// in INSERTION order, and "the first empty-parent span wins the root slot" is a statement
    /// about the walk.
    /// </summary>
    internal static void WriteOrdered(string baseTrcPath, in OrderedSpans spans, string? outputPath = null)
    {
        int spanCount = spans.Count;
        if (spanCount == 0) return;

        // One pass: group spans by trace id into per-trace accumulators.
        var traces = new Dictionary<TraceId, Acc>(spanCount / 2 + 1);
        long segMin = long.MaxValue, segMax = long.MinValue;

        for (int i = 0; i < spanCount; i++)
        {
            var s = spans[i];
            if (s.StartTimeUnixNano < segMin) segMin = s.StartTimeUnixNano;
            if (s.StartTimeUnixNano > segMax) segMax = s.StartTimeUnixNano;

            if (!traces.TryGetValue(s.TraceId, out var a))
            {
                a = new Acc { TraceId = s.TraceId };
                traces[s.TraceId] = a;
            }

            a.SpanCount++;
            if (s.Status == SpanStatusCode.Error) a.HasError = true;
            (a.Services ??= new HashSet<string>(2, StringComparer.Ordinal)).Add(s.ServiceName);

            if (s.StartTimeUnixNano < a.EarliestNano)
            {
                a.EarliestNano  = s.StartTimeUnixNano;
                a.FirstService  = s.ServiceName;
            }

            // First empty-parent span wins the "root" slot.
            if (s.ParentSpanId.IsEmpty && !a.HasRoot)
            {
                a.HasRoot        = true;
                a.RootSpanId     = s.SpanId;
                a.RootStartNano  = s.StartTimeUnixNano;
                a.RootDurNanos   = s.DurationNanos;
                a.RootStatus     = s.Status;
                a.RootHttpStatus = s.HttpStatusCode;
                a.RootName       = s.Name;
                a.RootService    = s.ServiceName;

                // READ OUT OF THE BLOB, NOT OUT OF A DECODE OF IT — TS#7(f).
                //
                // This used to be `GetAttr(s.Attributes, …)`, and on a record that holds a blob
                // `Attributes` IS the lazy decode: a Dictionary, a key string and a box per
                // attribute, ~987 B for an ordinary eight-attribute span against the blob's 375 B.
                // The flush already decodes every blob once, on this thread, to feed the bloom
                // (SpanWriter.TryAddAttrBlobToBloom); this was a second decode of every ROOT, and
                // the worse kind — SpanRecord memoises it, and these records are the snapshot
                // TraceStorageEngine keeps serving queries from (`_flushingSpans`) until the
                // publish, so each dictionary stayed attached for the rest of the flush.
                //
                // `Resolve` answers both questions in one non-decoding walk and allocates only the
                // strings it returns; a record with no blob (a dictionary-built one) still goes
                // through GetAttr. Same answers by construction — key order, first non-null value
                // wins, last copy of a key wins, ToString() text — and the golden .tracesum hash in
                // TraceFlushProbe, whose roots carry every value shape including a truncated blob,
                // holds that to the byte. It is also the helper the trace list and TraceQL's row
                // builder use, so the three readers of a trace row cannot drift apart again.
                HttpSemconvKeys.Resolve(s, out string rootMethod, out string rootPath);
                a.RootMethod     = rootMethod;
                a.RootPath       = rootPath;
            }
        }

        // Volume histogram on the fixed grid (keyed by each trace's representative start).
        var vol = new Dictionary<long, VolCell>(traces.Count);
        foreach (var a in traces.Values)
        {
            long grid = (a.HasRoot ? a.RootStartNano : a.EarliestNano) / GridNanos;
            vol.TryGetValue(grid, out var cell);
            cell.Traces++;
            if (a.HasError) cell.Errors++;
            vol[grid] = cell;
        }

        // ── Service pool, interned BEFORE the rows are written ──────────────────────
        //
        // The body is the pool followed by the rows, and the pool used to be discovered WHILE the
        // rows were written — so the rows went to a scratch MemoryStream, the pool to a second
        // one, the first was CopyTo'd behind the second, the result ToArray()'d, and that array
        // pickled: four copies of the body, two of them growing by doubling, on a sidecar the
        // recon measured at 6,2 MB per 50 000-span flush.
        //
        // Interning the pool FIRST, in exactly the order the row pass meets the names — per trace,
        // in `traces` order: the root's (else the first) service, then the trace's service set in
        // its own enumeration order — yields the same pool and the same indices, so the body can
        // be written front to back into ONE buffer. Neither collection is modified between the
        // two passes, so both enumerate identically; the row pass below interns again, which is
        // now a lookup that always hits.
        var pool    = new Dictionary<string, int>(StringComparer.Ordinal);
        var poolArr = new List<string>();
        foreach (var a in traces.Values)
        {
            Intern(pool, poolArr, a.HasRoot ? a.RootService : a.FirstService);
            foreach (var sv in a.Services!) Intern(pool, poolArr, sv);
        }

        // ONE RENTED BUFFER for the whole body, returned before the file is opened. Sized for the
        // common case — a ~60-byte fixed row plus three short strings per trace, and at most one
        // service index per span — so it grows at most once or twice rather than a dozen times.
        int    rawLength;
        byte[] compBody;
        var body = new PooledBody(traces.Count * 80 + spanCount * 4 + 256);
        try
        {
            body.UInt32((uint)poolArr.Count);
            foreach (var name in poolArr) body.Utf8(name, prefixBytes: 2, maxBytes: int.MaxValue);

            body.UInt32((uint)traces.Count);
            Span<byte> tid = stackalloc byte[16];
            foreach (var a in traces.Values)
            {
                a.TraceId.WriteTo(tid);
                body.Bytes(tid);
                body.UInt64(a.RootSpanId.RawValue);
                body.Int64(a.HasRoot ? a.RootStartNano : a.EarliestNano);
                body.Int64(a.HasRoot ? a.RootDurNanos  : 0L);
                body.UInt32(a.SpanCount);

                byte flags = 0;
                if (a.HasRoot)  flags |= 0b01;
                if (a.HasError) flags |= 0b10;
                body.Byte(flags);
                body.Byte((byte)a.RootStatus);
                body.Int16(a.RootHttpStatus);
                body.Int32(Intern(pool, poolArr, a.HasRoot ? a.RootService : a.FirstService));

                body.Utf8(a.HasRoot ? a.RootName   : string.Empty, prefixBytes: 2, maxBytes: ushort.MaxValue);
                body.Utf8(a.HasRoot ? a.RootMethod : string.Empty, prefixBytes: 1, maxBytes: byte.MaxValue);
                body.Utf8(a.HasRoot ? a.RootPath   : string.Empty, prefixBytes: 2, maxBytes: ushort.MaxValue);

                var svcs = a.Services!;
                body.UInt16((ushort)svcs.Count);
                foreach (var sv in svcs) body.Int32(Intern(pool, poolArr, sv));
            }

            // Pickled straight from the written span: the span overload of the same pickler at the
            // same default level (L00_FAST), and the golden hashes hold it to the same bytes.
            rawLength = body.Length;
            compBody  = LZ4Pickler.Pickle(body.Written);
        }
        finally
        {
            body.Dispose();
        }

        string path = outputPath ?? Path.ChangeExtension(baseTrcPath, ".tracesum");
        using var fs = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None, 65536);
        using var w  = new BinaryWriter(fs);

        w.Write(Magic);
        w.Write(Version);
        w.Write(segMin);
        w.Write(segMax);

        w.Write((uint)vol.Count);
        foreach (var (grid, cell) in vol)
        {
            w.Write(grid);
            w.Write(cell.Traces);
            w.Write(cell.Errors);
        }

        w.Write((uint)rawLength);
        w.Write((uint)compBody.Length);
        w.Write(compBody);

        w.Flush();
        fs.Flush(flushToDisk: true); // durable before the caller renames and resets the WAL
    }

    // ── Reader: volume header only (cheap) ──────────────────────────────────────

    /// <summary>True when the companion <c>.tracesum</c> sidecar exists for this segment.</summary>
    public static bool Exists(string trcFilePath) =>
        File.Exists(Path.ChangeExtension(trcFilePath, ".tracesum"));

    public static TraceVolumeSegment? ReadVolume(string trcFilePath)
    {
        var path = Path.ChangeExtension(trcFilePath, ".tracesum");
        if (!File.Exists(path)) return null;

        try
        {
            using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, 16384, FileOptions.SequentialScan);
            using var br = new BinaryReader(fs);

            if (br.ReadUInt32() != Magic) return null;
            br.ReadUInt16(); // version
            long min = br.ReadInt64();
            long max = br.ReadInt64();

            // Bounded like every other count in this file. The same field is read by
            // TryReadSummaries a few methods down, where it was bounded and here it was not —
            // and this is the WORSE path of the two: /api/traces/stats is polled every fifteen
            // seconds independently of the list, so it runs even while the list is frozen.
            // Measured on a real 2532-byte .tracesum with the volume count set to 0x02000000:
            // 512.2 MB allocated on one stats refresh, and 0x40000000 asks for 17.2 GB.
            uint volCount = br.ReadUInt32();
            FileBounds.RequireCountFits(volCount, fs.Length - fs.Position,
                fileBytesPerElement: 16, "Volume header", path);

            var buckets = new List<TraceVolumeEntry>((int)volCount);
            for (uint i = 0; i < volCount; i++)
            {
                long grid   = br.ReadInt64();
                uint traces = br.ReadUInt32();
                uint errors = br.ReadUInt32();
                buckets.Add(new TraceVolumeEntry(grid, traces, errors));
            }

            return new TraceVolumeSegment { MinStartNano = min, MaxStartNano = max, Buckets = buckets };
        }
        catch
        {
            // NULL FOR EVERYTHING, and narrowing this was a mistake worth writing down. Restricting
            // the catch to content failures let an IOException out of a method whose only caller has
            // no handler and whose endpoint has none either — so an antivirus or a backup agent
            // holding one .tracesum open turned the whole fifteen-second stats poll into a 500,
            // where before it fell back to the documented legacy recount. null is this method's
            // way of saying "use the other path", and a transient lock is exactly when it should.
            //
            // The caller distinguishes damage from a lock and says so; see GetTraceVolumeAsync,
            // which has the logger this static does not.
            return null;
        }
    }

    // ── Reader: full per-trace rows ─────────────────────────────────────────────

    /// <summary>
    /// Every row in the sidecar, unbounded in time, with "could not read" flattened into "no
    /// rows".
    ///
    /// <para>NOT ON ANY SCAN PATH ANY MORE — its only remaining caller is
    /// <c>TraceSummarySidecarTests</c>, which round-trips a file it has just written and for
    /// which the two answers really are the same. Every production reader goes through
    /// <see cref="TryReadSummaries"/>, because flattening them is what let a segment fall out of
    /// a window and the stream above it still report <c>done {"complete":true}</c>. Kept as the
    /// round-trip's front door and nothing else; a new caller here is almost certainly a bug.</para>
    /// </summary>
    public static List<TraceSummary> ReadSummaries(string trcFilePath) =>
        TryReadSummaries(trcFilePath, long.MinValue, long.MaxValue, out var rows) ? rows : [];

    /// <summary>
    /// The rows whose representative start falls in <c>[fromNano, toNano]</c>, or FALSE when the
    /// sidecar could not be read at all.
    ///
    /// <para>THE RETURN VALUE IS THE POINT. This used to end in <c>catch { return []; }</c>, so a
    /// file that vanished between the <see cref="Exists"/> probe and the open — which compaction
    /// produces by design, publishing its merged output before unlinking its sources — or one a
    /// power cut left truncated, merged as an EMPTY LIST. The walk then ran to the end of the
    /// window, recorded no floor, reported itself uncapped, and the stream above it sent
    /// <c>done {"complete":true}</c> over a window a whole segment had just fallen out of.</para>
    ///
    /// <para>THE RANGE IS PUSHED DOWN, and only that far. The body is one LZ4 blob with no index,
    /// so the whole of it is still decompressed to reach any row — what the bound removes is the
    /// per-row cost the caller used to pay and then throw away: the <see cref="TraceSummary"/>
    /// itself, its <c>Services</c> array, and the three UTF-8 strings, for every trace in a
    /// segment that compaction may have grown to 200 000 spans while the caller's whole budget is
    /// 2 500 rows. A segment nested inside a wider one stays below the cursor for up to the 24 h
    /// <c>SelectCompactionBatch</c> groups within, and is reopened on every page of every stream
    /// for as long as it does.</para>
    /// </summary>
    public static bool TryReadSummaries(
        string trcFilePath, long fromNano, long toNano, out List<TraceSummary> rows)
    {
        rows = [];
        var path = Path.ChangeExtension(trcFilePath, ".tracesum");

        try
        {
            using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, 65536, FileOptions.SequentialScan);
            using var br = new BinaryReader(fs);

            if (br.ReadUInt32() != Magic) return false;
            br.ReadUInt16(); // version
            br.ReadInt64();  // min
            br.ReadInt64();  // max

            // EVERY LENGTH BELOW IS BOUNDED BY EVIDENCE THE FILE CANNOT FORGE — the bytes actually
            // left in it, and the ceiling above. Returning false rather than throwing keeps the
            // caller's own classification intact: a sidecar that will not parse is a corrupt
            // sidecar, which it already knows how to say.
            uint volCount = br.ReadUInt32();
            long afterVol = fs.Position + volCount * 16L;
            if (!FileBounds.CountFits(volCount, fs.Length - fs.Position, 16)) { rows = []; return false; }
            fs.Seek(afterVol, SeekOrigin.Begin); // skip volume header

            uint uncompSize = br.ReadUInt32();
            uint compSize   = br.ReadUInt32();
            if (!FileBounds.LengthFits(compSize, fs.Length - fs.Position)
             || !FileBounds.LengthFits(uncompSize, MaxBodyBytes)) { rows = []; return false; }

            // BOTH ARRAYS ARE RENTED — the compressed one too, which I left
            // allocating last round.
            byte[] comp = ArrayPool<byte>.Shared.Rent((int)compSize);
            try
            {
                // ReadExactly, not ReadBytes: a short read raises EndOfStreamException, which
                // DescribesContent lists and the filter below answers with the same "will not
                // parse" false that the length check gives.
                fs.ReadExactly(comp, 0, (int)compSize);

                // The size INSIDE the payload, which the checks above never saw: LZ4 carries the
                // decompressed length in its own header, so a short well-formed block can still
                // ask for gigabytes. Same reasoning as SpanReader's MaxBlockBytes guard, same
                // failure without it.
                //
                // SLICED, and that slice is what makes the rental above safe. LZ4Pickler takes the
                // payload length from source.Length, so handing the whole rental to the array-form
                // overload would measure a pool bucket instead of the body — which is why I left
                // this array allocating last round and wrote a comment defending it. The span form
                // was three lines further down the whole time, and SpanReader has used it at four
                // sites all along.
                int rawLen = LZ4Pickler.UnpickledSize(comp.AsSpan(0, (int)compSize));
                if (rawLen is < 0 or > MaxBodyBytes) { rows = []; return false; }

                // Both arrays clear the large-object threshold on an ordinary segment. Sixteen
                // incompressible bytes of trace id per row put a floor near 16 B/row, so the
                // compressed body passes 85 KB at about three thousand rows and reaches 3.5 MB at
                // the two hundred thousand SPANS the doc above says compaction may grow a segment
                // to; the decompressed one is larger again. That is a large-object allocation per
                // cold segment per page of every stream, on the 512 MB box this branch exists to
                // protect. Neither escapes: ParseBody returns decoded rows and strings and keeps
                // no reference to the buffer.
                byte[] raw = ArrayPool<byte>.Shared.Rent(rawLen);
                try
                {
                    LZ4Pickler.Unpickle(comp.AsSpan(0, (int)compSize), raw.AsSpan(0, rawLen));
                    if (rawLen != uncompSize) { /* tolerate — trust actual length */ }

                    // rawLen, NOT raw.Length: a rent of 90 000 bytes hands back 131 072, and the
                    // string-pool bound inside ParseBody measures the body's remaining bytes.
                    // Against the rental's capacity it would admit a pool a third larger than the
                    // file can describe — the guard this branch installed, loosened by the fix
                    // standing next to it.
                    rows = ParseBody(raw, rawLen, fromNano, toNano);
                    return true;
                }
                finally { ArrayPool<byte>.Shared.Return(raw); }
            }
            finally { ArrayPool<byte>.Shared.Return(comp); }
        }
        catch (Exception ex) when (ex is FileNotFoundException or DirectoryNotFoundException)
        {
            // ABSENCE IS NOT A READ FAILURE, and this method has always answered it with false —
            // the caller re-probes Exists precisely to tell "the file is gone" from "the file will
            // not parse", which is what separates a compaction handover from a loss. Letting it
            // propagate would hand that question to an exception classifier that cannot answer it.
            rows = [];
            return false;
        }
        catch (Exception ex) when (FileBounds.DescribesContent(ex))
        {
            // Only the exceptions that describe CONTENT are answered here, and answered as false —
            // "this sidecar will not parse". Everything else is left to propagate, because the
            // caller is the only place that can tell a damaged file from a locked one and it was
            // reduced to guessing: a bare catch here meant a sharing violation, an SMB blip or a
            // remount arrived at the caller as "the file exists but would not read", which it
            // hardcoded to Corrupt — a permanent claim over a lock that clears in seconds, with
            // nothing in the log, because this branch logged nothing either.
            rows = [];
            return false;
        }
    }

    /// <param name="rawLen">
    /// The body's REAL length. <paramref name="raw"/> may be a pooled buffer larger than the body,
    /// and every bound below is measured from what is left of the body, not of the rental.
    /// </param>
    private static List<TraceSummary> ParseBody(byte[] raw, int rawLen, long fromNano, long toNano)
    {
        var ms = new MemoryStream(raw, 0, rawLen, writable: false);
        using var br = new BinaryReader(ms);

        // A string here costs at least its two-byte length prefix, so a pool larger than half the
        // body is a torn count and nothing else.
        // Two bytes on disk is the least a string can cost (an empty one, length prefix only).
        uint poolCount = br.ReadUInt32();
        FileBounds.RequireCountFits(poolCount, rawLen - ms.Position,
            fileBytesPerElement: 2, "Trace-summary string pool", "the summary body");
        var pool = new string[poolCount];
        for (uint i = 0; i < poolCount; i++)
        {
            ushort len = br.ReadUInt16();
            pool[i] = Encoding.UTF8.GetString(br.ReadBytes(len));
        }

        string PoolAt(int idx) => idx >= 0 && idx < pool.Length ? pool[idx] : string.Empty;

        uint traceCount = br.ReadUInt32();
        // Capacity on the TRACE COUNT would allocate the whole segment's worth of slots for a
        // window that may want none of them, which is half of what the bound is here to stop.
        var  result     = new List<TraceSummary>();
        Span<byte> tidBuf = stackalloc byte[16];

        for (uint i = 0; i < traceCount; i++)
        {
            br.Read(tidBuf);
            var    tid       = TraceId.Parse(tidBuf);
            var    rootSid   = new SpanId(br.ReadUInt64());
            long   startNano = br.ReadInt64();

            // The row is variable-length, so an out-of-range row still has to be WALKED past —
            // but nothing of it needs to be decoded or allocated.
            if (startNano < fromNano || startNano > toNano)
            {
                ms.Position += 8 + 4 + 1 + 1 + 2 + 4;   // dur, spanCount, flags, status, http, svc
                SkipStr16(br, ms);                      // name
                SkipStr8 (br, ms);                      // method
                SkipStr16(br, ms);                      // path
                // The read is its OWN statement, and it has to be. `ms.Position += Read...()`
                // evaluates the Position GETTER before the right-hand side, so the two bytes the
                // read consumes are then written back out of the total — every following row
                // parses two bytes early, and the body desynchronises into an exception the
                // caller reports as an unreadable segment.
                long svcIndexBytes = br.ReadUInt16() * 4L;
                ms.Position += svcIndexBytes;
                continue;
            }

            long   durNanos  = br.ReadInt64();
            uint   spanCount = br.ReadUInt32();
            byte   flags     = br.ReadByte();
            var    status    = (SpanStatusCode)br.ReadByte();
            short  httpSC    = br.ReadInt16();
            int    rootSvc   = br.ReadInt32();
            string name      = ReadStr16(br);
            string method    = ReadStr8(br);
            string httpPath  = ReadStr16(br);

            ushort svcCount  = br.ReadUInt16();
            var    services  = svcCount == 0 ? [] : new string[svcCount];
            for (int j = 0; j < svcCount; j++) services[j] = PoolAt(br.ReadInt32());

            result.Add(new TraceSummary
            {
                TraceId        = tid,
                RootSpanId     = rootSid,
                RootStartNano  = startNano,
                DurationNanos  = durNanos,
                SpanCount      = spanCount,
                HasRoot        = (flags & 0b01) != 0,
                HasError       = (flags & 0b10) != 0,
                RootStatus     = status,
                HttpStatusCode = httpSC,
                Name           = name,
                ServiceName    = PoolAt(rootSvc),
                HttpMethod     = method,
                HttpPath       = httpPath,
                Services       = services,
            });
        }

        return result;
    }

    // Length first, seek second — see the note at the call site: as one expression the Position
    // getter is evaluated before the read, and the length prefix is then un-consumed.
    private static void SkipStr8 (BinaryReader r, MemoryStream ms) { int n = r.ReadByte();   ms.Position += n; }
    private static void SkipStr16(BinaryReader r, MemoryStream ms) { int n = r.ReadUInt16(); ms.Position += n; }

    // ── Helpers ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// The pool index of <paramref name="name"/>, adding it at the end if it is new; -1 for the
    /// empty name, which is never pooled. Called by both passes of the writer — the pre-pass that
    /// fixes the pool's order and the row pass that reads the indices back — so it is one function
    /// over the collections it is handed rather than a local one closing over them.
    /// </summary>
    private static int Intern(Dictionary<string, int> pool, List<string> poolArr, string name)
    {
        if (name.Length == 0) return -1;
        if (pool.TryGetValue(name, out var idx)) return idx;
        idx = poolArr.Count;
        pool[name] = idx;
        poolArr.Add(name);
        return idx;
    }

    /// <summary>
    /// The <c>.tracesum</c> body, written front to back into ONE <see cref="ArrayPool{T}"/> rental
    /// — little-endian throughout, exactly as the <c>BinaryWriter</c> it replaces wrote it.
    ///
    /// <para>A <c>ref struct</c>, so it lives in the writer's frame and cannot be boxed or
    /// captured. Mutated through its methods, so it must be held in an ordinary local — NOT a
    /// <c>using var</c>, whose local is read-only and would make every call below work on a
    /// defensive copy; the caller returns the rental in a <c>finally</c> instead.</para>
    /// </summary>
    private ref struct PooledBody
    {
        private byte[] _buf;
        private int    _len;

        public PooledBody(int initialCapacity)
        {
            _buf = ArrayPool<byte>.Shared.Rent(Math.Max(initialCapacity, 256));
            _len = 0;
        }

        public readonly int                Length  => _len;
        public readonly ReadOnlySpan<byte> Written => _buf.AsSpan(0, _len);

        public void Byte  (byte v)   => Take(1)[0] = v;
        public void UInt16(ushort v) => BinaryPrimitives.WriteUInt16LittleEndian(Take(2), v);
        public void Int16 (short v)  => BinaryPrimitives.WriteInt16LittleEndian (Take(2), v);
        public void UInt32(uint v)   => BinaryPrimitives.WriteUInt32LittleEndian(Take(4), v);
        public void Int32 (int v)    => BinaryPrimitives.WriteInt32LittleEndian (Take(4), v);
        public void UInt64(ulong v)  => BinaryPrimitives.WriteUInt64LittleEndian(Take(8), v);
        public void Int64 (long v)   => BinaryPrimitives.WriteInt64LittleEndian (Take(8), v);
        public void Bytes (scoped ReadOnlySpan<byte> v) => v.CopyTo(Take(v.Length));

        /// <summary>
        /// A length-prefixed UTF-8 string, encoded straight into the buffer. The prefix is
        /// <paramref name="prefixBytes"/> wide (1 or 2) and holds the length CAST to that width;
        /// the text is cut at <paramref name="maxBytes"/> BYTES, which is what the old
        /// <c>GetBytes(s)[..max]</c> did — mid-character if that is where the limit falls. The
        /// service pool passes no limit and a two-byte prefix, reproducing its old
        /// <c>(ushort)bytes.Length</c> exactly, overflow included.
        /// </summary>
        public void Utf8(string s, int prefixBytes, int maxBytes)
        {
            var dst = Reserve(prefixBytes + Encoding.UTF8.GetMaxByteCount(s.Length));
            int n   = Encoding.UTF8.GetBytes(s, dst[prefixBytes..]);
            if (n > maxBytes) n = maxBytes;
            if (prefixBytes == 1) dst[0] = (byte)n;
            else                  BinaryPrimitives.WriteUInt16LittleEndian(dst, (ushort)n);
            _len += prefixBytes + n;
        }

        public void Dispose()
        {
            ArrayPool<byte>.Shared.Return(_buf);
            _buf = [];
            _len = 0;
        }

        private Span<byte> Take(int n)
        {
            var dst = Reserve(n)[..n];
            _len += n;
            return dst;
        }

        /// <summary>At least <paramref name="n"/> writable bytes at the cursor, without advancing it.</summary>
        private Span<byte> Reserve(int n)
        {
            if (_buf.Length - _len < n)
            {
                var next = ArrayPool<byte>.Shared.Rent(Math.Max(_buf.Length * 2, _len + n));
                _buf.AsSpan(0, _len).CopyTo(next);
                ArrayPool<byte>.Shared.Return(_buf);
                _buf = next;
            }
            return _buf.AsSpan(_len);
        }
    }

    private static string ReadStr8(BinaryReader r)  => Encoding.UTF8.GetString(r.ReadBytes(r.ReadByte()));
    private static string ReadStr16(BinaryReader r) => Encoding.UTF8.GetString(r.ReadBytes(r.ReadUInt16()));

    private struct VolCell { public uint Traces; public uint Errors; }

    private sealed class Acc
    {
        public TraceId        TraceId;
        public uint           SpanCount;
        public bool           HasError;
        public long           EarliestNano = long.MaxValue;
        public string         FirstService = string.Empty;

        public bool           HasRoot;
        public SpanId         RootSpanId;
        public long           RootStartNano;
        public long           RootDurNanos;
        public SpanStatusCode RootStatus;
        public short          RootHttpStatus;
        public string         RootName    = string.Empty;
        public string         RootService = string.Empty;
        public string         RootMethod  = string.Empty;
        public string         RootPath    = string.Empty;

        public HashSet<string>? Services;
    }
}
