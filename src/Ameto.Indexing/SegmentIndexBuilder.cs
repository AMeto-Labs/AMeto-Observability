using System.Buffers;
using MessagePack;
using Ameto.Core;
using Ameto.Storage;

namespace Ameto.Indexing;

/// <summary>
/// Builds all three index structures (inverted, trigram, bloom) for ONE INDEX GROUP.
///
/// <para>An <see cref="ISegmentIndexSink"/>: the segment writer pushes each event in as it
/// stages it, and asks for the sections when the group's payload budget is reached. Pushing is
/// what lets a segment be written from a stream — the previous contract had the builder re-read
/// the group's events out of a <see cref="HotTierSegment"/> at the boundary, which only works
/// while something still holds them.</para>
///
/// The property walk reads each event's msgpack payload with a streaming
/// <see cref="MessagePackReader"/> and feeds the indexes directly — no per-event
/// <c>Dictionary</c>, no boxing, no per-attribute strings. This is the flush-path allocation hot
/// spot (index build was ~16 KB/event); the streaming walk is byte-parity with the old dictionary
/// path (see <see cref="BuildReference"/>, exercised by the parity test).
/// </summary>
public sealed unsafe class SegmentIndexBuilder : ISegmentIndexSink
{
    private readonly SegmentInvertedIndex _inverted;
    private readonly SegmentTrigramIndex  _trigram;
    private readonly SegmentBloomFilter   _bloom;
    private readonly IndexBuildHints?     _hints;

    private readonly int _maxFlattenDepth;

    // Per-build scratch (Build is single-threaded per flush). Grown on demand. Everything is
    // UTF-8: keys are the payload's own bytes copied behind their prefix, numbers are formatted
    // as UTF-8, and a value is case-folded ONCE (byte-wise, ASCII) for the bloom and the
    // trigram together. The old walk decoded every key and every value to UTF-16, folded them
    // as chars, encoded them back for the bloom, and lowered them again for the trigram — six
    // passes over each value's bytes per event.
    private readonly PinnedSpanMemoryManager _payload = new();   // MessagePackReader needs a sequence
    private byte[] _key  = new byte[256];   // accumulated flat (dot-notation) key, UTF-8
    private byte[] _val  = new byte[64];    // formatted numeric value (serialised form, prefix at [0..2])
    private byte[] _fold = new byte[256];   // case-folded ASCII value for bloom + trigram
    private char[] _wide = new char[256];   // non-ASCII value decoded for the UTF-16 fold path

    /// <summary>
    /// Terms per event assumed when the caller has nothing measured to offer. The filter is a
    /// TERM filter — level, message template, exception type, trace/span id, service name and
    /// every flattened property key and value all go in — but it used to be sized by EVENT
    /// count, i.e. ~10 bits per event against 50-150 entries per event. That is roughly 0.2 bits
    /// per term: the filter said "maybe" to everything, and the prefilter it exists to power
    /// (a bloom miss drops the whole segment before the MB-sized indexes are read) never
    /// rejected anything on prop-dense events. Sizing on terms restores ~10 bits/term.
    ///
    /// <para>It is a fallback, not the normal path, and it is deliberately generous: 64 is
    /// above every shape measured (<c>BloomSizingProbe</c>: 21.1 terms/event prop-dense, 7.0
    /// thin), because under-sizing brings back the saturation this exists to prevent and
    /// over-sizing only wastes bits. Generous is not free, though — at 10 bits a term it is
    /// 80 bytes of filter per event WHATEVER the event holds, so a thin-event group paid nine
    /// times the bits its terms could use. So the writer measures instead: it reads
    /// <see cref="BloomTermsAdded"/> off each sealed group and forecasts the next one from it
    /// (see <c>SegmentWriter.EnsureSink</c>), and this number is left for the first group of a
    /// file, which has nothing behind it to measure.</para>
    /// </summary>
    public const int EstimatedBloomTermsPerEvent = 64;

    /// <param name="estimatedTermsPerEvent">
    /// Bloom terms the caller expects each event to contribute. The filter is allocated up front
    /// and cannot be resized, so this decides the section's size outright; over-estimating wastes
    /// bits, under-estimating saturates the filter and the query prefilter stops rejecting.
    /// <see cref="SegmentBloomFilter.Create"/> bounds the product absolutely, so a wrong estimate
    /// costs selectivity rather than an unbounded allocation.
    ///
    /// <para>ZERO OR LESS means the caller has measured nothing and
    /// <see cref="EstimatedBloomTermsPerEvent"/> is used. That is a real case, not a guard: the
    /// first group of a file has no sealed group behind it to measure. It must not be read as
    /// "no terms" — a filter sized for one term per event saturates instantly.</para>
    /// </param>
    /// <param name="hints">
    /// What the previous group measured (distinct terms and trigrams), to pre-size the
    /// accumulators; the builder writes its own counts back when it seals. Optional.
    /// </param>
    public SegmentIndexBuilder(int expectedEventCount, int maxFlattenDepth = 5,
                               int estimatedTermsPerEvent = EstimatedBloomTermsPerEvent,
                               IndexBuildHints? hints = null)
    {
        long termsPerEvent = estimatedTermsPerEvent > 0 ? estimatedTermsPerEvent : EstimatedBloomTermsPerEvent;
        _bloom            = SegmentBloomFilter.Create((long)Math.Max(1, expectedEventCount) * termsPerEvent);
        _maxFlattenDepth  = maxFlattenDepth;
        _hints            = hints;
        _inverted         = new SegmentInvertedIndex(hints?.LastTerms ?? 0);
        _trigram          = new SegmentTrigramIndex(hints?.LastTrigrams ?? 0);
    }

    // ── Build ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// Indexes one event at its FILE ordinal. The writer calls this while the event is in its
    /// hand, so <c>ev.Properties</c> is still a live span over the producer's buffer and
    /// nothing has to be copied or retained.
    /// </summary>
    public void Add(uint fileOrdinal, in SegmentEventRef ev)
    {
        IndexHeaderFields(in ev, fileOrdinal);
        IndexPropertiesStreaming(ev.Properties, fileOrdinal);
    }

    /// <summary>
    /// Streaming (zero-alloc) build. Must be called while <paramref name="hot"/> is frozen.
    ///
    /// <paramref name="order"/> is the file write order produced by
    /// <c>SegmentWriter.ComputeSortOrder</c>: posting-list offsets are the event's ordinal
    /// IN THE .SEG FILE (sorted by @t, id), so the reader can map candidate offsets straight
    /// to blocks/rows. Null = identity (hot insertion order) — only for segments written
    /// without sorting (tests).
    /// </summary>
    public void Build(HotTierSegment hot, StringInternPool pool, int[]? order = null)
        => Build(hot, pool, order, 0, order?.Length ?? hot.Count);

    /// <summary>
    /// Indexes one INDEX GROUP: the events at file ordinals
    /// <c>[firstOrdinal, firstOrdinal + eventCount)</c>, i.e. <c>order[firstOrdinal..]</c>.
    ///
    /// <para>The by-index entry point, kept for the flush-path probes and the parity oracle;
    /// production drives <see cref="Add"/> from the writer. Both read the event through
    /// <see cref="HotTierEventSource.EventAt"/>, so there is one definition of what a row is.</para>
    ///
    /// <para>Posting offsets stay file-global (<c>firstOrdinal + pos</c>), not group-local —
    /// see the ordinal contract on <c>SegmentWriter.ComputeSortOrder</c>. A fresh builder per
    /// group is what bounds peak memory: the trigram accumulator costs ~7.6 B per posting and
    /// scales with indexed text bytes, so a day of one level would otherwise retain ~610 MB
    /// of managed state in a single build.</para>
    /// </summary>
    public void Build(HotTierSegment hot, StringInternPool pool, int[]? order, int firstOrdinal, int eventCount)
    {
        // Bound by the ORDER, not the tier: a level-split flush indexes one level's subset
        // per segment, and posting offsets are ordinals within that segment's own file.
        int n = Math.Min(firstOrdinal + eventCount, order?.Length ?? hot.Count);
        for (int pos = firstOrdinal; pos < n; pos++)
        {
            int i = order?[pos] ?? pos;
            Add((uint)pos, HotTierEventSource.EventAt(hot, pool, i));
        }
    }

    /// <summary>
    /// Reference build via the old per-event <c>Dictionary</c> path. Kept only as the
    /// correctness oracle for the streaming-parity test; not used in production.
    /// </summary>
    public void BuildReference(HotTierSegment hot, StringInternPool pool, int[]? order = null)
    {
        int n = order?.Length ?? hot.Count;
        for (int pos = 0; pos < n; pos++)
        {
            int  i      = order?[pos] ?? pos;
            uint offset = (uint)pos;
            IndexHeaderFieldsReference(HotTierEventSource.EventAt(hot, pool, i), offset);

            var props = hot.ReadPropertiesPayload(i, pool);
            if (props is not null)
                FlattenProperties(string.Empty, props, offset, depth: 0);
        }
    }

    // ── Per-event header fields ────────────────────────────────────────────────
    //
    // BLOOM ADDS HAPPEN ON FIRST SIGHT ONLY. The filter is a set: adding a term it already
    // holds sets bits that are already set, and costs a case fold, a UTF-8 encode, three
    // Murmur passes and two random writes into a multi-MB array. The inverted index already
    // knows whether a (property, value) is new — IndexAddOutcome — so the level, the service,
    // the exception fields and every property key and value go to the bloom exactly once per
    // distinct term. The template is not in the inverted index and is memoised by reference
    // (a 256-slot direct-mapped cache: templates are interned, and a miss only costs the add
    // the old code made every time). The bits are identical to adding on every event —
    // IndexBuildParityTests pins that against the reference build, which still adds every
    // time on purpose. What the writer sizes the next group's filter from is the number of
    // terms PRESENTED (_bloomPresented), repeats included, exactly the count it saw before.

    private static readonly byte[][] LevelUtf8 = BuildLevelTable();

    private static byte[][] BuildLevelTable()
    {
        var t = new byte[8][];
        for (int i = 0; i < t.Length; i++)
            t[i] = System.Text.Encoding.UTF8.GetBytes(((LogLevel)i).ToSeqString());
        return t;
    }

    private void IndexHeaderFields(in SegmentEventRef ev, uint offset)
    {
        // Level — inverted + bloom. Six spellings, pre-encoded once for the process.
        int lvl = (int)ev.Level;
        var levelUtf8 = (uint)lvl < (uint)LevelUtf8.Length ? LevelUtf8[lvl] : LevelUtf8[(int)LogLevel.Information];
        if (_inverted.AddUtf8(offset, "@l"u8, levelUtf8) != IndexAddOutcome.Existing) _bloom.AddUtf8(levelUtf8);
        _bloomPresented++;

        // Message template — trigram only.
        string template = ev.MessageTemplate;
        if (!string.IsNullOrEmpty(template))
        {
            _trigram.Add(offset, template);
            BloomAddTemplate(template);
        }

        // Exception (structured). The index is the ONLY consumer that needs anything of it —
        // type, message and inner type — so this is where the decode belongs. On the merge
        // path that is a span read that skips the stack trace; the writer copies the same
        // bytes through untouched.
        IndexException(in ev, offset);

        // TraceId / SpanId — 32 / 16 lowercase hex digits, formatted straight into stack scratch.
        Span<byte> hex = stackalloc byte[32];
        if (ev.HasTraceId)
        {
            ev.TraceIdHi.TryFormat(hex,       out _, "x16");
            ev.TraceIdLo.TryFormat(hex[16..], out _, "x16");
            if (_inverted.AddUtf8(offset, "@tr"u8, hex) != IndexAddOutcome.Existing) _bloom.Add(hex);   // already folded
            _bloomPresented++;
        }
        if (ev.HasSpanId)
        {
            ev.SpanId.TryFormat(hex, out _, "x16");
            if (_inverted.AddUtf8(offset, "@sp"u8, hex[..16]) != IndexAddOutcome.Existing) _bloom.Add(hex[..16]);
            _bloomPresented++;
        }

        // ServiceName — interned, so memoised by reference: one transcode per distinct service.
        string? service = ev.ServiceName;
        if (!string.IsNullOrEmpty(service))
        {
            if (!ReferenceEquals(service, _serviceRef))
            {
                int max = System.Text.Encoding.UTF8.GetMaxByteCount(service.Length);
                if (max > _serviceUtf8.Length) _serviceUtf8 = new byte[Math.Max(max, _serviceUtf8.Length * 2)];
                _serviceLen = System.Text.Encoding.UTF8.GetBytes(service, _serviceUtf8);
                _serviceRef = service;
            }
            var svc = _serviceUtf8.AsSpan(0, _serviceLen);
            if (_inverted.AddUtf8(offset, "service.name"u8, svc) != IndexAddOutcome.Existing) _bloom.AddUtf8(svc);
            _bloomPresented++;
        }
    }

    private string? _serviceRef;
    private byte[]  _serviceUtf8 = new byte[64];
    private int     _serviceLen;

    private void IndexException(in SegmentEventRef ev, uint offset)
    {
        if (ev.Exception is { } exception)
        {
            // Flush path: the hot tier holds the decoded object.
            AddExists(offset);
            if (!string.IsNullOrEmpty(exception.Type))
            {
                AddString(offset, "@x.type"u8, exception.Type);
                if (exception.Type.Length >= 3) _trigram.Add(offset, exception.Type);
            }
            if (!string.IsNullOrEmpty(exception.Message) && exception.Message.Length >= 3)
                _trigram.Add(offset, exception.Message);
            if (exception.Inner is { Type.Length: > 0 } inner)
                AddString(offset, "@x.inner.type"u8, inner.Type);
            return;
        }

        if (ev.ExceptionPayload.IsEmpty) return;

        // Merge path: the raw msgpack, decoded whole (stack trace included) — replaced by a
        // span read of the three indexed fields in the next change.
        var decoded = ev.DecodeException();
        if (decoded is null) return;
        AddExists(offset);
        if (!string.IsNullOrEmpty(decoded.Type))
        {
            AddString(offset, "@x.type"u8, decoded.Type);
            if (decoded.Type.Length >= 3) _trigram.Add(offset, decoded.Type);
        }
        if (!string.IsNullOrEmpty(decoded.Message) && decoded.Message.Length >= 3)
            _trigram.Add(offset, decoded.Message);
        if (decoded.Inner is { Type.Length: > 0 } decodedInner)
            AddString(offset, "@x.inner.type"u8, decodedInner.Type);
    }

    private void AddExists(uint offset)
    {
        if (_inverted.AddUtf8(offset, "@x.exists"u8, "true"u8) != IndexAddOutcome.Existing) _bloom.Add("@x.exists"u8);
        _bloomPresented++;
    }

    /// <summary>A header string through the transcoding overload — exception fields on the flush path.</summary>
    private void AddString(uint offset, ReadOnlySpan<byte> property, string value)
    {
        int max = System.Text.Encoding.UTF8.GetMaxByteCount(value.Length);
        byte[]? rented = max > 512 ? ArrayPool<byte>.Shared.Rent(max) : null;
        Span<byte> buf = rented ?? stackalloc byte[512];
        try
        {
            int n = System.Text.Encoding.UTF8.GetBytes(value, buf);
            if (_inverted.AddUtf8(offset, property, buf[..n]) != IndexAddOutcome.Existing) _bloom.AddUtf8(buf[..n]);
            _bloomPresented++;
        }
        finally
        {
            if (rented is not null) ArrayPool<byte>.Shared.Return(rented);
        }
    }

    /// <summary>Trigram of a UTF-8 value: byte-wise fold when ASCII, the UTF-16 path otherwise.</summary>
    private void TrigramUtf8(uint offset, ReadOnlySpan<byte> utf8)
    {
        if (utf8.Length < 3) return;
        EnsureFold(utf8.Length);
        if (System.Text.Ascii.ToLower(utf8, _fold, out int n) == OperationStatus.Done)
            _trigram.AddFoldedAscii(offset, _fold.AsSpan(0, n));
        else
            _trigram.Add(offset, utf8);
    }

    /// <summary>The reference oracle's header path: strings, adding to the bloom every time.</summary>
    private void IndexHeaderFieldsReference(in SegmentEventRef ev, uint offset)
    {
        string levelStr = ev.Level.ToSeqString();
        _inverted.Add(offset, "@l", levelStr);
        _bloom.Add(levelStr);
        _bloomPresented++;

        string template = ev.MessageTemplate;
        if (!string.IsNullOrEmpty(template))
        {
            _trigram.Add(offset, template);
            _bloom.Add(template);
            _bloomPresented++;
        }

        var exception = ev.DecodeException();
        if (exception is not null)
        {
            _inverted.Add(offset, ClefFields.ExceptionExists, "true");
            _bloom.Add(ClefFields.ExceptionExists);
            _bloomPresented++;
            if (!string.IsNullOrEmpty(exception.Type))
            {
                _inverted.Add(offset, ClefFields.ExceptionType, exception.Type);
                _bloom.Add(exception.Type);
                _bloomPresented++;
                if (exception.Type.Length >= 3) _trigram.Add(offset, exception.Type);
            }
            if (!string.IsNullOrEmpty(exception.Message) && exception.Message.Length >= 3)
                _trigram.Add(offset, exception.Message);
            if (exception.Inner is { Type.Length: > 0 } inner)
            {
                _inverted.Add(offset, ClefFields.ExceptionInnerType, inner.Type);
                _bloom.Add(inner.Type);
                _bloomPresented++;
            }
        }

        if (ev.HasTraceId)
        {
            string traceHex = TraceIdHelper.FormatTraceId(ev.TraceIdHi, ev.TraceIdLo)!;
            _inverted.Add(offset, ClefFields.TraceId, traceHex);
            _bloom.Add(traceHex);
            _bloomPresented++;
        }
        if (ev.HasSpanId)
        {
            string spanHex = TraceIdHelper.FormatSpanId(ev.SpanId)!;
            _inverted.Add(offset, ClefFields.SpanId, spanHex);
            _bloom.Add(spanHex);
            _bloomPresented++;
        }
        if (!string.IsNullOrEmpty(ev.ServiceName))
        {
            _inverted.Add(offset, ClefFields.ServiceName, ev.ServiceName);
            _bloom.Add(ev.ServiceName);
            _bloomPresented++;
        }
    }

    private void BloomAddTemplate(string template)
    {
        _bloomPresented++;
        int slot = System.Runtime.CompilerServices.RuntimeHelpers.GetHashCode(template) & (TemplateMemoSlots - 1);
        if (ReferenceEquals(_templateMemo[slot], template)) return;
        _templateMemo[slot] = template;
        _bloom.Add(template);
    }

    private const int TemplateMemoSlots = 256;
    private readonly string?[] _templateMemo = new string?[TemplateMemoSlots];

    /// <summary>Terms handed to the bloom, repeats included — see <see cref="BloomTermsAdded"/>.</summary>
    private long _bloomPresented;

    // ── Streaming property walk (msgpack → indexes, no Dictionary/boxing) ───────
    private void IndexPropertiesStreaming(ReadOnlySpan<byte> payload, uint offset)
    {
        if (payload.IsEmpty) return;
        // Read in place: the hot tier's native arena or the merge's block buffer, pinned for the
        // walk. Nothing is copied.
        fixed (byte* p = payload)
        {
            _payload.Set(p, payload.Length);
            try
            {
                var reader = new MessagePackReader(new ReadOnlySequence<byte>(_payload.Memory));
                try { WalkMap(ref reader, 0, offset, 0); }
                catch { /* malformed payload — index what we could, mirror old try/catch tolerance */ }
            }
            finally
            {
                _payload.Set(null, 0);
            }
        }
    }

    private void WalkMap(ref MessagePackReader reader, int prefixLen, uint offset, int depth)
    {
        if (depth > _maxFlattenDepth) { reader.Skip(); return; }
        int count = reader.ReadMapHeader();
        for (int e = 0; e < count; e++)
        {
            ReadOnlySpan<byte> keyUtf8 = ReadStr(ref reader);
            EnsureKey(prefixLen + keyUtf8.Length + 1);
            keyUtf8.CopyTo(_key.AsSpan(prefixLen));
            WalkValue(ref reader, prefixLen + keyUtf8.Length, offset, depth);
        }
    }

    private void WalkValue(ref MessagePackReader reader, int flatLen, uint offset, int depth)
    {
        switch (reader.NextMessagePackType)
        {
            case MessagePackType.Map:
                EnsureKey(flatLen + 1);
                _key[flatLen] = (byte)ClefFields.PropertyPathSeparator;
                WalkMap(ref reader, flatLen + 1, offset, depth + 1);
                break;

            case MessagePackType.Array:
                int n = reader.ReadArrayHeader();
                for (int i = 0; i < n; i++) WalkValue(ref reader, flatLen, offset, depth);
                break;

            default:
                AddScalar(ref reader, flatLen, offset);
                break;
        }
    }

    private void AddScalar(ref MessagePackReader reader, int flatLen, uint offset)
    {
        var flatKey = _key.AsSpan(0, flatLen);

        switch (reader.NextMessagePackType)
        {
            case MessagePackType.String:
            {
                ReadOnlySpan<byte> v = ReadStr(ref reader);
                int prop = _inverted.PropertyId(flatKey, out bool newKey);
                var r    = _inverted.AddValue(offset, prop, v);          // serialised == plain for strings
                _bloomPresented += 2;
                if (newKey) _bloom.AddUtf8(flatKey);
                FoldValue(offset, v, bloom: r == IndexAddOutcome.NewValue, trigram: v.Length >= 3);
                break;
            }
            case MessagePackType.Integer:
            {
                if (reader.NextCode == MessagePackCode.UInt64)
                {
                    ulong u = reader.ReadUInt64();
                    if (u > (ulong)long.MaxValue)
                    {
                        // ulong > long.Max: SerialiseValue default → plain ToString(), no prefix.
                        u.TryFormat(_val, out int uw, default, System.Globalization.CultureInfo.InvariantCulture);
                        AddNumeric(offset, flatKey, _val.AsSpan(0, uw), _val.AsSpan(0, uw));
                        break;
                    }
                    AddLong((long)u, offset, flatKey); break;
                }
                AddLong(reader.ReadInt64(), offset, flatKey);
                break;
            }
            case MessagePackType.Float:
            {
                double d = reader.ReadDouble();
                // serialised = "\0d" + R-format; plain = same digits (default ToString == R in modern .NET).
                _val[0] = 0; _val[1] = (byte)'d';
                d.TryFormat(_val.AsSpan(2), out int w, "R", System.Globalization.CultureInfo.InvariantCulture);
                AddNumeric(offset, flatKey, _val.AsSpan(2, w), _val.AsSpan(0, 2 + w));
                break;
            }
            case MessagePackType.Boolean:
            {
                bool b = reader.ReadBoolean();
                int prop = _inverted.PropertyId(flatKey, out bool newKey);
                var r    = _inverted.AddValue(offset, prop, b ? "\0true"u8 : "\0false"u8);
                _bloomPresented += 2;
                if (newKey) _bloom.AddUtf8(flatKey);
                if (r == IndexAddOutcome.NewValue) _bloom.Add(b ? "true"u8 : "false"u8);   // "True"/"False" folded
                _trigram.AddFoldedAscii(offset, b ? "true"u8 : "false"u8);
                break;
            }
            case MessagePackType.Nil:
            {
                reader.ReadNil();
                int prop = _inverted.PropertyId(flatKey, out bool newKey);
                var r    = _inverted.AddValue(offset, prop, "\0null"u8);
                _bloomPresented += 2;
                if (newKey) _bloom.AddUtf8(flatKey);
                if (r == IndexAddOutcome.NewValue) _bloom.Add(ReadOnlySpan<byte>.Empty);   // v?.ToString() ?? "" → ""
                break;
            }
            default:
                reader.Skip();
                break;
        }
    }

    private void AddLong(long l, uint offset, ReadOnlySpan<byte> flatKey)
    {
        _val[0] = 0; _val[1] = (byte)'l';
        l.TryFormat(_val.AsSpan(2), out int w, default, System.Globalization.CultureInfo.InvariantCulture);
        AddNumeric(offset, flatKey, _val.AsSpan(2, w), _val.AsSpan(0, 2 + w));
    }

    /// <summary>
    /// Files one numeric scalar: typed value in the inverted bucket, plain digits in the
    /// bloom, and — since this change — the plain digits in the trigram index too.
    ///
    /// <para><c>FilterEvaluator</c>'s <c>contains</c>/<c>startsWith</c>/<c>like</c> stringify
    /// whatever the property holds (<c>val?.ToString()</c>), so <c>contains(StatusCode,'50')</c>
    /// matches a msgpack integer 503 on a full scan. The trigram index saw string values only,
    /// and <c>SegmentTrigramIndex.Lookup</c> reads a missing trigram in a populated index as
    /// PROOF of absence — so that predicate returned rows while the events were hot and
    /// dropped the segment unread the moment it flushed.</para>
    ///
    /// <para>The alternative was to narrow the SCAN to the index (what free-text search does
    /// in <c>FilterEvaluator.ValueContainsTerm</c>), but that makes substring search over
    /// numbers silently match nothing in BOTH tiers — consistent and useless. Widening the
    /// index is the direction that keeps rows, and an index covering MORE than the scan costs
    /// a re-check, never a row. Cost is bounded: <c>SegmentTrigramIndex.Add</c> ignores
    /// anything shorter than three characters, so one- and two-digit values add nothing.</para>
    /// </summary>
    private void AddNumeric(uint offset, ReadOnlySpan<byte> flatKey, ReadOnlySpan<byte> plain, ReadOnlySpan<byte> serialised)
    {
        int prop = _inverted.PropertyId(flatKey, out bool newKey);
        var r    = _inverted.AddValue(offset, prop, serialised);
        _bloomPresented += 2;
        if (newKey) _bloom.AddUtf8(flatKey);
        FoldValue(offset, plain, bloom: r == IndexAddOutcome.NewValue, trigram: true);
    }

    /// <summary>
    /// Case-folds a value once and feeds the bloom (if asked) and the trigram from the same
    /// bytes. ASCII — every number, id and the vast majority of strings — is lowered byte-wise
    /// by <c>Ascii.ToLower</c>, which is what <c>ToLowerInvariant</c> does to ASCII, so the
    /// bloom hashes and trigram keys are the ones the UTF-16 path produces. Anything else is
    /// decoded and goes through that UTF-16 path unchanged.
    /// </summary>
    private void FoldValue(uint offset, ReadOnlySpan<byte> v, bool bloom, bool trigram)
    {
        if (!bloom && !trigram) return;
        EnsureFold(v.Length);
        if (System.Text.Ascii.ToLower(v, _fold, out int n) == OperationStatus.Done)
        {
            var folded = _fold.AsSpan(0, n);
            if (bloom)   _bloom.Add(folded);
            if (trigram) _trigram.AddFoldedAscii(offset, folded);
            return;
        }

        int chars = System.Text.Encoding.UTF8.GetCharCount(v);
        if (chars > _wide.Length) _wide = new char[Math.Max(chars, _wide.Length * 2)];
        System.Text.Encoding.UTF8.GetChars(v, _wide);
        var text = _wide.AsSpan(0, chars);
        if (bloom)   _bloom.Add(text);
        if (trigram) _trigram.Add(offset, text);
    }

    private static ReadOnlySpan<byte> ReadStr(ref MessagePackReader reader)
        => reader.TryReadStringSpan(out ReadOnlySpan<byte> span) ? span : ReadStrSlow(ref reader);

    private static byte[] _empty = System.Array.Empty<byte>();
    private static ReadOnlySpan<byte> ReadStrSlow(ref MessagePackReader reader)
    {
        // Rare: string spans buffer segments. Our payload is one array, so this is unreachable,
        // but keep it correct — materialise once.
        var seq = reader.ReadStringSequence();
        return seq.HasValue ? seq.Value.ToArray() : _empty;
    }

    private void EnsureKey(int len)  { if (len > _key.Length)  System.Array.Resize(ref _key,  Math.Max(len, _key.Length * 2)); }
    private void EnsureFold(int len) { if (len > _fold.Length) System.Array.Resize(ref _fold, Math.Max(len, _fold.Length * 2)); }

    // ── Reference recursive flatten (used only by BuildReference) ──────────────
    private void FlattenProperties(string prefix, Dictionary<string, object?> dict, uint offset, int depth)
    {
        if (depth > _maxFlattenDepth) return;
        foreach (var (k, v) in dict)
        {
            string flatKey = prefix.Length == 0 ? k : string.Concat(prefix, ClefFields.PropertyPathSeparator, k);
            FlattenValue(flatKey, v, offset, depth);
        }
    }

    private void FlattenValue(string flatKey, object? v, uint offset, int depth)
    {
        switch (v)
        {
            case Dictionary<string, object?> nested:
                FlattenProperties(flatKey, nested, offset, depth + 1);
                break;
            case object[] arr:
                foreach (var item in arr) FlattenValue(flatKey, item, offset, depth);
                break;
            default:
                _inverted.Add(offset, flatKey, v);
                // Added on EVERY event here, deliberately: the streaming path adds on first
                // sight only, and the byte-for-byte parity of the two bloom sections is what
                // proves that a set does not care.
                _bloom.Add(flatKey);
                _bloomPresented += 2;
                // Invariant, exactly like the streaming path's `plain` — the parity test
                // compares the two builds byte for byte, and a ru-KZ host formats 2.5 as "2,5".
                string valStr = IndexValueForms.PlainText(v);
                _bloom.Add(valStr);
                // Every scalar's text, not just strings — mirrors AddNumeric/AddScalar on the
                // streaming path, which is what the parity test compares this against.
                if (valStr.Length >= 3) _trigram.Add(offset, valStr);
                break;
        }
    }

    // ── Serialise ─────────────────────────────────────────────────────────────

    /// <summary>
    /// Bloom terms PRESENTED so far, repeats included — see
    /// <see cref="ISegmentIndexSink.BloomTermsAdded"/>. Counted by the builder rather than the
    /// filter since the builder started skipping repeats (see <see cref="IndexHeaderFields"/>):
    /// the writer's next-group forecast and its group-full check were calibrated on the
    /// presented count, and this keeps both exactly where they were. The filter's own
    /// <see cref="SegmentBloomFilter.AddedTermCount"/> is the de-duplicated number — sizing
    /// by THAT (10 bits per distinct term is the design point) would shrink every bloom
    /// section after the first and is a deliberate follow-up, not a side effect of this.
    /// Managed state, readable after <see cref="Dispose"/>.
    /// </summary>
    public long BloomTermsAdded => _bloomPresented;

    /// <summary>
    /// Terms the filter was actually able to buy — see
    /// <see cref="ISegmentIndexSink.BloomTermCapacity"/>. Read off the filter rather than
    /// recomputed from this constructor's two arguments, because the filter is where the request
    /// is bounded: <see cref="SegmentBloomFilter.Create"/> caps one filter's bytes, and a
    /// capacity taken from the request would have the writer fill bits that were never allocated.
    /// </summary>
    public long BloomTermCapacity => _bloom.Capacity;

    /// <summary>
    /// Pooled managed bytes the two accumulators hold right now — term slabs, tables, entries,
    /// posting slabs. This is the number to read for "what does one in-flight group retain":
    /// a GC-heap delta cannot tell a buffer this builder holds from one parked in the
    /// <c>ArrayPool</c> by a builder that already sealed.
    /// </summary>
    public long BuildRetainedBytes => _inverted.BuildRetainedBytes + _trigram.BuildRetainedBytes;

    /// <summary>
    /// The three sections one at a time, for probes and tests that want to compare or size just
    /// one of them; production takes all three at once through <see cref="Serialise"/>.
    ///
    /// <para>These predate the builder having a lifetime at all, and read as though it still had
    /// none. It does: <see cref="Dispose"/> frees the bloom's bits, so
    /// <see cref="SerialisedBloomFilter"/> after disposal is a read of freed memory. It is the
    /// FILTER that enforces this rather than an argument check here, because the builder is not
    /// the only door to those bytes — <see cref="SegmentBloomFilter"/> is public, the query path
    /// deserialises and disposes its own, and a guard placed on this property would leave every
    /// other caller reading garbage exactly as before.</para>
    /// </summary>
    public byte[] SerialisedInvertedIndex  => _inverted.Serialise();
    public byte[] SerialisedTrigramIndex   => _trigram.Serialise();
    public byte[] SerialisedBloomFilter    => _bloom.Serialise();

    public (byte[] Inverted, byte[] Trigram, byte[] Bloom) Serialise()
    {
        RecordHints();
        return (_inverted.Serialise(), _trigram.Serialise(), _bloom.Serialise());
    }

    /// <summary>What this group measured, for the next one to size itself by.</summary>
    private void RecordHints() => _hints?.Record(_inverted.TermCount, _trigram.BucketCount);

    /// <summary>
    /// The production path — see <see cref="ISegmentIndexSink.WriteSections"/>. The inverted
    /// and trigram sections stream through one pooled 1 MB buffer straight into the file; the
    /// bloom goes from its native bits to the stream with no managed copy at all. Where a
    /// section's length is known up front it is written first; otherwise a placeholder is
    /// patched once the section is out (the file is seekable, and the seek is per group).
    /// Same bytes as <see cref="Serialise"/> framed by the writer — pinned by
    /// <c>SectionStreamingTests</c>.
    /// </summary>
    public void WriteSections(Stream destination, out long invertedOffset, out long trigramOffset, out long bloomOffset)
    {
        RecordHints();
        using var w = new StreamSectionWriter(destination);

        invertedOffset = destination.Position;
        WritePlaceholder(destination);
        w.ResetCount();
        _inverted.WriteTo(w);
        w.Flush();
        PatchLength(destination, invertedOffset, w.Total);

        trigramOffset = destination.Position;
        long trigramLen = _trigram.ExactSerialisedSize();
        WriteLength(destination, trigramLen);
        w.ResetCount();
        _trigram.WriteTo(w);
        w.Flush();
        if (w.Total != trigramLen)
            throw new InvalidOperationException($"trigram section wrote {w.Total} bytes against a computed {trigramLen}");

        bloomOffset = destination.Position;
        WriteLength(destination, _bloom.SerialisedLength);
        _bloom.WriteTo(destination);
    }

    private static void WritePlaceholder(Stream s)
    {
        Span<byte> z = stackalloc byte[4];
        z.Clear();
        s.Write(z);
    }

    private static void WriteLength(Stream s, long len)
    {
        Span<byte> b = stackalloc byte[4];
        System.Buffers.Binary.BinaryPrimitives.WriteUInt32LittleEndian(b, checked((uint)len));
        s.Write(b);
    }

    private static void PatchLength(Stream s, long at, long len)
    {
        long end = s.Position;
        s.Position = at;
        WriteLength(s, len);
        s.Position = end;
    }

    /// <summary>
    /// Frees the bloom filter's bits and hands the accumulators' pooled buffers back.
    ///
    /// <para>The bits live in <c>NativeMemory</c> and <see cref="SegmentBloomFilter"/> has no
    /// finaliser, so before the sink contract made the builder's lifetime explicit every sealed
    /// group leaked its filter off-heap — ~10 MB per group at the documented ~10 bits/term
    /// sizing, invisible to every managed-heap probe. The trigram accumulator's slot table,
    /// buckets and posting slabs are <c>ArrayPool</c> rentals for the same reason in reverse:
    /// tens of MB per group that would otherwise be fresh LOH allocations every flush. Both are
    /// released here, so <see cref="Serialise"/> and the per-section accessors throw
    /// <see cref="ObjectDisposedException"/> afterwards — a returned pooled array is somebody
    /// else's the moment they rent it, which is exactly the freed-memory read the bloom guard
    /// exists for.</para>
    /// </summary>
    public void Dispose()
    {
        _bloom.Dispose();
        _trigram.ReleaseBuildBuffers();
        _inverted.ReleaseBuildBuffers();
    }
}
