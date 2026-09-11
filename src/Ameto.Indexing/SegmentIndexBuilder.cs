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
public sealed class SegmentIndexBuilder : ISegmentIndexSink
{
    private readonly SegmentInvertedIndex _inverted = new();
    private readonly SegmentTrigramIndex  _trigram  = new();
    private readonly SegmentBloomFilter   _bloom;

    private readonly int _maxFlattenDepth;

    // Per-build scratch (Build is single-threaded per flush). Grown on demand.
    private byte[] _mp  = new byte[512];   // payload copy for MessagePackReader (needs a sequence)
    private char[] _key = new char[256];   // accumulated flat (dot-notation) key
    private char[] _val = new char[128];   // formatted value (serialised form, prefix at [0..2])

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
    public SegmentIndexBuilder(int expectedEventCount, int maxFlattenDepth = 5,
                               int estimatedTermsPerEvent = EstimatedBloomTermsPerEvent)
    {
        long termsPerEvent = estimatedTermsPerEvent > 0 ? estimatedTermsPerEvent : EstimatedBloomTermsPerEvent;
        _bloom            = SegmentBloomFilter.Create((long)Math.Max(1, expectedEventCount) * termsPerEvent);
        _maxFlattenDepth  = maxFlattenDepth;
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
            IndexHeaderFields(HotTierEventSource.EventAt(hot, pool, i), offset);

            var props = hot.ReadPropertiesPayload(i, pool);
            if (props is not null)
                FlattenProperties(string.Empty, props, offset, depth: 0);
        }
    }

    // ── Per-event header fields (shared by both paths) ─────────────────────────
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

    private void IndexHeaderFields(in SegmentEventRef ev, uint offset)
    {
        // Level — inverted + bloom
        string levelStr = ev.Level.ToSeqString();
        if (_inverted.Add(offset, "@l", levelStr) != IndexAddOutcome.Existing) _bloom.Add(levelStr);
        _bloomPresented++;

        // Message template — trigram only.
        string template = ev.MessageTemplate;
        if (!string.IsNullOrEmpty(template))
        {
            _trigram.Add(offset, template);
            BloomAddTemplate(template);
        }

        // Exception (structured). The index is the ONLY consumer that needs the object graph —
        // it indexes type, message and inner type as strings — so this is where the decode
        // belongs. On the merge path the writer copies the same bytes through untouched.
        var exception = ev.DecodeException();
        if (exception is not null)
        {
            if (_inverted.Add(offset, ClefFields.ExceptionExists, "true") != IndexAddOutcome.Existing)
                _bloom.Add(ClefFields.ExceptionExists);
            _bloomPresented++;

            if (!string.IsNullOrEmpty(exception.Type))
            {
                if (_inverted.Add(offset, ClefFields.ExceptionType, exception.Type) != IndexAddOutcome.Existing)
                    _bloom.Add(exception.Type);
                _bloomPresented++;
                if (exception.Type.Length >= 3) _trigram.Add(offset, exception.Type);
            }
            if (!string.IsNullOrEmpty(exception.Message) && exception.Message.Length >= 3)
                _trigram.Add(offset, exception.Message);
            if (exception.Inner is { Type.Length: > 0 } inner)
            {
                if (_inverted.Add(offset, ClefFields.ExceptionInnerType, inner.Type) != IndexAddOutcome.Existing)
                    _bloom.Add(inner.Type);
                _bloomPresented++;
            }
        }

        // TraceId / SpanId
        if (ev.HasTraceId)
        {
            string traceHex = TraceIdHelper.FormatTraceId(ev.TraceIdHi, ev.TraceIdLo)!;
            if (_inverted.Add(offset, ClefFields.TraceId, traceHex) != IndexAddOutcome.Existing) _bloom.Add(traceHex);
            _bloomPresented++;
        }
        if (ev.HasSpanId)
        {
            string spanHex = TraceIdHelper.FormatSpanId(ev.SpanId)!;
            if (_inverted.Add(offset, ClefFields.SpanId, spanHex) != IndexAddOutcome.Existing) _bloom.Add(spanHex);
            _bloomPresented++;
        }

        // ServiceName
        if (!string.IsNullOrEmpty(ev.ServiceName))
        {
            if (_inverted.Add(offset, ClefFields.ServiceName, ev.ServiceName) != IndexAddOutcome.Existing)
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
        if (payload.Length > _mp.Length) _mp = new byte[Math.Max(payload.Length, _mp.Length * 2)];
        payload.CopyTo(_mp);
        var reader = new MessagePackReader(new ReadOnlySequence<byte>(_mp, 0, payload.Length));
        try { WalkMap(ref reader, 0, offset, 0); }
        catch { /* malformed payload — index what we could, mirror old try/catch tolerance */ }
    }

    private void WalkMap(ref MessagePackReader reader, int prefixLen, uint offset, int depth)
    {
        if (depth > _maxFlattenDepth) { reader.Skip(); return; }
        int count = reader.ReadMapHeader();
        for (int e = 0; e < count; e++)
        {
            ReadOnlySpan<byte> keyUtf8 = ReadStr(ref reader);
            int keyChars = System.Text.Encoding.UTF8.GetCharCount(keyUtf8);
            EnsureKey(prefixLen + keyChars + 1);
            System.Text.Encoding.UTF8.GetChars(keyUtf8, _key.AsSpan(prefixLen));
            WalkValue(ref reader, prefixLen + keyChars, offset, depth);
        }
    }

    private void WalkValue(ref MessagePackReader reader, int flatLen, uint offset, int depth)
    {
        switch (reader.NextMessagePackType)
        {
            case MessagePackType.Map:
                EnsureKey(flatLen + 1);
                _key[flatLen] = ClefFields.PropertyPathSeparator;
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
                ReadOnlySpan<byte> vUtf8 = ReadStr(ref reader);
                int vc = System.Text.Encoding.UTF8.GetCharCount(vUtf8);
                EnsureVal(vc);
                System.Text.Encoding.UTF8.GetChars(vUtf8, _val);
                var v = _val.AsSpan(0, vc);
                var r = _inverted.AddSpan(offset, flatKey, v);   // serialised == plain for strings
                _bloomPresented += 2;
                if (r == IndexAddOutcome.NewProperty) _bloom.Add(flatKey);
                if (r != IndexAddOutcome.Existing)    _bloom.Add(v);
                if (vc >= 3) _trigram.Add(offset, v);
                break;
            }
            case MessagePackType.Integer:
            {
                if (reader.NextCode == MessagePackCode.UInt64)
                {
                    ulong u = reader.ReadUInt64();
                    if (u > (ulong)long.MaxValue) { WriteUnsigned(u, out var pl, out var sr); AddNumeric(offset, flatKey, pl, sr); break; }
                    AddLong((long)u, offset, flatKey); break;
                }
                AddLong(reader.ReadInt64(), offset, flatKey);
                break;
            }
            case MessagePackType.Float:
            {
                double d = reader.ReadDouble();
                // serialised = "\0d" + R-format; plain = same digits (default ToString == R in modern .NET).
                _val[0] = '\0'; _val[1] = 'd';
                EnsureVal(2 + 40);
                d.TryFormat(_val.AsSpan(2), out int w, "R", System.Globalization.CultureInfo.InvariantCulture);
                AddNumeric(offset, flatKey, _val.AsSpan(2, w), _val.AsSpan(0, 2 + w));
                break;
            }
            case MessagePackType.Boolean:
            {
                bool b = reader.ReadBoolean();
                var r  = _inverted.AddSpan(offset, flatKey, b ? "\0true" : "\0false");
                _bloomPresented += 2;
                if (r == IndexAddOutcome.NewProperty) _bloom.Add(flatKey);
                if (r != IndexAddOutcome.Existing)    _bloom.Add(b ? "True" : "False");
                _trigram.Add(offset, b ? "True" : "False");
                break;
            }
            case MessagePackType.Nil:
            {
                reader.ReadNil();
                var r = _inverted.AddSpan(offset, flatKey, "\0null");
                _bloomPresented += 2;
                if (r == IndexAddOutcome.NewProperty) _bloom.Add(flatKey);
                if (r != IndexAddOutcome.Existing)    _bloom.Add(ReadOnlySpan<char>.Empty);   // v?.ToString() ?? "" → ""
                break;
            }
            default:
                reader.Skip();
                break;
        }
    }

    private void AddLong(long l, uint offset, ReadOnlySpan<char> flatKey)
    {
        _val[0] = '\0'; _val[1] = 'l';
        EnsureVal(2 + 24);
        l.TryFormat(_val.AsSpan(2), out int w, default, System.Globalization.CultureInfo.InvariantCulture);
        AddNumeric(offset, flatKey, _val.AsSpan(2, w), _val.AsSpan(0, 2 + w));
    }

    private void WriteUnsigned(ulong u, out ReadOnlySpan<char> plain, out ReadOnlySpan<char> serialised)
    {
        // ulong > long.Max: SerialiseValue default → plain ToString(), no prefix.
        EnsureVal(24);
        u.TryFormat(_val, out int w, default, System.Globalization.CultureInfo.InvariantCulture);
        plain = serialised = _val.AsSpan(0, w);
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
    private void AddNumeric(uint offset, ReadOnlySpan<char> flatKey, ReadOnlySpan<char> plain, ReadOnlySpan<char> serialised)
    {
        var r = _inverted.AddSpan(offset, flatKey, serialised);
        _bloomPresented += 2;
        if (r == IndexAddOutcome.NewProperty) _bloom.Add(flatKey);
        if (r != IndexAddOutcome.Existing)    _bloom.Add(plain);
        _trigram.Add(offset, plain);
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

    private void EnsureKey(int len) { if (len > _key.Length) System.Array.Resize(ref _key, Math.Max(len, _key.Length * 2)); }
    private void EnsureVal(int len) { if (len > _val.Length) System.Array.Resize(ref _val, Math.Max(len, _val.Length * 2)); }

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
        => (_inverted.Serialise(), _trigram.Serialise(), _bloom.Serialise());

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
    }
}
