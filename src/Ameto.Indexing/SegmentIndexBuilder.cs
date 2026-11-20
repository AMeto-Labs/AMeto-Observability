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
/// path (see <see cref="BuildReference"/>, exercised by the parity tests) for every valid UTF-8
/// input without a repeated key — keys that are not strings, nil and extension elements of arrays
/// and binary values included, which it files as the reader reads them (see the note above
/// <c>WalkMap</c>). A key repeated in one map is filed once per occurrence, where the dictionary
/// keeps only the last value: the index then holds more than the scan reads, which costs a
/// re-check and never a row. A key or value that is NOT valid UTF-8 is indexed as its raw bytes,
/// where the char-based walk indexed the U+FFFD-replaced decoding — see
/// <see cref="SegmentInvertedIndex"/>.
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
    // as UTF-8, and a value is case-folded ONCE for the bloom and the trigram together — byte-wise
    // when it is ASCII, which nearly every value is, and through CaseFold's table straight off the
    // UTF-8 when it is not. The old walk decoded every key and every value to UTF-16, folded them
    // as chars, encoded them back for the bloom, and lowered them again for the trigram — six
    // passes over each value's bytes per event.
    private readonly PinnedSpanMemoryManager _payload = new();   // MessagePackReader needs a sequence
    private byte[] _key  = new byte[256];   // accumulated flat (dot-notation) key, UTF-8
    private byte[] _val  = new byte[64];    // formatted numeric value (serialised form, prefix at [0..2])
    private byte[] _fold = new byte[256];   // case-folded ASCII value for bloom + trigram
    private char[] _wide = new char[256];   // case-folded non-ASCII value, one UTF-16 unit per scalar

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
    public SegmentIndexBuilder(int expectedEventCount, int maxFlattenDepth = IndexingOptions.DefaultMaxPropertyFlattenDepth,
                               int estimatedTermsPerEvent = EstimatedBloomTermsPerEvent,
                               IndexBuildHints? hints = null, Microsoft.Extensions.Logging.ILogger? log = null)
    {
        long termsPerEvent = estimatedTermsPerEvent > 0 ? estimatedTermsPerEvent : EstimatedBloomTermsPerEvent;
        _bloom            = SegmentBloomFilter.Create((long)Math.Max(1, expectedEventCount) * termsPerEvent);
        _maxFlattenDepth  = maxFlattenDepth;
        _hints            = hints;
        _log              = log;
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
        if (!_vouchedForExceptionMarker) VouchForExceptionMarker();
        IndexHeaderFields(in ev, fileOrdinal);
        IndexPropertiesStreaming(ev.Properties, fileOrdinal);
    }

    /// <summary>
    /// Files <see cref="ClefFields.ExceptionExistsComplete"/> — a property with no value — once
    /// per group, before its first event: this builder's word that the group's
    /// <c>@x.exists</c> bucket names EVERY row <see cref="ExceptionInfo.IsPresent"/> accepts
    /// (see <see cref="IndexException"/>, which files the marker before it reads the payload).
    /// A reader lets the marker answer <c>has(@x)</c> only where it finds this, so a group
    /// written before the marker was complete is scanned instead of trusted.
    ///
    /// <para>No value is ever filed under it, and that emptiness is the whole signal: a property
    /// enters the index with its first value everywhere else, so nothing but this call writes
    /// an empty one. Should a client send a property of this very name, its value lands in the
    /// same bucket, the bucket is no longer empty, and the group vouches for nothing — the
    /// reader scans, which is the safe way to be wrong. Not a bloom term: nothing probes for it.</para>
    /// </summary>
    private void VouchForExceptionMarker()
    {
        _vouchedForExceptionMarker = true;
        _inverted.PropertyId("@x.exists.complete"u8, out _);
    }

    private bool _vouchedForExceptionMarker;

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
            if (!_vouchedForExceptionMarker) VouchForExceptionMarker();
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
    //
    // "NEW TO THE INVERTED INDEX" STANDS FOR "NEW TO THE BLOOM" ONLY WHERE WHOEVER FILED THE
    // TERM FIRST GAVE THE BLOOM THE SAME TEXT. A client property may be named like a bucket
    // filed here, and a string in it that spells a value the header files IS the header's
    // term. For the level, the service, the ids and the exception types that costs no row:
    // each gives the bloom the very bytes it files, folded as the walk folds a string, so the
    // text is there whichever of the two came first. The exception marker is the one that
    // does not — its bloom term is the bucket's name — and it keeps its own account: see
    // AddExists.
    //
    // One thing that rule does NOT make true, said here so the sentences above are read as
    // narrowly as they hold: the KEY of such a property. Where the header made the bucket, the
    // walk never sees the key as new and never adds it, so for that input the filter differs
    // from the reference build's by that one term. Nothing asks the bloom for a property's
    // name — the gate probes values — so no query can tell.
    //
    // The same meeting happens between two SCALARS. The inverted index files a typed value
    // under a type-tagged form (`\0l5` for the long 5) and a string under its own bytes, so a
    // string that begins with NUL can spell a typed value's term exactly, and the two then
    // share it with different bloom texts. Whichever was filed second used to be skipped, and
    // `P = 5` found nothing once flushed beside the string "\0l5". Such a string now always
    // gives the bloom its text and marks its property, and a typed value under a marked
    // property always gives its own: see MaySpellATypedTerm. The three header strings a client
    // controls follow the same rule (AddHeaderString); the level and the ids are the server's
    // own text and never begin with NUL.

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
        // type, message and inner type — so this is where the decode belongs: a span read that
        // skips the stack trace; the writer copies the same bytes through untouched.
        IndexException(in ev, offset);

        // TraceId / SpanId — 32 / 16 lowercase hex digits, formatted straight into stack scratch.
        // The bucket files them as written (it copies them); the bloom takes them folded, which for
        // hex digits is upper case, done in place — every event has its own ids, so this runs per
        // event and must not cost a buffer.
        Span<byte> hex = stackalloc byte[32];
        if (ev.HasTraceId)
        {
            ev.TraceIdHi.TryFormat(hex,       out _, "x16");
            ev.TraceIdLo.TryFormat(hex[16..], out _, "x16");
            if (_inverted.AddUtf8(offset, "@tr"u8, hex) != IndexAddOutcome.Existing) BloomAddHex(hex);
            _bloomPresented++;
        }
        if (ev.HasSpanId)
        {
            ev.SpanId.TryFormat(hex, out _, "x16");
            if (_inverted.AddUtf8(offset, "@sp"u8, hex[..16]) != IndexAddOutcome.Existing) BloomAddHex(hex[..16]);
            _bloomPresented++;
        }

        // ServiceName — interned, so memoised by reference: one transcode per distinct service.
        // Filed under `@service` (ClefFields.ServiceName). Groups built before that filed it
        // under `service.name`; the readers fall back to that bucket for them.
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
            AddHeaderString(offset, "@service"u8, _serviceUtf8.AsSpan(0, _serviceLen));
        }
    }

    /// <summary>
    /// Files one of the header strings a client controls — the service, an exception's type or
    /// inner type — and gives the bloom its text on first sight, or every time when it may spell
    /// a typed term (<see cref="MaySpellATypedTerm"/>): a client property named like the bucket
    /// can file a typed value in it, which would share the term with a different text.
    /// </summary>
    private void AddHeaderString(uint offset, ReadOnlySpan<byte> bucket, ReadOnlySpan<byte> value)
    {
        int  prop   = _inverted.PropertyId(bucket, out _);
        bool tagged = MaySpellATypedTerm(value);
        if (tagged) NoteTaggedSpelling(prop);
        if (_inverted.AddValue(offset, prop, value) == IndexAddOutcome.NewValue || tagged) _bloom.AddUtf8(value);
        _bloomPresented++;
    }

    private string? _serviceRef;
    private byte[]  _serviceUtf8 = new byte[64];
    private int     _serviceLen;

    /// <summary>Lowercase hex digits into the bloom as <see cref="CaseFold"/> folds them — upper
    /// case, in the caller's scratch, which the bucket has already copied from.</summary>
    private void BloomAddHex(Span<byte> hex)
    {
        System.Text.Ascii.ToUpperInPlace(hex, out _);
        _bloom.Add(hex);
    }

    private void IndexException(in SegmentEventRef ev, uint offset)
    {
        // nil, an empty legacy string and any non-string, non-map value are "no exception" to
        // ExceptionInfo.FromBytes, not a malformed one. TryReadIndexFields answers false for all
        // three as it does for a broken map, so they are told apart here, before the count below
        // could take them for a producer writing exceptions this reader cannot read. An empty
        // column — every row this server writes without an exception — leaves on the first test.
        if (!ExceptionInfo.IsPresent(ev.ExceptionPayload))
        {
            if (!ev.ExceptionPayload.IsEmpty && !ReadsAsNoException(ev.ExceptionPayload))
                NoteMalformedException(offset, ev.ExceptionPayload.Length);
            return;
        }

        // THE MARKER FIRST, and on IsPresent's answer alone — before anything tries to read the
        // payload. LogEvent.HasException is this same probe over these same bytes (the writer
        // stores the column untouched), so the bucket names exactly the rows `has(@x)` is true
        // for, the ones whose map cannot be read below included. That is what
        // VouchForExceptionMarker promises a reader, and what lets the marker stand in for a
        // scan. It used to be filed after the read, so an unreadable map was a row the scan
        // counted and the bucket did not.
        AddExists(offset);

        // The raw msgpack, on the flush path and the merge path alike (the hot tier keeps the bytes
        // the client sent, as a segment's column does). Read only the three fields the index wants,
        // in place — the stack trace, 1-5 KB of UTF-16 nobody here reads, is skipped, not decoded.
        // That decode cost an Error-level merge 4 879 B per row for three short strings.
        fixed (byte* p = ev.ExceptionPayload)
        {
            _exception.Set(p, ev.ExceptionPayload.Length);
            try
            {
                if (!ExceptionInfo.TryReadIndexFields(_exception.Memory, out var type, out var message, out var innerType))
                {
                    // The row is written whatever its exception column holds, and it keeps its
                    // place in the marker above; what is lost is its place in the type, inner
                    // type and message terms. The old full decode threw here and
                    // failed the merge, so this is counted rather than silent: a non-zero count
                    // is a producer writing exception maps this reader cannot read (a non-string
                    // type, a truncated map), which is worth knowing about and not worth losing a
                    // compaction over. It is reported at Warning ONCE, when the group seals (see
                    // RecordHints), with the group's count — not per row, and not at Debug, which
                    // the production level never prints.
                    NoteMalformedException(offset, ev.ExceptionPayload.Length);
                    return;
                }
                if (!type.IsEmpty)
                {
                    type = WellFormed(type);
                    AddHeaderString(offset, "@x.type"u8, type);
                    TrigramUtf8(offset, type);
                }
                // ExceptionInfo.Read keeps an empty message as "" and the object path indexes
                // only a message of 3+ chars; a UTF-8 length of 3+ can be a 1-char non-ASCII
                // message, which the trigram then ignores itself — same outcome. The trigram
                // decodes the bytes itself, so an ill-formed message needs no WellFormed.
                if (!message.IsEmpty) TrigramUtf8(offset, message);
                if (!innerType.IsEmpty)
                {
                    innerType = WellFormed(innerType);
                    AddHeaderString(offset, "@x.inner.type"u8, innerType);
                }
            }
            finally
            {
                _exception.Set(null, 0);
            }
        }
    }

    private readonly PinnedSpanMemoryManager _exception = new();

    private char[]? _wellFormedChars;
    private byte[]? _wellFormedBytes;

    /// <summary>
    /// An exception's type as the reference oracle's string road files it. The hot tier keeps
    /// <c>@x</c> as the client sent it, and a client that mishandles encodings sends ill-formed
    /// UTF-8 in it; the tree reads that as U+FFFD per invalid sequence (<c>Encoding.UTF8</c>), and
    /// a term filed from the raw bytes instead differed from the oracle's — harmless to a lookup,
    /// which decodes stored terms the same way, but a term that is not UTF-8 on disk all the same.
    /// Well-formed bytes, which is every type a real client sends, come back as they are after one
    /// vectorised check; the transcode, through scratch of the builder's own, runs on the exception.
    /// (A property value on this path is filed as its bytes, as it always was.)
    /// </summary>
    private ReadOnlySpan<byte> WellFormed(ReadOnlySpan<byte> utf8)
    {
        if (System.Text.Unicode.Utf8.IsValid(utf8)) return utf8;

        if (_wellFormedChars is null || _wellFormedChars.Length < utf8.Length) _wellFormedChars = new char[Math.Max(utf8.Length, 64)];
        System.Text.Unicode.Utf8.ToUtf16(utf8, _wellFormedChars, out _, out int chars, replaceInvalidSequences: true, isFinalBlock: true);
        int max = chars * 3;   // U+FFFD is three bytes; nothing decodes to more per char
        if (_wellFormedBytes is null || _wellFormedBytes.Length < max) _wellFormedBytes = new byte[Math.Max(max, 192)];
        System.Text.Unicode.Utf8.FromUtf16(_wellFormedChars.AsSpan(0, chars), _wellFormedBytes, out _, out int bytes, isFinalBlock: true);
        return _wellFormedBytes.AsSpan(0, bytes);
    }

    /// <summary>
    /// Whether a payload <see cref="ExceptionInfo.IsPresent"/> calls absent really is "no
    /// exception" to delivery, rather than a torn header delivery throws on.
    ///
    /// <para>IsPresent answers from the lead byte and, for a string, the length that follows it —
    /// on purpose, since it runs per scanned row. So a str8/16/32 header too short to hold its own
    /// length (<c>D9</c>, <c>DA 00</c>, <c>DB 00 00 00</c>), or a scalar cut short, is ABSENT to it
    /// while <see cref="ExceptionInfo.FromBytes(ReadOnlyMemory{byte})"/> throws
    /// <see cref="EndOfStreamException"/>. Before IsPresent was consulted here those reached the
    /// malformed count, as everything TryReadIndexFields cannot read does; returning on IsPresent
    /// alone lost them from it, and only delivery noticed.</para>
    ///
    /// <para>FromBytes itself is asked, so the two cannot disagree. Over the shapes that get here
    /// it builds nothing — nil, an empty string and a skipped value are all null — so a readable
    /// one costs a header walk and no allocation, and none get here at all from a segment this
    /// server wrote, where a row without an exception has an empty column. IsPresent and
    /// FromBytes are unchanged: a presence probe still answers from the header, and delivery
    /// still throws.</para>
    ///
    /// <para>What counts as "throws" is <see cref="FileBounds.DescribesContent"/>, not a list of its
    /// own. The first version caught only MessagePackSerializationException and
    /// EndOfStreamException, and MessagePack 3.1.7 has a third answer for a torn header: a count
    /// of 2^31 or more (<c>DD 80 00 00 00</c>, or the same one array down) reaches a checked
    /// uint→int conversion in <c>TrySkip</c> / <c>TryReadArrayHeader</c> /
    /// <c>TryReadMapHeader</c> and throws <see cref="OverflowException"/>. That escaped
    /// <c>Add</c> and failed the merge, which the engine did not classify as corruption, so the
    /// same batch was re-selected every pass — where before this check the row was simply
    /// written. The shared list is also the list the engine quarantines a batch by, so what this
    /// lets escape is exactly what the engine will retry. Probed with two million byte-flipped
    /// exception maps and two million random 1-8 byte payloads, FromBytes threw nothing outside
    /// it (and a skip nested two million arrays deep did not recurse).</para>
    /// </summary>
    private bool ReadsAsNoException(ReadOnlySpan<byte> payload)
    {
        fixed (byte* p = payload)
        {
            _exception.Set(p, payload.Length);
            try
            {
                return ExceptionInfo.FromBytes(_exception.Memory) is null;
            }
            catch (Exception ex) when (FileBounds.DescribesContent(ex))
            {
                return false;
            }
            finally
            {
                _exception.Set(null, 0);
            }
        }
    }

    /// <summary>
    /// Counts a row whose exception column delivery cannot read, remembering where the group's
    /// first one is for the Warning <see cref="RecordHints"/> writes.
    /// </summary>
    private void NoteMalformedException(uint offset, int bytes)
    {
        if (_malformedExceptions++ == 0)
        {
            _firstMalformedOrdinal = offset;
            _firstMalformedBytes   = bytes;
        }
        _hints?.NoteMalformedException();
    }

    /// <summary>
    /// Files the row under the exception marker: the bucket <c>@x.exists</c>, the one value
    /// <c>"true"</c>. The bloom is given the bucket's NAME and never that value — which makes this
    /// the one term filed from the header whose presence does not mean the bloom holds its text.
    ///
    /// <para>The property walk reads "already in the inverted index" as "already in the bloom"
    /// (see the note on first sight above), and a client can reach this term: a property
    /// literally named <c>@x.exists</c> shares the bucket, and the string <c>"true"</c> in it IS
    /// this term. Filed after the marker it read as a repeat and its bloom add was skipped. If
    /// nothing else in the group had given the bloom that text — a boolean <c>true</c> under any
    /// property does — the bloom gate then dropped the group for a filter on the value:
    /// <c>['@x.exists'][0] = 'true'</c>, the property as an array, which is the shape a filter
    /// can reach, returned its row from the hot tier and nothing once the tier was flushed.
    /// Filed BEFORE any exception it was new, and added; that is the whole difference.</para>
    ///
    /// <para>So a term the marker made is remembered as owing the bloom its text
    /// (<see cref="_markerOwesBloom"/>), and the walk pays that the first time a client files
    /// the value (the string case of <see cref="AddScalar"/>) — once for the group. A term the
    /// client made first owes nothing: the walk added the text, and the bucket's name with it,
    /// as the property's key.</para>
    /// </summary>
    private void AddExists(uint offset)
    {
        int prop = _inverted.PropertyId(MarkerBucket, out _);
        if (_inverted.AddValue(offset, prop, MarkerValue) == IndexAddOutcome.NewValue)
        {
            _bloom.AddUtf8(MarkerBucket);   // folded, as the reference build's string add folds it
            _markerOwesBloom = prop;
        }
        _bloomPresented++;
    }

    /// <summary>The exception marker's bucket (<see cref="ClefFields.ExceptionExists"/>) and the
    /// one value it files there — spelled once, for the marker and for the walk that meets it.</summary>
    private static ReadOnlySpan<byte> MarkerBucket => "@x.exists"u8;
    private static ReadOnlySpan<byte> MarkerValue  => "true"u8;

    /// <summary>
    /// The property id of the marker's bucket while its <c>"true"</c> is of the marker's own
    /// making and the bloom has not been given that text; -1 otherwise — no exception indexed
    /// yet, the value filed by a client first, or the debt already paid. See <see cref="AddExists"/>.
    /// </summary>
    private int _markerOwesBloom = -1;

    /// <summary>Trigram of a UTF-8 value: byte-wise fold when ASCII, the table off the UTF-8 otherwise.</summary>
    private void TrigramUtf8(uint offset, ReadOnlySpan<byte> utf8)
    {
        if (utf8.Length < 3) return;
        EnsureFold(utf8.Length);
        if (System.Text.Ascii.ToUpper(utf8, _fold, out int n) == OperationStatus.Done)
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
                // A payload the reader cannot read either — truncated, or holding a code msgpack
                // never uses. Not a key that is not a string: that is valid msgpack, and WalkMap
                // skips its pair rather than ending here.
                try { WalkMap(ref reader, 0, offset, 0); }
                catch { /* malformed payload — index what we could, mirror old try/catch tolerance */ }
            }
            finally
            {
                _payload.Set(null, 0);
            }
        }
    }

    // WHAT THE WALK FILES IS WHAT THE SCAN READS. A filter is matched against the event as
    // LogEventSerializer reads it — in the hot tier, and in a flushed group once the index has let
    // the group through — and both gates in front of that read "not filed" as "not there". So a
    // value has to be filed under the text the scan reads, or the row is one the hot tier returns
    // and the flushed segment does not. Where the reader does not read a value as itself, the walk
    // follows the reader: a pair whose key is not a string is skipped (WalkMap), a nil or extension
    // ELEMENT of an array is the string "<null>" and a binary value the text "System.Byte[]"
    // (AddScalar). The reference build reads the properties through that same reader, so the
    // parity tests hold the walk to it.
    //
    // Not every reading is followed. A map or an array is not filed as a WHOLE: the scan's text
    // predicates read one as its .NET type name ("System.Object[]", "System.Collections.Generic.
    // Dictionary`2[…]"), which no client is shown and no filter should match — there the scan is
    // the side that is wrong, and `contains(Payload, 'Dictionary')` still finds a row hot that a
    // flushed group drops. That is left to the scan.

    private void WalkMap(ref MessagePackReader reader, int prefixLen, uint offset, int depth)
    {
        if (depth > _maxFlattenDepth) { reader.Skip(); return; }
        int count = reader.ReadMapHeader();
        for (int e = 0; e < count; e++)
        {
            // A key that is not a string — msgpack allows any; the project's own Serilog sink
            // writes a Dictionary<int, …> property's keys as integers — names nothing a filter can
            // ask for: the scan's probe skips the pair and reads on, and so does its dictionary.
            // So does the walk. Read as a string, the key threw, the catch-all in
            // IndexPropertiesStreaming swallowed it, and every property after it on the row went
            // unindexed: `R = 'needle'` returned the row hot and nothing once flushed.
            if (reader.NextMessagePackType != MessagePackType.String)
            {
                reader.Skip();   // the key
                reader.Skip();   // its value
                continue;
            }
            ReadOnlySpan<byte> keyUtf8 = ReadStr(ref reader);
            EnsureKey(prefixLen + keyUtf8.Length + 1);
            keyUtf8.CopyTo(_key.AsSpan(prefixLen));
            WalkValue(ref reader, prefixLen + keyUtf8.Length, offset, depth, element: false);
        }
    }

    /// <param name="element">
    /// The value is an element of an array, at any depth of arrays — which is where the reader
    /// reads a nil or an extension as the string <c>"&lt;null&gt;"</c> (<see cref="AddScalar"/>).
    /// A map resets it: its values are map values again, wherever the map sits.
    /// </param>
    private void WalkValue(ref MessagePackReader reader, int flatLen, uint offset, int depth, bool element)
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
                for (int i = 0; i < n; i++) WalkValue(ref reader, flatLen, offset, depth, element: true);
                break;

            default:
                AddScalar(ref reader, flatLen, offset, element);
                break;
        }
    }

    /// <summary>
    /// Files one scalar under the flat key in <see cref="_key"/>, as the scan reads it.
    ///
    /// <para>Three of them the reader does not read as themselves, and the walk follows it. A nil
    /// or an extension (a msgpack timestamp, say) is null to <c>LogEventSerializer.ReadDynamic</c>;
    /// as a map value it stays null, and is filed as a nil — but as an ELEMENT of an array
    /// <c>ReadArray</c> turns it into the string <c>"&lt;null&gt;"</c>, which the scan compares,
    /// the events API shows and the UI's "filter by this value" builds <c>Tags[1] = '&lt;null&gt;'</c>
    /// from, so it is filed as that string. A binary value is a <c>byte[]</c>, which the scan's text
    /// predicates (<c>contains</c>, <c>like</c>, <c>startsWith</c>, <c>in</c> and the text
    /// functions) read as its <c>ToString()</c>, <c>"System.Byte[]"</c> — the text clients are shown
    /// for it too — so it is filed as that text. The walk used to file a nil element as a nil and skip the other two,
    /// and those filters found the row hot and nothing once flushed.</para>
    ///
    /// <para>The index follows the reader rather than the reader the index because the reader's
    /// text is what every client sees and every saved filter means, while the index is read only to
    /// decide which rows the scan reads. Nothing is lost by it either: the nil an element was filed
    /// as is a term no query asks for — <c>X = null</c> is never hinted, since a missing property
    /// matches it too.</para>
    /// </summary>
    private void AddScalar(ref MessagePackReader reader, int flatLen, uint offset, bool element)
    {
        var flatKey = _key.AsSpan(0, flatLen);

        switch (reader.NextMessagePackType)
        {
            case MessagePackType.String:
                AddString(offset, flatKey, ReadStr(ref reader));
                break;
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
                if (r == IndexAddOutcome.NewValue || HasTaggedSpelling(prop)) _bloom.Add(b ? "TRUE"u8 : "FALSE"u8);   // "True"/"False" folded
                _trigram.AddFoldedAscii(offset, b ? "TRUE"u8 : "FALSE"u8);
                break;
            }
            case MessagePackType.Nil:
                reader.ReadNil();
                if (element) AddString(offset, flatKey, NullElementText);
                else         AddNil(offset, flatKey);
                break;

            case MessagePackType.Binary:
                reader.Skip();
                AddString(offset, flatKey, BinaryText);
                break;

            default:
                // An extension. (The one code that is neither, 0xC1, is never valid msgpack: the
                // skip throws on it, as the reader's does, and the row's walk ends there.)
                reader.Skip();
                if (element) AddString(offset, flatKey, NullElementText);
                else         AddNil(offset, flatKey);
                break;
        }
    }

    /// <summary>
    /// The string <c>LogEventSerializer.ReadArray</c> reads a nil or extension ELEMENT of an array
    /// as — and the text <c>MsgPackJsonTranscoder</c> shows clients for one.
    /// </summary>
    private static ReadOnlySpan<byte> NullElementText => "<null>"u8;

    /// <summary>
    /// What the scan's text predicates read a binary value as — <c>byte[].ToString()</c> — and the
    /// text <c>MsgPackJsonTranscoder</c> shows clients for one.
    /// </summary>
    private static ReadOnlySpan<byte> BinaryText => "System.Byte[]"u8;

    /// <summary>
    /// Files one string: a client's, or the text the reader reads a value as
    /// (<see cref="NullElementText"/>, <see cref="BinaryText"/>) — which IS the term a client string
    /// of that text files, with the same bloom text, so the two can meet in either order.
    /// </summary>
    private void AddString(uint offset, ReadOnlySpan<byte> flatKey, ReadOnlySpan<byte> v)
    {
        int prop = _inverted.PropertyId(flatKey, out bool newKey);
        // Marked BEFORE it is filed, as AddHeaderString does: a filed string whose property
        // is not marked is the one state the rule must never leave behind, and the mark can
        // throw (it may grow the bitset).
        bool tagged = MaySpellATypedTerm(v);
        if (tagged) NoteTaggedSpelling(prop);
        var r    = _inverted.AddValue(offset, prop, v);          // serialised == plain for strings
        _bloomPresented += 2;
        if (newKey) _bloom.AddUtf8(flatKey);
        // New to the inverted index — or a repeat the bloom may not have been given: a
        // string that may spell a typed value's term (see MaySpellATypedTerm), or the
        // exception marker's own term met by a client's value for the first time (see
        // AddExists; that debt is struck off once the text is in the filter).
        bool owed = r == IndexAddOutcome.Existing && prop == _markerOwesBloom && v.SequenceEqual(MarkerValue);
        FoldValue(offset, v, bloom: owed || tagged || r == IndexAddOutcome.NewValue, trigram: v.Length >= 3);
        if (owed) _markerOwesBloom = -1;
    }

    /// <summary>Files a nil map value — or an extension one, which the reader reads as a nil.</summary>
    private void AddNil(uint offset, ReadOnlySpan<byte> flatKey)
    {
        int prop = _inverted.PropertyId(flatKey, out bool newKey);
        var r    = _inverted.AddValue(offset, prop, "\0null"u8);
        _bloomPresented += 2;
        if (newKey) _bloom.AddUtf8(flatKey);
        if (r == IndexAddOutcome.NewValue || HasTaggedSpelling(prop)) _bloom.Add(ReadOnlySpan<byte>.Empty);   // v?.ToString() ?? "" → ""
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
        FoldValue(offset, plain, bloom: r == IndexAddOutcome.NewValue || HasTaggedSpelling(prop), trigram: true);
    }

    // ── Strings that may spell a typed term ────────────────────────────────────

    /// <summary>
    /// Whether a string's bytes may spell a typed value's TERM — the type-tagged form the inverted
    /// index files a typed value under (<see cref="IndexValueForms.Serialise"/>: <c>\0l5</c>,
    /// <c>\0d2.5</c>, <c>\0true</c>, <c>\0null</c>) while it files a string as its own bytes.
    ///
    /// <para>Every tag begins with NUL, and a value filed without one is filed under its own text
    /// (a number too large for a long), so no other string can: a term that a string not beginning
    /// with NUL shares was filed under the same bytes with the same text — by a string, a header
    /// string or such a number — or by the exception marker, which keeps its own account
    /// (<see cref="AddExists"/>). A string that does begin with NUL may be a typed value's term
    /// with the other text, and is handled on both sides. It always gives the bloom its own text,
    /// and it marks its property (<see cref="NoteTaggedSpelling"/>) so that a typed value under it
    /// always gives its own. <c>BloomFirstSightTests</c> pins the forms this rests on — the walk's
    /// own, which production files, and <see cref="IndexValueForms.Serialise"/>'s, which the
    /// reference build files.</para>
    ///
    /// <para>What it costs: one byte read per string. A property that never holds such a string —
    /// every property of every ordinary client — pays nothing more; one that does gives up the
    /// first-sight shortcut for its typed values and those strings, which is what every value
    /// cost before the shortcut existed.</para>
    /// </summary>
    private static bool MaySpellATypedTerm(ReadOnlySpan<byte> value) => !value.IsEmpty && value[0] == 0;

    /// <summary>
    /// One bit per property id, set once a string that may spell a typed term
    /// (<see cref="MaySpellATypedTerm"/>) has been filed under that property; null until the
    /// first — which most groups never meet, so they allocate nothing for it.
    /// </summary>
    private ulong[]? _taggedSpellings;

    private void NoteTaggedSpelling(int prop)
    {
        int word = prop >> 6;
        if (_taggedSpellings is null || word >= _taggedSpellings.Length)
            Array.Resize(ref _taggedSpellings, Math.Max(word + 1, (_taggedSpellings?.Length ?? 0) * 2));
        _taggedSpellings[word] |= 1UL << (prop & 63);
    }

    /// <summary>Whether a string that may spell a typed term has been filed under <paramref name="prop"/>.</summary>
    private bool HasTaggedSpelling(int prop) =>
        _taggedSpellings is { } bits && prop >> 6 < bits.Length && (bits[prop >> 6] & (1UL << (prop & 63))) != 0;

    /// <summary>
    /// Case-folds a value once and feeds the bloom (if asked) and the trigram from the same
    /// result, folded by <see cref="CaseFold"/> — the fold whose equal is the scan's (see there).
    /// ASCII — every number, id and the vast majority of strings — is upper-cased byte-wise by
    /// <c>Ascii.ToUpper</c>, which IS that fold over ASCII, at the cost <c>Ascii.ToLower</c> had
    /// here before; the bloom hashes and trigram keys are the ones the char path produces.
    /// Anything else is folded through the table straight off its UTF-8, one char per scalar, and
    /// that one result is hashed for the bloom and cut into trigrams.
    /// </summary>
    private void FoldValue(uint offset, ReadOnlySpan<byte> v, bool bloom, bool trigram)
    {
        if (!bloom && !trigram) return;
        EnsureFold(v.Length);
        if (System.Text.Ascii.ToUpper(v, _fold, out int n) == OperationStatus.Done)
        {
            var folded = _fold.AsSpan(0, n);
            if (bloom)   _bloom.Add(folded);
            if (trigram) _trigram.AddFoldedAscii(offset, folded);
            return;
        }

        // Folded text is never longer than its UTF-8 counted in bytes.
        if (v.Length > _wide.Length) _wide = new char[Math.Max(v.Length, _wide.Length * 2)];
        int chars = CaseFold.FoldUtf8(v, _wide);
        var text  = _wide.AsSpan(0, chars);
        if (bloom)   _bloom.AddCanonical(text);
        if (trigram) _trigram.AddFolded(offset, text);
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
    /// Exception payloads this group could not read as an exception map. Each is a row written
    /// without its exception type, inner type and message terms. One whose payload opens as a
    /// map or a non-empty string is still filed under the <c>@x.exists</c> marker, because a
    /// scan counts it as having an exception (<see cref="ExceptionInfo.IsPresent"/>); one cut
    /// off before that is counted here and marked nowhere, because a scan does not count it
    /// either. The process-wide total is on <see cref="IndexBuildHints.MalformedExceptionPayloads"/>.
    ///
    /// <para>ENCOUNTERS, not distinct rows. The merged segment keeps the raw payload, so the same
    /// row is met and counted again at every later merge level, and a merge that fails after this
    /// group was added counts its rows again on the retry. The figure therefore grows with
    /// compaction depth rather than with ingest, and a fully compacted store reports 0 after a
    /// restart while <c>@x.type</c> queries still miss those rows. Read it as "how much unreadable
    /// exception data the merges are meeting", never as a count of affected rows.</para>
    /// </summary>
    public long MalformedExceptionPayloads => _malformedExceptions;

    private long _malformedExceptions;
    private readonly Microsoft.Extensions.Logging.ILogger? _log;

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

    /// <summary>What this group measured, for the next one to size itself by — once, however
    /// many of the section accessors are called (a probe calls both paths). Also where a group
    /// that met unreadable exception maps is reported: once per group, at Warning, with its
    /// count, the first one's file ordinal and the process-wide total.</summary>
    private void RecordHints()
    {
        if (_hintsRecorded) return;
        _hintsRecorded = true;
        _hints?.Record(_inverted.TermCount, _trigram.BucketCount);
        if (_malformedExceptions > 0 && _log is { } log && log.IsEnabled(Microsoft.Extensions.Logging.LogLevel.Warning))
            LogMalformedExceptions(log, _malformedExceptions, _firstMalformedOrdinal, _firstMalformedBytes,
                                   _hints?.MalformedExceptionPayloads ?? _malformedExceptions, null);
    }

    private bool _hintsRecorded;
    private uint _firstMalformedOrdinal;
    private int  _firstMalformedBytes;

    /// <summary>Pre-compiled so the report formats without boxing its arguments.</summary>
    private static readonly Action<Microsoft.Extensions.Logging.ILogger, long, uint, int, long, Exception?> LogMalformedExceptions =
        Microsoft.Extensions.Logging.LoggerMessage.Define<long, uint, int, long>(
            Microsoft.Extensions.Logging.LogLevel.Warning,
            new Microsoft.Extensions.Logging.EventId(0, "IndexMalformedExceptionPayloads"),
            "Index build: {Count} exception payload(s) in this group are not readable exception maps (first at file ordinal {Ordinal}, {Bytes} B); the rows are written but their exception type and message are not indexed, so @x.type filters and free-text search over the merged segment miss them. Process total: {Total}");

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
