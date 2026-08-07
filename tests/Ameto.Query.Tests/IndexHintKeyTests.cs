using System.Buffers;
using Ameto.Core;
using Ameto.Indexing;
using Ameto.Query.Filtering;
using Ameto.Storage;
using MessagePack;

namespace Ameto.Query.Tests;

/// <summary>
/// Covers the gap between <c>FilterEvaluatorTests</c> (which never builds an index) and
/// <c>SegmentIndexCodecFormatTests</c> (which never compiles a filter): real events, a real
/// segment index, and the exact prefilter <c>QueryExecutor</c> runs against it — bloom gate,
/// <c>MightContain</c> gate, then <c>LookupIntersect</c> candidate narrowing — before the
/// surviving rows are evaluated.
///
/// <para>Every defect in this class was invisible to both existing suites because the hot
/// tier consults no index at all: a filter matched while the events were in memory and
/// returned nothing the moment the segment flushed. So each test asserts two things — that
/// the index path returns the expected events, and that it returns exactly what a full scan
/// of the same events returns. The second assertion is the real invariant: an index may cost
/// a query its acceleration, never its rows.</para>
/// </summary>
public sealed class IndexHintKeyTests : IDisposable
{
    // ── Fixture: three events covering every indexed header field ─────────────

    private const string TimeoutType   = "System.TimeoutException";
    private const string SocketType    = "System.Net.Sockets.SocketException";
    private const string InvalidOpType = "System.InvalidOperationException";
    private const string ShippedTpl    = "Order {OrderId} shipped to {Region}";
    private const string ExMessage     = "The operation has timed out after 30s";
    private const string ExStack       = "   at Wallet.Api.Ship(Order o) in Ship.cs:line 42";
    private const string InnerMessage  = "Connection reset by peer";

    private const ulong  TraceHi = 0x0123456789ABCDEFUL;
    private const ulong  TraceLo = 0xFEDCBA9876543210UL;
    private const ulong  Span    = 0x00FF00FF00FF00FFUL;

    private static readonly string TraceHex = TraceIdHelper.FormatTraceId(TraceHi, TraceLo)!;
    private static readonly string SpanHex  = TraceIdHelper.FormatSpanId(Span)!;

    private readonly HotTierSegment   _hot  = new(16, 64 * 1024);
    private readonly StringInternPool _pool = new();
    private readonly List<LogEvent>   _events;
    private readonly byte[]           _invertedBytes;
    private readonly byte[]           _trigramBytes;
    private readonly byte[]           _bloomBytes;

    public IndexHintKeyTests()
    {
        var boom = new ExceptionInfo
        {
            Type       = TimeoutType,
            Message    = ExMessage,
            StackTrace = ExStack,
            Inner      = new ExceptionInfo { Type = SocketType, Message = InnerMessage },
        };

        Write(id: 9001, LogLevel.Error, ShippedTpl, "Wallet.API", boom,
              trace: true, props: Props(("Region", "ae-dxb"), ("Tags", new[] { "urgent", "ops" }),
                                        ("Score", 12345L), ("Ratio", 2.5d), ("Enabled", true)));

        Write(id: 9002, LogLevel.Information, "User {User} signed in", "Auth.API", exception: null,
              trace: false, props: Props(("Region", "ae-auh")));

        Write(id: 9003, LogLevel.Warning, "Retrying {Attempt}", "Wallet.API",
              new ExceptionInfo { Type = InvalidOpType },
              trace: false, props: Props(("Region", "ae-shj")));

        _hot.Freeze();
        _events = [.. _hot.ReadAll(_pool)];

        var builder = new SegmentIndexBuilder(_hot.Count);
        builder.Build(_hot, _pool);
        _invertedBytes = builder.SerialisedInvertedIndex;
        _trigramBytes  = builder.SerialisedTrigramIndex;
        _bloomBytes    = builder.SerialisedBloomFilter;
    }

    public void Dispose() => _hot.Dispose();

    // ── The seed defect ───────────────────────────────────────────────────────

    [Fact]
    public void ExceptionTypeEquality_ReturnsTheEvent()
    {
        // `@x = 'System.TimeoutException'` used to return the event while it sat in the hot
        // tier and zero rows once the segment flushed: the hint went out under the key "@x"
        // and the index files the type under "@x.type".
        AssertIndexPath($"@x = '{TimeoutType}'", 9001);
    }

    [Fact]
    public void ExceptionTypeIn_ReturnsTheEvent() =>
        AssertIndexPath($"@x in ['{TimeoutType}']", 9001);

    // ── Alias axis: a spelling the index has never heard of ───────────────────

    [Fact] public void ExceptionAlias()            => AssertIndexPath($"Exception = '{TimeoutType}'", 9001);
    [Fact] public void ExceptionTypeDotted()       => AssertIndexPath($"@x.type = '{TimeoutType}'", 9001);
    [Fact] public void ExceptionTypePascalDotted() => AssertIndexPath($"Exception.Type = '{TimeoutType}'", 9001);
    [Fact] public void ExceptionTypeBracketed()    => AssertIndexPath($"['@x.type'] = '{TimeoutType}'", 9001);
    [Fact] public void InnerExceptionTypeDotted()  => AssertIndexPath($"@x.inner.type = '{SocketType}'", 9001);
    [Fact] public void InnerExceptionTypePascal()  => AssertIndexPath($"Exception.Inner.Type = '{SocketType}'", 9001);
    [Fact] public void LevelAlias()                => AssertIndexPath("Level = 'Error'", 9001);
    [Fact] public void TraceIdAlias()              => AssertIndexPath($"TraceId = '{TraceHex}'", 9001);
    [Fact] public void SpanIdAlias()               => AssertIndexPath($"SpanId = '{SpanHex}'", 9001);
    [Fact] public void ServiceNameDotted()         => AssertIndexPath("service.name = 'Auth.API'", 9002);

    // ── Coverage axis: fields no inverted bucket holds ────────────────────────
    // The hint must be suppressed entirely. A hint here is unanswerable, and the index
    // answered "no matching events", which dropped the segment.

    [Fact] public void MessageTemplateAt()      => AssertIndexPath($"@mt = '{ShippedTpl}'", 9001);
    [Fact] public void MessageTemplatePascal()  => AssertIndexPath($"MessageTemplate = '{ShippedTpl}'", 9001);
    [Fact] public void MessageAt()              => AssertIndexPath($"@m = '{ShippedTpl}'", 9001);
    [Fact] public void MessagePascal()          => AssertIndexPath($"Message = '{ShippedTpl}'", 9001);
    [Fact] public void ExceptionMessageDotted() => AssertIndexPath($"@x.message = '{ExMessage}'", 9001);
    [Fact] public void ExceptionMessagePascal() => AssertIndexPath($"Exception.Message = '{ExMessage}'", 9001);
    [Fact] public void ExceptionStackDotted()   => AssertIndexPath($"@x.stack = '{ExStack}'", 9001);
    [Fact] public void ExceptionStackPascal()   => AssertIndexPath($"Exception.StackTrace = '{ExStack}'", 9001);
    [Fact] public void InnerMessageDotted()     => AssertIndexPath($"@x.inner.message = '{InnerMessage}'", 9001);
    [Fact] public void InnerMessagePascal()     => AssertIndexPath($"Exception.Inner.Message = '{InnerMessage}'", 9001);

    [Fact]
    public void TimestampAt()
    {
        string ts = _events[0].Timestamp.ToString("O");
        AssertIndexPath($"@t = '{ts}'", 9001);
    }

    [Fact]
    public void TimestampPascal()
    {
        string ts = _events[0].Timestamp.ToString("O");
        AssertIndexPath($"Timestamp = '{ts}'", 9001);
    }

    [Fact] public void EventIdAt()     => AssertIndexPath("@id = 9001", 9001);
    [Fact] public void EventIdPascal() => AssertIndexPath("Id = 9001", 9001);

    // ── Subscript axis: array elements live under the bare parent key ─────────

    [Fact] public void ArraySubscript()      => AssertIndexPath("Tags[0] = 'urgent'", 9001);
    [Fact] public void ArraySubscriptSecond() => AssertIndexPath("Tags[1] = 'ops'", 9001);

    // ── Value axis: the key is right and the casing is not ────────────────────
    // The scan compares strings case-insensitively, so a value bucket the index refuses to
    // match on casing alone prunes rows the scan would have returned.

    [Fact] public void LevelValueLowercase() => AssertIndexPath("@l = 'error'", 9001);
    [Fact] public void LevelAliasLowercase() => AssertIndexPath("Level = 'ERROR'", 9001);

    [Fact]
    public void TraceIdUppercaseHex() =>
        AssertIndexPath($"@tr = '{TraceHex.ToUpperInvariant()}'", 9001);

    [Fact]
    public void ExceptionTypeDifferentCase() =>
        AssertIndexPath($"@x = '{TimeoutType.ToUpperInvariant()}'", 9001);

    [Fact]
    public void UserPropertyValueDifferentCase() => AssertIndexPath("Region = 'AE-DXB'", 9001);

    // ── Spellings that already worked must keep working ───────────────────────

    [Fact] public void CanonicalLevel()   => AssertIndexPath("@l = 'Error'", 9001);
    [Fact] public void CanonicalTraceId() => AssertIndexPath($"@tr = '{TraceHex}'", 9001);
    [Fact] public void CanonicalSpanId()  => AssertIndexPath($"@sp = '{SpanHex}'", 9001);
    [Fact] public void UserProperty()     => AssertIndexPath("Region = 'ae-dxb'", 9001);
    [Fact] public void BareLevelKeyword() => AssertIndexPath("Error", 9001);
    [Fact] public void ServiceNameBracketed() => AssertIndexPath("['service.name'] = 'Auth.API'", 9002);

    // ── Reversed operands: the only entry that returned too MANY rows ─────────

    [Fact]
    public void LiteralOnTheLeft_FiltersOnTheRightHandProperty()
    {
        // Used to match every event in the hot tier (null compared to null) and none in
        // cold — the same predicate failing in opposite directions in the two tiers.
        AssertIndexPath($"'{TimeoutType}' = @x", 9001);
    }

    [Fact]
    public void LiteralOnTheLeft_OrdersComparisonsTheRightWayRound() =>
        AssertIndexPath("'ae-dxb' = Region", 9001);

    [Fact]
    public void AndOfTwoAliases() =>
        AssertIndexPath($"Level = 'Error' and @x = '{TimeoutType}'", 9001);

    // ── Trigram coverage axis: the same defect, one index over ────────────────
    // SegmentTrigramIndex.Lookup reads a missing trigram in a POPULATED index as proof of
    // absence, so a substring/prefix hint on text the builder never trigrammed drops the
    // segment exactly as fatally as the seed defect's unanswerable bucket name did. The
    // builder trigrams the message template, the exception type and message, and property
    // values — never a hex id, a level, an event id, a service name or a stack trace.

    [Fact] public void ContainsTraceId()      => AssertIndexPath($"contains(@tr, '{TraceHex[..8]}')", 9001);
    [Fact] public void StartsWithTraceId()    => AssertIndexPath($"startsWith(@tr, '{TraceHex[..8]}')", 9001);
    [Fact] public void LikeTraceId()          => AssertIndexPath($"@tr like '{TraceHex[..8]}%'", 9001);
    [Fact] public void ContainsSpanId()       => AssertIndexPath($"contains(@sp, '{SpanHex[..6]}')", 9001);
    [Fact] public void ContainsLevel()        => AssertIndexPath("contains(@l, 'rro')", 9001);
    [Fact] public void ContainsEventId()      => AssertIndexPath("contains(@id, '900')", 9001, 9002, 9003);
    [Fact] public void ContainsStackTrace()   => AssertIndexPath("contains(@x.stack, 'Wallet.Api')", 9001);
    [Fact] public void ContainsServiceName()  => AssertIndexPath("contains(service.name, 'allet')", 9001, 9003);
    [Fact] public void LikeServiceName()      => AssertIndexPath("service.name like 'Wallet%'", 9001, 9003);
    [Fact] public void ContainsInnerType()    => AssertIndexPath("contains(@x.inner.type, 'Sockets')", 9001);

    /// <summary>A msgpack integer is not a string, and the scan stringifies it anyway.</summary>
    [Fact] public void ContainsNumericProperty() => AssertIndexPath("contains(Score, '234')", 9001);

    /// <summary>A msgpack bool, same shape.</summary>
    [Fact] public void ContainsBoolProperty() => AssertIndexPath("contains(Enabled, 'rue')", 9001);

    /// <summary>
    /// The LIKE single-character wildcard is a PATTERN character, not a character of the
    /// value: hinting the raw <c>ae_dxb</c> looks up trigrams the event never had.
    /// </summary>
    [Fact] public void LikeSingleCharWildcard() => AssertIndexPath("Region like 'ae_dxb'", 9001);

    [Fact] public void LikeSingleCharWildcardMidPattern() => AssertIndexPath("Region like '%e_dx%'", 9001);

    // ── The trigram index must still narrow, not just stop being wrong ────────

    [Fact]
    public void AbsentTrigramInCoveredText_StillSkipsTheSegment()
    {
        // The message template IS trigrammed, so its silence is proof and must keep pruning.
        var filter = CompiledFilter.Compile("contains(@mt, 'zzqqxx')");
        Assert.False(Prefilter(filter, out _), "trigrammed text the index lacks must still skip the segment");
    }

    // ── Value type-form axis: the index is typed, the scan coerces ────────────

    [Fact] public void QuotedIntegerLiteral() => AssertIndexPath("Score = '12345'", 9001);
    [Fact] public void IntegerLiteral()       => AssertIndexPath("Score = 12345", 9001);
    [Fact] public void QuotedBoolLiteral()    => AssertIndexPath("Enabled = 'true'", 9001);
    [Fact] public void BoolLiteral()          => AssertIndexPath("Enabled = true", 9001);
    [Fact] public void QuotedDoubleLiteral()  => AssertIndexPath("Ratio = '2.5'", 9001);
    [Fact] public void DoubleLiteral()        => AssertIndexPath("Ratio = 2.5", 9001);

    [Fact]
    public void WrongNumericValue_StillSkipsTheSegment()
    {
        var filter = CompiledFilter.Compile("Score = 999");
        Assert.False(Prefilter(filter, out _));
    }

    // ── The constants BuiltinFields hints with must name real buckets ─────────

    [Theory]
    [InlineData(ClefFields.Level,              "Error",      0u)]
    [InlineData(ClefFields.ExceptionType,      TimeoutType,  0u)]
    [InlineData(ClefFields.ExceptionInnerType, SocketType,   0u)]
    [InlineData(ClefFields.ServiceName,        "Auth.API",   1u)]
    public void HintKey_NamesABucketTheBuilderWrote(string indexKey, string value, uint offset)
    {
        // If a constant here ever stops matching what SegmentIndexBuilder writes, the lookup
        // returns null ("unknown property") instead of the event — which is the whole defect,
        // caught at the seam rather than through a filter expression.
        var idx = SegmentIndexReader.Load(_invertedBytes, _trigramBytes, _bloomBytes);
        Assert.Equal([offset], idx.LookupIntersect([(indexKey, (object?)value)]));
    }

    [Fact]
    public void TraceAndSpanHintKeys_NameBucketsTheBuilderWrote()
    {
        var idx = SegmentIndexReader.Load(_invertedBytes, _trigramBytes, _bloomBytes);
        Assert.Equal([0u], idx.LookupIntersect([(ClefFields.TraceId, (object?)TraceHex)]));
        Assert.Equal([0u], idx.LookupIntersect([(ClefFields.SpanId,  (object?)SpanHex)]));
    }

    // ── The index must still narrow, not just stop being wrong ────────────────

    [Fact]
    public void KnownPropertyMissingValue_StillSkipsTheSegment()
    {
        // The other half of the contract: "@x.type exists in this index and holds no such
        // value" IS proof, and must keep pruning. Only "no such property" is no information.
        var filter = CompiledFilter.Compile("@x = 'System.NoSuchException'");
        Assert.False(Prefilter(filter, out _), "a value the index knows is absent must still skip the segment");
    }

    [Fact]
    public void UserPropertyMissingValue_StillSkipsTheSegment()
    {
        var filter = CompiledFilter.Compile("Region = 'eu-fra'");
        Assert.False(Prefilter(filter, out _));
    }

    [Fact]
    public void EqualityHint_NarrowsToTheMatchingOffsets()
    {
        var filter = CompiledFilter.Compile($"@x = '{TimeoutType}'");
        Assert.True(Prefilter(filter, out uint[]? candidates));
        Assert.NotNull(candidates);
        Assert.Equal([0u], candidates!);
    }

    // ── Assertions ────────────────────────────────────────────────────────────

    /// <summary>
    /// Runs <paramref name="expression"/> through the real index path and asserts it yields
    /// <paramref name="expectedIds"/> — and that a full scan of the same events agrees, so a
    /// test can never pass because both paths are broken the same way.
    /// </summary>
    private void AssertIndexPath(string expression, params ulong[] expectedIds)
    {
        var filter = CompiledFilter.Compile(expression);

        ulong[] scanned = Scan(filter);
        Assert.Equal(expectedIds, scanned);           // guards the fixture itself

        ulong[] viaIndex = QueryThroughIndex(filter);
        Assert.Equal(expectedIds, viaIndex);
    }

    /// <summary>Full scan — what the hot tier does, and the answer the cold tier owes.</summary>
    private ulong[] Scan(CompiledFilter filter)
    {
        var hits = new List<ulong>();
        foreach (var ev in _events)
            if (filter.Matches(ev)) hits.Add(ev.Id.RawValue);
        return [.. hits];
    }

    /// <summary>Prefilter, then evaluate only the rows the prefilter left standing.</summary>
    private ulong[] QueryThroughIndex(CompiledFilter filter)
    {
        if (!Prefilter(filter, out uint[]? candidates)) return [];

        var hits = new List<ulong>();
        for (int i = 0; i < _events.Count; i++)
        {
            if (candidates is not null && Array.IndexOf(candidates, (uint)i) < 0) continue;
            if (filter.Matches(_events[i])) hits.Add(_events[i].Id.RawValue);
        }
        return [.. hits];
    }

    /// <summary>
    /// Runs <c>QueryExecutor</c>'s own prefilter over this fixture's segment: the phase-1
    /// bloom gate on the equality hint, then phase-2's definitive <c>MightContain</c>,
    /// trigram offsets and inverted narrowing. Returns false when the segment would be
    /// dropped outright.
    ///
    /// <para>These are the production methods, not a copy of them. A copy passed this suite
    /// green while <c>PrefilterSegmentsAsync</c> itself was reverted to a losing version —
    /// which is precisely the failure these tests exist to catch.</para>
    /// </summary>
    private bool Prefilter(CompiledFilter filter, out uint[]? candidates)
    {
        candidates          = null;
        var trigramHints    = filter.GetTrigramHints();
        var invertedHints   = filter.GetInvertedHints();
        bool hasIndexHint   = !filter.IsMatchAll && filter.TryGetIndexHint(out _, out _);

        // The executor's own fast path: nothing to ask the index, so nothing may be dropped.
        if (!hasIndexHint && invertedHints.Count == 0 && trigramHints.Count == 0)
            return true;

        if (hasIndexHint)
        {
            using var bloom = SegmentBloomFilter.Deserialise(_bloomBytes);
            if (!QueryExecutor.PassesBloomGate(filter, bloom)) return false;
        }

        var idx = SegmentIndexReader.Load(_invertedBytes, _trigramBytes, _bloomBytes);
        return QueryExecutor.TryNarrowWithIndex(filter, idx, out candidates);
    }

    // ── Fixture helpers ───────────────────────────────────────────────────────

    private void Write(
        ulong id, LogLevel level, string template, string serviceName,
        ExceptionInfo? exception, bool trace, byte[] props)
    {
        var header = new LogEventHeader
        {
            Id                       = id,
            TimestampUtcTicks        = new DateTimeOffset(2026, 8, 7, 10, 0, (int)(id % 60), TimeSpan.Zero).UtcTicks,
            MessageTemplatePoolIndex = _pool.Intern(template),
            ServiceNamePoolIndex     = _pool.Intern(serviceName),
            Level                    = level,
            TraceIdHi                = trace ? TraceHi : 0,
            TraceIdLo                = trace ? TraceLo : 0,
            SpanId                   = trace ? Span    : 0,
        };
        Assert.True(_hot.TryWrite(header, props, template, exception));
    }

    /// <summary>Msgpack map of the given (key, value) pairs — the payload shape ingest writes.</summary>
    private static byte[] Props(params (string Key, object Value)[] pairs)
    {
        var buf = new ArrayBufferWriter<byte>(128);
        var w   = new MessagePackWriter(buf);
        w.WriteMapHeader(pairs.Length);
        foreach (var (key, value) in pairs)
        {
            w.Write(key);
            switch (value)
            {
                case string s:   w.Write(s); break;
                case int i:      w.Write(i); break;
                case long l:     w.Write(l); break;
                case double d:   w.Write(d); break;
                case bool b:     w.Write(b); break;
                case string[] a:
                    w.WriteArrayHeader(a.Length);
                    foreach (var item in a) w.Write(item);
                    break;
                default: throw new NotSupportedException(value.GetType().Name);
            }
        }
        w.Flush();
        return buf.WrittenSpan.ToArray();
    }
}
