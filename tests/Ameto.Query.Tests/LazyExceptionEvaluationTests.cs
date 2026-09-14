using System.Buffers;
using Ameto.Core;
using Ameto.Query.Filtering;
using MessagePack;
using Xunit;
using Xunit.Abstractions;

namespace Ameto.Query.Tests;

/// <summary>
/// WHAT THE EVALUATOR IS ALLOWED TO DECODE. A cold-tier row carries its exception as msgpack
/// bytes and builds the object only when something reads <c>LogEvent.Exception</c> — but two
/// predicates read it for EVERY row a scan touches, which is where the laziness was worth the
/// most and where it was not being taken:
///
/// <list type="bullet">
///   <item><c>has @x</c> / <c>@x is not null</c> resolved to <c>ev.Exception?.Type</c>, so a
///         presence question built the whole tree, stack trace included.</item>
///   <item>a free-text term read <c>ev.Exception</c> per row per term, and on a level-split
///         Error segment that is every row.</item>
/// </list>
///
/// <para>Both answer from the bytes now. The risk in that is DISAGREEMENT — a byte-level
/// reading that says something different from the object it replaced — so the cover here is an
/// oracle: every case is evaluated twice over the same exception, once as an object (the
/// hot-tier shape) and once as raw bytes (the cold-tier shape), and the two must match.</para>
/// </summary>
public sealed class LazyExceptionEvaluationTests
{
    private readonly ITestOutputHelper _out;
    public LazyExceptionEvaluationTests(ITestOutputHelper o) => _out = o;

    private static LogEvent WithObject(ExceptionInfo? ex, string template = "order {Id} failed") => new()
    {
        Id = new EventId(0u, 1u), Timestamp = DateTimeOffset.UtcNow,
        Level = LogLevel.Error, MessageTemplate = template, Exception = ex,
    };

    private static LogEvent WithBytes(ReadOnlyMemory<byte> raw, string template = "order {Id} failed") => new()
    {
        Id = new EventId(0u, 1u), Timestamp = DateTimeOffset.UtcNow,
        Level = LogLevel.Error, MessageTemplate = template, RawException = raw,
    };

    private static bool Eval(string filter, LogEvent ev) => CompiledFilter.Compile(filter).Matches(ev);

    private static byte[] Pack(PackAction write)
    {
        var buf = new ArrayBufferWriter<byte>(128);
        var w   = new MessagePackWriter(buf);
        write(ref w);
        w.Flush();
        return buf.WrittenSpan.ToArray();
    }

    private delegate void PackAction(ref MessagePackWriter writer);

    /// <summary>Payloads covering every shape the wire format decodes, paired with the object
    /// they decode to — so a raw-bytes reading can be checked against the object reading.</summary>
    public static TheoryData<string> Payloads() =>
    [
        "map-full", "map-no-type", "map-nil-type", "map-no-msg", "map-nested",
        "legacy-string", "legacy-empty", "nil", "not-an-exception",
    ];

    private static byte[] PayloadBytes(string which) => which switch
    {
        "map-full" => new ExceptionInfo
        {
            Type = "System.InvalidOperationException", Message = "gateway timeout on settle",
            StackTrace = "   at Billing.Settle()\n   at Billing.Run()",
        }.ToBytes(),

        "map-nested" => new ExceptionInfo
        {
            Type = "System.AggregateException", Message = "one or more errors",
            StackTrace = new string('f', 900),
            Inner = new ExceptionInfo { Type = "System.FormatException", Message = "inner detail" },
        }.ToBytes(),

        // No `type` key at all → Type defaults to "Exception".
        "map-no-type" => Pack((ref MessagePackWriter w) =>
        {
            w.WriteMapHeader(2);
            w.Write("msg"); w.Write("gateway timeout on settle");
            w.Write("stk"); w.Write("   at Billing.Settle()");
        }),

        // `type` present but nil → Type ALSO defaults to "Exception".
        "map-nil-type" => Pack((ref MessagePackWriter w) =>
        {
            w.WriteMapHeader(2);
            w.Write("type"); w.WriteNil();
            w.Write("msg");  w.Write("gateway timeout on settle");
        }),

        "map-no-msg" => Pack((ref MessagePackWriter w) =>
        {
            w.WriteMapHeader(1);
            w.Write("type"); w.Write("System.TimeoutException");
        }),

        "legacy-string"    => Pack((ref MessagePackWriter w) => w.Write("gateway timeout on settle")),
        "legacy-empty"     => Pack((ref MessagePackWriter w) => w.Write("")),
        "nil"              => Pack((ref MessagePackWriter w) => w.WriteNil()),
        "not-an-exception" => Pack((ref MessagePackWriter w) => w.Write(42)),

        _ => throw new ArgumentOutOfRangeException(nameof(which), which, null),
    };

    /// <summary>
    /// THE ORACLE. Same exception, read as bytes and as an object; every filter that touches
    /// <c>@x</c> must agree. A disagreement here is a query returning different rows for a
    /// cold-tier event than for the identical hot-tier one.
    /// </summary>
    [Theory]
    [MemberData(nameof(Payloads))]
    public void RawBytesAndObjectAgreeOnEveryExceptionPredicate(string which)
    {
        var bytes = PayloadBytes(which);
        var asObj = ExceptionInfo.FromBytes(bytes.AsSpan());

        var objEv = WithObject(asObj);
        var rawEv = WithBytes(bytes);

        // Presence, as LogEvent reports it and as the old reading computed it.
        Assert.Equal(asObj is not null, rawEv.HasException);
        Assert.Equal(objEv.HasException, rawEv.HasException);

        foreach (var filter in new[]
                 {
                     "has @x", "@x is not null", "@x is null",
                     "timeout", "gateway", "settle", "exception", "EXCEPTION",
                     "invalidoperation", "System.TimeoutException", "formatexception",
                     "nosuchterm", "order",                       // the last one hits the template
                     "@x.type = 'System.TimeoutException'",
                     "@x.type like '%Timeout%'",
                     "@x.message like '%gateway%'",
                     "@x.stack like '%Billing%'",
                     "@x.inner.type = 'System.FormatException'",
                 })
        {
            // Fresh events each time: evaluating one filter may materialise the raw side, and
            // the question is what a filter answers against an UNTOUCHED row.
            bool onObject = Eval(filter, WithObject(asObj));
            bool onRaw    = Eval(filter, WithBytes(bytes));
            Assert.True(onObject == onRaw,
                $"{which} / {filter}: object said {onObject}, raw bytes said {onRaw}");
        }
    }

    /// <summary>
    /// …and the point of all that: neither predicate decodes. This is what fails on a revert —
    /// the answers above are identical either way, the cost is not.
    /// </summary>
    [Theory]
    [InlineData("has @x")]
    [InlineData("@x is not null")]
    [InlineData("@x is null")]
    [InlineData("timeout")]                 // free text, matches the exception message
    [InlineData("gateway settle")]          // two terms, both in the message
    [InlineData("nosuchterm")]              // free text, matches nothing
    [InlineData("invalidoperation")]        // free text, matches the exception TYPE
    public void NeitherPresenceNorFreeTextDecodesTheException(string filter)
    {
        var bytes = PayloadBytes("map-full");
        var ev    = WithBytes(bytes);

        _ = Eval(filter, ev);

        Assert.True(ev.HasException);
        Assert.False(ev.ExceptionMaterialised,
            $"`{filter}` built the ExceptionInfo tree — the stack trace with it");
    }

    /// <summary>A predicate that genuinely needs a FIELD still decodes, once.</summary>
    [Fact]
    public void AFieldPredicateStillDecodesExactlyOnce()
    {
        var ev = WithBytes(PayloadBytes("map-full"));

        Assert.True(Eval("@x.stack like '%Billing%'", ev));
        Assert.True(ev.ExceptionMaterialised);
        Assert.Same(ev.Exception, ev.Exception);
    }

    /// <summary>
    /// The free-text saving, in bytes. A term tested against a row with a fat stack trace must
    /// cost the two fields it reads, not the tree it used to build.
    /// </summary>
    [Fact]
    public void FreeTextOverAFatStackTraceAllocatesNothing()
    {
        var fat = new ExceptionInfo
        {
            Type       = "System.InvalidOperationException",
            Message    = "gateway timeout on settle",
            StackTrace = string.Join('\n', Enumerable.Repeat(
                "   at Billing.Api.Orders.SettleAsync(OrderId id) in /src/Orders.cs:line 118", 40)),
            Inner = new ExceptionInfo { Type = "System.FormatException", Message = "bad amount" },
        }.ToBytes();

        var filter = CompiledFilter.Compile("nosuchterm");
        for (int i = 0; i < 50; i++) filter.Matches(WithBytes(fat));    // warm

        const int Iterations = 500;
        long b0 = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < Iterations; i++) filter.Matches(WithBytes(fat));
        long perRow = (GC.GetAllocatedBytesForCurrentThread() - b0) / Iterations;

        _out.WriteLine($"{fat.Length} B exception payload, one non-matching term: {perRow} B per row");

        // The LogEvent itself is the floor (~120 B). The tree this used to build is several
        // times the payload, so anything near it means the decode is back.
        Assert.True(perRow < fat.Length / 4,
            $"{perRow} B per row against a {fat.Length} B payload — the exception is being decoded");
    }
}
