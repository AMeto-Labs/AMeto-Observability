using System.Buffers;
using System.Reflection;
using MessagePack;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Ameto.Core;
using Ameto.Indexing;
using Ameto.Query.Filtering;
using Ameto.Storage;

namespace Ameto.Query.Tests;

/// <summary>
/// <c>@tr = 'hex32'</c> and <c>@sp = 'hex16'</c> compile to a <see cref="TraceIdCompareNode"/>
/// (Q1), and the evaluator finds a node through a <see cref="NodeKind"/> jump table (Q3) whose
/// untagged fallback is a hand-kept type switch. The two landed on separate branches. If the
/// node reached neither the table nor the switch, every trace-id filter would stop matching:
/// the trace-logs endpoint (<c>/api/traces/{id}/logs</c>) and the span-logs endpoint issue
/// nothing else, and on cold segments no header pre-check stands in front of the evaluator to
/// hide it.
///
/// <para>So these tests pin the ANSWER, not the arm. Each id filter is checked against the
/// string compare it replaced, which is the oracle: the same literal as a plain
/// <see cref="CompareNode"/> renders the event's id and compares text. That holds on a hot-shaped
/// event (no raw bytes), a cold-shaped one (raw msgpack properties, as the segment decoder hands
/// it over), and end to end through <see cref="QueryExecutor"/> over a flushed segment.</para>
/// </summary>
public sealed class TraceIdFilterDispatchTests
{
    private const ulong TraceHi   = 0x0123456789abcdefUL;
    private const ulong TraceLo   = 0xfedcba9876543210UL;
    private const ulong Span      = 0x00ff00ff00ff00ffUL;
    private const ulong OtherLo   = 0x1111111111111111UL;
    private const ulong OtherSpan = 0x2222222222222222UL;

    private static readonly string TraceHex = TraceIdHelper.FormatTraceId(TraceHi, TraceLo)!;
    private static readonly string SpanHex  = TraceIdHelper.FormatSpanId(Span)!;

    public enum Shape { Hot, Cold }

    public enum Ids { Same, Different, None }

    public static TheoryData<string, string, Shape, Ids> Cases()
    {
        var data = new TheoryData<string, string, Shape, Ids>();
        foreach (var field in new[] { "@tr", "@sp" })
        foreach (var op in new[] { "=", "!=" })
        foreach (var shape in new[] { Shape.Hot, Shape.Cold })
        foreach (var ids in new[] { Ids.Same, Ids.Different, Ids.None })
            data.Add(field, op, shape, ids);
        return data;
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public void An_id_filter_answers_like_the_string_compare_it_replaced(string field, string op, Shape shape, Ids ids)
    {
        var    ev       = Event(shape, ids, properties: shape == Shape.Cold);
        string literal  = field == "@tr" ? TraceHex : SpanHex;
        bool   expected = ids == Ids.Same ? op == "=" : op == "!=";

        var filter = CompiledFilter.Compile($"{field} {op} '{literal}'");
        var root   = RootOf(filter);

        // The rewrite has to have happened, or this would only be testing the string road again.
        var tid = Assert.IsType<TraceIdCompareNode>(root);
        Assert.Equal(NodeKind.TraceIdCompare, tid.DispatchKind);

        // The oracle: the literal as the CompareNode the compiler replaced.
        var cmp = new CompareNode(field, op == "=" ? CompareOp.Eq : CompareOp.Ne, literal);
        Assert.Equal(expected, FilterEvaluator.Matches(cmp, ev));

        // The jump table.
        Assert.Equal(expected, filter.Matches(ev));
        Assert.Equal(expected, FilterEvaluator.Matches(tid, ev));

        // Inside the compound shapes a real query wraps it in.
        Assert.Equal(expected,  CompiledFilter.Compile($"@l = 'Information' and {field} {op} '{literal}'").Matches(ev));
        Assert.Equal(!expected, CompiledFilter.Compile($"not ({field} {op} '{literal}')").Matches(ev));

        // The type switch behind the table: the arm an instance without its tag reaches.
        var untagged = new TraceIdCompareNode(tid.Op, tid.IsSpan, tid.Hi, tid.Lo, tid.Property);
        DispatchKindField.SetValue(untagged, NodeKind.Other);
        Assert.Equal(NodeKind.Other, untagged.DispatchKind);
        Assert.Equal(expected, FilterEvaluator.Matches(untagged, ev));
    }

    /// <summary>
    /// The trace-logs endpoint's own query, over a segment that has left the hot tier. A third of
    /// the rows carry the trace, a third carry another one, a third carry none.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task A_trace_id_filter_finds_its_rows_in_a_cold_segment(bool forward)
    {
        const int Rows = 30;
        string dir = Path.Combine(Path.GetTempPath(), "ameto-traceid-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        var engine = new StorageEngine(
            Options.Create(new ServerOptions { DataDirectory = dir }),
            new RetentionStore(new ServerOptions { DataDirectory = dir }, NullLogger<RetentionStore>.Instance),
            NullLogger<StorageEngine>.Instance);
        try
        {
            var query     = new QueryExecutor(engine, new SegmentIndexReaderFactory(), NullLogger<QueryExecutor>.Instance);
            long baseTicks = new DateTimeOffset(2026, 9, 15, 0, 0, 0, TimeSpan.Zero).UtcTicks;
            int  tmplIdx   = engine.TemplatePool.Intern("step {n} of the checkout");
            int  svcIdx    = engine.TemplatePool.Intern("Svc.Checkout");

            var buf = new ArrayBufferWriter<byte>(64);
            for (int i = 0; i < Rows; i++)
            {
                buf.ResetWrittenCount();
                var w = new MessagePackWriter(buf);
                w.WriteMapHeader(1);
                w.Write("n"); w.Write((long)i);
                w.Flush();

                var ids = (Ids)(i % 3);
                Assert.True(engine.TryWrite(new LogEventHeader
                {
                    Id                       = new EventId(0u, (uint)i).RawValue,
                    TimestampUtcTicks        = baseTicks + i * TimeSpan.TicksPerSecond,
                    Level                    = LogLevel.Information,
                    MessageTemplatePoolIndex = tmplIdx,
                    ServiceNamePoolIndex     = svcIdx,
                    TraceIdHi                = ids == Ids.None ? 0 : TraceHi,
                    TraceIdLo                = ids switch { Ids.Same => TraceLo, Ids.Different => OtherLo, _ => 0 },
                    SpanId                   = ids switch { Ids.Same => Span,    Ids.Different => OtherSpan, _ => 0 },
                }, buf.WrittenSpan.ToArray()));
            }
            await engine.FlushHotTierAsync();

            // Cold, and only cold: nothing left for the hot tier's header pre-check to answer.
            Assert.Single(engine.ListSegments());
            using (var hot = ((ISegmentProvider)engine).OpenHotTierReader())
                Assert.Empty(hot.ReadAll());

            long[] same  = [.. Enumerable.Range(0, Rows).Where(static i => i % 3 == 0).Select(static i => (long)i)];
            long[] other = [.. Enumerable.Range(0, Rows).Where(static i => i % 3 != 0).Select(static i => (long)i)];

            Assert.Equal(same,  await RowsAsync(query, $"@tr = '{TraceHex}'", forward));
            Assert.Equal(same,  await RowsAsync(query, $"@tr = '{TraceHex.ToUpperInvariant()}'", forward));
            Assert.Equal(other, await RowsAsync(query, $"@tr != '{TraceHex}'", forward));
            Assert.Equal(same,  await RowsAsync(query, $"@sp = '{SpanHex}'", forward));
            Assert.Equal(other, await RowsAsync(query, $"@sp != '{SpanHex}'", forward));
        }
        finally
        {
            await engine.DisposeAsync();
            // RetentionStore's pooled SQLite connection keeps Ameto.db open; without this the
            // delete fails on Windows and every run leaves a directory in TEMP.
            Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();
            try { Directory.Delete(dir, true); } catch { /* best-effort */ }
        }
    }

    private static async Task<long[]> RowsAsync(QueryExecutor query, string filter, bool forward)
    {
        var rows = await QuerySegmentFixtures.RunAsync(query, filter, count: 100, forward: forward);
        var n    = new long[rows.Count];
        for (int i = 0; i < rows.Count; i++)
            n[i] = Convert.ToInt64(rows[i].Properties!["n"]);
        Array.Sort(n);
        return n;
    }

    private static readonly FieldInfo RootField =
        typeof(CompiledFilter).GetField("_root", BindingFlags.Instance | BindingFlags.NonPublic)
        ?? throw new InvalidOperationException("CompiledFilter no longer keeps its tree in _root");

    private static readonly FieldInfo DispatchKindField =
        typeof(FilterNode).GetField(nameof(FilterNode.DispatchKind), BindingFlags.Instance | BindingFlags.NonPublic)
        ?? throw new InvalidOperationException("FilterNode no longer has a DispatchKind field");

    private static FilterNode RootOf(CompiledFilter filter) => (FilterNode)RootField.GetValue(filter)!;

    private static LogEvent Event(Shape shape, Ids ids, bool properties)
    {
        ReadOnlyMemory<byte> raw = default;
        if (properties)
        {
            var buf = new ArrayBufferWriter<byte>(64);
            var w   = new MessagePackWriter(buf);
            w.WriteMapHeader(2);
            w.Write("Region");  w.Write("ae-dxb");
            w.Write("Elapsed"); w.Write(125);
            w.Flush();
            raw = buf.WrittenMemory;
        }

        return new LogEvent
        {
            Id              = new EventId(0u, 7u),
            Timestamp       = new DateTimeOffset(2026, 9, 15, 10, 0, 0, TimeSpan.Zero),
            Level           = LogLevel.Information,
            MessageTemplate = shape == Shape.Cold ? "request handled for {Region}" : "request handled",
            ServiceName     = "Svc.Checkout",
            RawProperties   = raw,
            TraceIdHi       = ids == Ids.None ? 0 : TraceHi,
            TraceIdLo       = ids switch { Ids.Same => TraceLo, Ids.Different => OtherLo, _ => 0 },
            SpanId          = ids switch { Ids.Same => Span,    Ids.Different => OtherSpan, _ => 0 },
        };
    }
}
