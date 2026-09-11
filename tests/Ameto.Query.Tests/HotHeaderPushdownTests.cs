using System.Buffers;
using MessagePack;
using Ameto.Core;
using Ameto.Query.Filtering;
using Ameto.Storage;

namespace Ameto.Query.Tests;

/// <summary>
/// The header-level pre-check (<see cref="CompiledFilter.HeaderPredicate"/>) may only ever
/// SKIP materialisations: for every filter, the hot scan with the predicate applied must
/// yield exactly the events the scan without it yields once the evaluator has had its say —
/// same set, same order. Tiers here mix levels, services (including events with none),
/// trace / span ids (including absent ones) and a handful of user properties, so every
/// pushed-down leaf has events on both sides of it.
/// </summary>
public sealed class HotHeaderPushdownTests : IDisposable
{
    private const int EventsPerTier = 400;

    private const string TraceA = "0123456789abcdef0123456789abcdef";
    private const string TraceB = "ffffffffffffffff0000000000000001";
    private const string SpanA  = "00000000000000aa";

    private readonly StringInternPool _pool = new();
    private readonly HotTierSegment   _frozen;
    private readonly HotTierSegment   _current;
    private readonly long             _base = new DateTimeOffset(2026, 9, 1, 0, 0, 0, TimeSpan.Zero).UtcTicks;

    public HotHeaderPushdownTests()
    {
        var rng  = new Random(11);
        _frozen  = BuildTier(rng, tierNo: 0);
        _current = BuildTier(rng, tierNo: 1);
    }

    public void Dispose()
    {
        _frozen.Dispose();
        _current.Dispose();
    }

    public static TheoryData<string, bool> Filters => new()
    {
        // expression, expected to produce a header predicate
        { "@l = 'Error'",                                              true  },
        { "@l != 'Error'",                                             true  },
        { "not @l = 'Information'",                                    true  },
        { "@l in ['Error', 'Fatal']",                                  true  },
        { "not @l in ['Debug', 'Verbose']",                            true  },
        { "@l = 'Error' or @l = 'Warning'",                            true  },
        { "not (@l = 'Debug' or @l = 'Verbose')",                      true  },
        { "@l = 'Info'",                                               true  },   // alias: matches nothing
        { "@l > 'Error'",                                              true  },   // ordinal string order, mirrored exactly
        { "Error",                                                     true  },   // bare LevelNode
        { "service.name = 'Svc.B'",                                    true  },
        { "service.name != 'Svc.B'",                                   true  },
        { "not service.name = 'svc.b'",                                true  },   // case-insensitive, negated
        { "service.name in ['Svc.A', 'Svc.C']",                        true  },
        { "service.name = null",                                       true  },   // events without a service
        { "service.name != null",                                      true  },
        { "@tr = '" + TraceA + "'",                                    true  },
        { "@tr = '" + TraceA.ToUpperInvariant() + "'",                 true  },   // literal case must not matter
        { "@tr != '" + TraceA + "'",                                   true  },   // includes events with no trace id
        { "not @tr = '" + TraceB + "'",                                true  },
        { "@tr = null",                                                true  },
        { "@tr != null",                                               true  },
        { "@tr = '00000000000000000000000000000000'",                  true  },   // the all-zero id is "absent": nothing
        { "@sp = '" + SpanA + "'",                                     true  },
        { "@sp != '" + SpanA + "'",                                    true  },
        { "@tr = 'abc'",                                               false },   // not an id rendering: string path
        { "@l = 'Error' and service.name = 'Svc.B' and @tr != null",   true  },
        { "@l = 'Error' and Route = '/api/pay'",                       true  },   // level pushed, property left to the evaluator
        { "@l = 'Error' or Route = '/api/pay'",                        false },   // OR across fields: must NOT push
        { "@l = 'Error' or service.name = 'Svc.A'",                    false },
        { "not (@l = 'Error' and Route = '/api/pay')",                 false },   // not(a and b) is a disjunction
        { "not (@l = 'Error' or Route = '/api/pay')",                  true  },   // not(a or b) = not a and not b: level half pushes
        { "Route = '/api/pay'",                                        false },
        { "",                                                          false },
    };

    [Theory]
    [MemberData(nameof(Filters))]
    public void Pushdown_yields_exactly_what_the_evaluator_alone_yields(string expression, bool expectPredicate)
    {
        var filter = CompiledFilter.Compile(expression);
        Assert.Equal(expectPredicate, filter.HeaderPredicate is not null);

        foreach (bool forward in new[] { true, false })
        {
            var without = Run(filter, pushdown: false, forward);
            var with    = Run(filter, pushdown: true,  forward);
            Assert.Equal(without, with);
        }
    }

    [Fact]
    public void Every_filter_here_has_matches_and_non_matches()
    {
        // Guards the suite against vacuity: a pushed-down leaf that rejects nothing (or
        // everything) proves nothing about the other side.
        foreach (var row in Filters)
        {
            string expression = (string)row[0];
            if (expression is "" or "@l = 'Info'" or "@tr = 'abc'" or "@tr = '00000000000000000000000000000000'") continue;
            var filter  = CompiledFilter.Compile(expression);
            int matched = Run(filter, pushdown: false, forward: true).Count;
            Assert.True(matched > 0 && matched < 2 * EventsPerTier, $"'{expression}' matched {matched}");
        }
    }

    [Fact]
    public void Derived_levels_are_the_levels_the_filter_admits()
    {
        Assert.Null(CompiledFilter.Compile("Route = '/api/pay'").DerivedLevels);
        Assert.Null(CompiledFilter.Compile("@l = 'Error' or Route = 'x'").DerivedLevels);
        Assert.Equal([LogLevel.Error], CompiledFilter.Compile("@l = 'Error'").DerivedLevels!);
        Assert.Equal(
            new HashSet<LogLevel> { LogLevel.Verbose, LogLevel.Debug, LogLevel.Information, LogLevel.Warning, LogLevel.Fatal },
            CompiledFilter.Compile("@l != 'Error'").DerivedLevels!);
        Assert.Empty(CompiledFilter.Compile("@l = 'Info'").DerivedLevels!);
        Assert.Equal(
            new HashSet<LogLevel> { LogLevel.Error, LogLevel.Fatal },
            CompiledFilter.Compile("@l in ['Error', 'Fatal'] and Route = 'x'").DerivedLevels!);
    }

    [Fact]
    public void Trace_id_rewrite_keeps_the_inverted_hint_the_string_compare_gave()
    {
        var hints = CompiledFilter.Compile("@tr = '" + TraceA + "'").GetInvertedHints();
        Assert.Single(hints);
        Assert.Equal(TraceA, hints[0].value);

        Assert.True(CompiledFilter.Compile("@tr = '" + TraceA + "'").TryGetIndexHint(out _, out var v));
        Assert.Equal(TraceA, v);
    }

    private List<ulong> Run(CompiledFilter filter, bool pushdown, bool forward)
    {
        var ids = new List<ulong>();
        foreach (var ev in HotTierScan.ReadSorted(
                     _current, [_frozen], _pool,
                     long.MinValue, long.MaxValue, null, null, forward, levels: null,
                     headerPredicate: pushdown ? filter.HeaderPredicate : null))
        {
            if (filter.Matches(ev)) ids.Add(ev.Id.RawValue);
        }
        return ids;
    }

    private HotTierSegment BuildTier(Random rng, int tierNo)
    {
        var tier = new HotTierSegment(EventsPerTier + 1, EventsPerTier * 512 + 1024 * 1024);

        int    tmplIdx = _pool.Intern("evt {n}");
        string tmpl    = _pool.Get(tmplIdx);
        int[]  svc     = [_pool.Intern("Svc.A"), _pool.Intern("Svc.B"), _pool.Intern("Svc.C"), -1];
        var    buf     = new ArrayBufferWriter<byte>(64);

        TraceIdHelper.TryParseTraceId(TraceA, out ulong aHi, out ulong aLo);
        TraceIdHelper.TryParseTraceId(TraceB, out ulong bHi, out ulong bLo);
        TraceIdHelper.TryParseSpanId(SpanA, out ulong spanA);

        for (int i = 0; i < EventsPerTier; i++)
        {
            buf.Clear();
            var w = new MessagePackWriter(buf);
            w.WriteMapHeader(2);
            w.Write("n");     w.Write((long)i);
            w.Write("Route"); w.Write(i % 4 == 0 ? "/api/pay" : "/api/other");
            w.Flush();

            int traceKind = rng.Next(0, 4);   // 0 absent, 1 A, 2 B, 3 random
            (ulong hi, ulong lo) = traceKind switch
            {
                1 => (aHi, aLo),
                2 => (bHi, bLo),
                3 => ((ulong)rng.NextInt64(1, long.MaxValue), (ulong)rng.NextInt64()),
                _ => (0UL, 0UL),
            };
            int spanKind = rng.Next(0, 3);    // 0 absent, 1 A, 2 random
            ulong span = spanKind switch { 1 => spanA, 2 => (ulong)rng.NextInt64(1, long.MaxValue), _ => 0 };

            var h = new LogEventHeader
            {
                Id                       = new EventId(0u, (uint)(tierNo * EventsPerTier + i)).RawValue,
                // Duplicate timestamps across tiers, out of order within a tier.
                TimestampUtcTicks        = _base + rng.Next(0, 300) * TimeSpan.TicksPerMillisecond,
                Level                    = (LogLevel)rng.Next(0, 6),
                MessageTemplatePoolIndex = tmplIdx,
                ServiceNamePoolIndex     = svc[rng.Next(0, 4)],
                TraceIdHi                = hi,
                TraceIdLo                = lo,
                SpanId                   = span,
            };
            Assert.True(tier.TryWrite(h, buf.WrittenSpan, tmpl));
        }
        tier.Freeze();
        return tier;
    }
}
