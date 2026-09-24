using Ameto.Core;
using Ameto.Metrics;
using Ameto.Metrics.Storage;
using Ameto.Testing;
using Microsoft.Extensions.Logging.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// THE METRIC LABEL POOL IS RESET BY EPOCH (#88). It never evicts, and churning labels — pod names,
/// container ids — fill it; after that every label it has not seen cost a fresh string on every
/// export, and every label set of such a series a fresh set per point, until the process restarted.
/// Now a miss that finds it full resets it — once <see cref="MetricLabelInterner.ResetInterval"/>
/// has passed since the last reset — and live traffic re-interns.
///
/// <para>Time is a <see cref="ManualTimeProvider"/>: it moves only in <c>Advance</c>, and the
/// interner reads it only on a miss against a full pool, or on text new to both pools while a reset's
/// bridge is up — no timer, nothing racing the asserts.</para>
/// </summary>
public sealed class MetricLabelPoolResetTests : IAsyncLifetime
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-mreset-" + Guid.NewGuid().ToString("N"));
    private MetricStorageEngine? _engine;

    public Task InitializeAsync()
    {
        Directory.CreateDirectory(_dir);
        return Task.CompletedTask;
    }

    public async Task DisposeAsync()
    {
        if (_engine is not null) await _engine.DisposeAsync();
        try { Directory.Delete(_dir, true); } catch { }
    }

    private static readonly TimeSpan Tick = TimeSpan.FromTicks(1);

    /// <summary>Fills an 8-string pool up with <c>prefix0</c>, <c>prefix1</c>, … — every one of them pooled.</summary>
    private static void Fill(MetricLabelInterner interner, string prefix)
    {
        for (int i = 0; interner.Strings.ClaimedCount < 8; i++)
            Assert.True(Intern(interner, prefix + i.ToString(System.Globalization.CultureInfo.InvariantCulture), out _) >= 0);
    }

    /// <summary>From UTF-8, as the OTLP parsers intern: a miss materialises a NEW string instance.</summary>
    private static int Intern(MetricLabelInterner interner, string text, out string value) =>
        interner.Intern(System.Text.Encoding.UTF8.GetBytes(text), out value);

    [Fact]
    public void A_full_pool_resets_after_the_interval_and_not_before_and_then_interns_again()
    {
        var clock    = new ManualTimeProvider();
        var interner = new MetricLabelInterner(maxStrings: 8, labelSetSlots: 16, clock);

        Fill(interner, "pod-a-");
        Assert.Equal(-1, interner.Intern("pod-b-0", out _));                // full: not pooled
        Assert.Equal((1, 0), (interner.Saturations, interner.Resets));

        clock.Advance(MetricLabelInterner.ResetInterval - Tick);
        Assert.Equal(-1, interner.Intern("pod-b-1", out _));                // an hour less a tick: still full
        Assert.Equal((1, 0), (interner.Saturations, interner.Resets));

        clock.Advance(Tick);
        Assert.Equal(-1, interner.Intern("pod-b-2", out _));                // this miss resets; it stays unpooled
        Assert.Equal((1, 1), (interner.Saturations, interner.Resets));
        Assert.Equal(0, interner.Strings.ClaimedCount);                      // a new, empty epoch

        // New labels intern again — the same instance for the same text, from the bytes and from a string.
        int id = Intern(interner, "pod-b-2", out string first);
        Assert.True(id >= 0);
        Assert.Equal(id, interner.Intern("pod-b-2", out string second));
        Assert.Same(first, second);

        // THE THRASH GUARD: the new epoch filling at once does not reset it again before the interval.
        Fill(interner, "pod-c-");
        Assert.Equal(-1, interner.Intern("pod-d-0", out _));
        Assert.Equal((2, 1), (interner.Saturations, interner.Resets));
        clock.Advance(MetricLabelInterner.ResetInterval - Tick);
        Assert.Equal(-1, interner.Intern("pod-d-1", out _));
        Assert.Equal(1, interner.Resets);
        clock.Advance(Tick);
        Assert.Equal(-1, interner.Intern("pod-d-2", out _));
        Assert.Equal((2, 2), (interner.Saturations, interner.Resets));
    }

    [Fact]
    public void A_pool_that_is_not_full_is_never_reset_however_long_it_lives()
    {
        var clock    = new ManualTimeProvider();
        var interner = new MetricLabelInterner(maxStrings: 8, labelSetSlots: 16, clock);
        int id = interner.Intern("svc", out string svc);

        clock.Advance(TimeSpan.FromDays(30));
        for (int i = 0; i < 6; i++) interner.Intern("v" + i.ToString(System.Globalization.CultureInfo.InvariantCulture), out _);
        // Seven of eight held; and a string too long to pool is a miss too — but not a full pool's.
        interner.Intern(new string('x', MetricLabelInterner.MaxInternedUtf8Bytes + 1), out _);

        Assert.Equal((0, 0), (interner.Saturations, interner.Resets));
        Assert.Equal(id, interner.Intern("svc", out string again));
        Assert.Same(svc, again);
    }

    /// <summary>
    /// THE BRIDGE (#88 review, finding 6). A string still sent after a reset re-enters the new pool
    /// as its OLD instance — the one the hot tier's and the WAL index's keys hold — so a live series
    /// is matched by reference, not by a string compare per label per point. A string nobody sends
    /// is not carried over, and the bridge ends after an interval: what comes back later is new.
    /// </summary>
    [Fact]
    public void A_string_sent_again_after_a_reset_keeps_its_instance_for_one_interval()
    {
        var clock    = new ManualTimeProvider();
        var interner = new MetricLabelInterner(maxStrings: 8, labelSetSlots: 16, clock);
        Intern(interner, "live", out string live);
        Intern(interner, "dead", out string dead);
        Fill(interner, "churn-");
        clock.Advance(MetricLabelInterner.ResetInterval);
        Assert.Equal(-1, interner.Intern("trigger", out _));
        Assert.Equal(1, interner.Resets);

        // Sent again: its old instance, claimed into the new pool (and the JSON path's string overload too).
        Assert.True(Intern(interner, "live", out string again) >= 0);
        Assert.Same(live, again);
        Assert.True(interner.Intern(new string("live".AsSpan()), out string viaString) >= 0);
        Assert.Same(live, viaString);
        Assert.Equal(1, interner.Strings.ClaimedCount);                       // nothing else came over
        Assert.False(interner.Strings.TryGet("dead".AsSpan(), out _, out _));

        // An interval on, text new to both pools ends the bridge; what returns after that is new.
        clock.Advance(MetricLabelInterner.ResetInterval);
        Intern(interner, "brand-new", out _);
        Intern(interner, "dead", out string deadAgain);
        Assert.Equal("dead", deadAgain);
        Assert.NotSame(dead, deadAgain);
    }

    /// <summary>
    /// NO SPLIT: a series ingested before the reset and after it — its label set built from the old
    /// epoch's strings, then from the new epoch's — is ONE series in storage and in the catalog. The
    /// two label sets are different instances holding different string instances, and equal by value,
    /// which is what storage keys on.
    /// </summary>
    [Fact]
    public async Task A_hot_series_is_one_series_across_a_reset()
    {
        var clock    = new ManualTimeProvider();
        var interner = new MetricLabelInterner(maxStrings: 8, labelSetSlots: 16, clock);
        _engine = new MetricStorageEngine(_dir, NullLogger<MetricStorageEngine>.Instance, new MetricsOptions
        {
            HotTierBytes    = MemoryBudgets.MetricHotTierCapBytes,
            MinFlushBytes   = MemoryBudgets.MetricHotTierCapBytes / 10,
            WalInitialBytes = 1L * 1024 * 1024,
        });

        long t0 = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L - 60_000_000_000L;
        var before = Labels(interner, ("k8s.pod.name", "checkout-7d9f-abcde"), ("service.name", "checkout"));
        _engine.Ingest([Point(before, t0), Point(before, t0 + 1_000_000_000L)]);

        // Churn fills the pool; an hour later a miss resets it.
        Fill(interner, "gone-");
        clock.Advance(MetricLabelInterner.ResetInterval);
        interner.Intern("gone-last", out _);
        Assert.Equal(1, interner.Resets);

        var after = Labels(interner, ("k8s.pod.name", "checkout-7d9f-abcde"), ("service.name", "checkout"));
        // Carried over the bridge: its strings, and then the set itself from the old table — the very
        // instance the stored key holds, so every later point matches it by reference.
        for (int i = 0; i < before.Count; i++)
        {
            Assert.Same(before.KeyAt(i),   after.KeyAt(i));
            Assert.Same(before.ValueAt(i), after.ValueAt(i));
        }
        Assert.Same(before, after);
        Assert.Equal(before, after);
        Assert.Equal(before.GetHashCode(), after.GetHashCode());
        Assert.Same(after, Labels(interner, ("service.name", "checkout"), ("k8s.pod.name", "checkout-7d9f-abcde")));   // cached again

        _engine.Ingest([Point(after, t0 + 2_000_000_000L), Point(after, t0 + 3_000_000_000L)]);

        var series = new List<MetricSeries>();
        await foreach (var s in _engine.QueryAsync("churn.gauge")) series.Add(s);
        Assert.Single(series);
        Assert.Equal(4, series[0].Points.Count);
        Assert.Equal(1, Assert.Single(_engine.GetCatalog("churn.gauge")).Cardinality);
    }

    /// <summary>A label set built the way the OTLP parsers build one: every string through the interner, then the set.</summary>
    private static LabelSet Labels(MetricLabelInterner interner, params (string K, string V)[] pairs)
    {
        var kv  = new string[pairs.Length * 2];
        var ids = new int[pairs.Length * 2];
        for (int i = 0; i < pairs.Length; i++)
        {
            ids[2 * i]     = Intern(interner, pairs[i].K, out kv[2 * i]);
            ids[2 * i + 1] = Intern(interner, pairs[i].V, out kv[2 * i + 1]);
        }
        return interner.GetLabelSet(kv, ids);
    }

    private static MetricIngestItem Point(LabelSet labels, long nano) => new()
    {
        Name              = "churn.gauge",
        Kind              = MetricKind.Gauge,
        Unit              = "1",
        Labels            = labels,
        TimestampUnixNano = nano,
        ScalarValue       = 1,
    };
}
