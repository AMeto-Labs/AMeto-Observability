using Ameto.Metrics;
using Ameto.Metrics.Storage;

namespace Ameto.Storage.Tests;

/// <summary>
/// A cold read resolves its label text through <see cref="MetricLabelInterner.Shared"/> — the pool
/// the OTLP parsers and the WAL replay already share (issue #83: the WP6 follow-up left to WP7,
/// whose file the reader is). What it buys: the series a query reads back are, almost always,
/// the series ingest is still sending, so their strings and label sets already exist; a read that
/// decoded them afresh allocated ten strings and a label set per five-label series, per query,
/// per rollup chunk, per catalog seed.
/// </summary>
public sealed class MetricReaderInterningTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-mrintern-" + Guid.NewGuid().ToString("N"));

    public MetricReaderInterningTests() => Directory.CreateDirectory(_dir);
    public void Dispose() { try { Directory.Delete(_dir, true); } catch { } }

    /// <summary>
    /// The shared pool is process-wide and bounded; a fact about sharing is only meaningful while
    /// it still has room, so a full pool fails loudly here instead of passing for nothing.
    /// </summary>
    private static string Pooled(string text)
    {
        int id = MetricLabelInterner.Shared.Intern(text, out string pooled);
        Assert.True(id >= 0, $"the shared metric label pool is full (earlier tests filled it); '{text}' could not be pooled");
        return pooled;
    }

    private string WriteOne(LabelSet labels, string unit)
    {
        var items = new List<(SeriesKey, HotSeries)>
        {
            (new SeriesKey("intern.m", MetricKind.Gauge, unit, labels),
             new HotSeries([new MetricDataPoint { TimestampUnixNano = 1_784_800_020_000_000_000L, Value = 1 }])),
        };
        return Assert.Single(MetricWriter.Write(_dir, items, MetricGranularity.Raw)).FilePath;
    }

    [Fact]
    public void A_cold_read_hands_back_the_strings_and_the_label_set_live_ingest_holds()
    {
        string tag   = Guid.NewGuid().ToString("N")[..12];
        string key   = Pooled("intern.key." + tag);
        string value = Pooled("intern.value." + tag);
        string unit  = Pooled("unit." + tag);

        // Built from COPIES, as a series written long ago was: nothing below may lean on the
        // writer having been handed the pooled instances.
        var labels = new LabelSet([new(new string(key.AsSpan()), new string(value.AsSpan()))]);
        string file = WriteOne(labels, new string(unit.AsSpan()));

        var first  = Assert.Single(MetricReader.ReadAllSync(file));
        var second = Assert.Single(MetricReader.ReadAllSync(file));

        Assert.Same(key,   first.Labels.KeyAt(0));
        Assert.Same(value, first.Labels.ValueAt(0));
        Assert.Same(unit,  first.Unit);
        // Every string pooled, so the label set itself is the interner's: one instance per series,
        // however many times it is read.
        Assert.Same(first.Labels, second.Labels);
        Assert.Equal(labels, first.Labels);
    }

    [Fact]
    public void Text_the_pool_will_not_hold_is_read_as_before_equal_by_value()
    {
        // Over MetricLabelInterner.MaxInternedUtf8Bytes: never pooled, so each read builds its own
        // string and its own label set — equal, as a read always was.
        string longValue = new('x', MetricLabelInterner.MaxInternedUtf8Bytes + 1);
        string file = WriteOne(new LabelSet([new("k", longValue)]), "");

        var first  = Assert.Single(MetricReader.ReadAllSync(file));
        var second = Assert.Single(MetricReader.ReadAllSync(file));

        Assert.Equal(longValue, first.Labels.ValueAt(0));
        Assert.NotSame(first.Labels.ValueAt(0), second.Labels.ValueAt(0));
        Assert.Equal(first.Labels, second.Labels);
        Assert.Equal(first.Labels.GetHashCode(), second.Labels.GetHashCode());
        Assert.Same(string.Empty, first.Unit);
    }

    [Fact]
    public void A_label_set_too_large_to_intern_is_decoded_whole()
    {
        var pairs = new List<KeyValuePair<string, string>>();
        for (int i = 0; i < 80; i++) pairs.Add(new("k" + i.ToString("D3", System.Globalization.CultureInfo.InvariantCulture), "v" + i));
        var labels = new LabelSet(pairs);
        string file = WriteOne(labels, "u");

        var got = Assert.Single(MetricReader.ReadAllSync(file));
        Assert.Equal(labels, got.Labels);
        Assert.Equal(80, got.Labels.Count);
    }
}
