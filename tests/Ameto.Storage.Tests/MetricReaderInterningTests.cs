using Ameto.Metrics;
using Ameto.Metrics.Storage;

namespace Ameto.Storage.Tests;

/// <summary>
/// A cold read LOOKS UP its label text in the metric label interner and adds nothing to it (issue
/// #83: the WP6 follow-up left to WP7, then made lookup-only by the WP7 review, finding F1).
///
/// <para>Live text — what the OTLP parsers and the WAL replay pooled — comes back as the pool's
/// shared instances, and a label set the table holds as that very set. Text the pool does not
/// hold is built fresh and NOT pooled: the pool never evicts, and a cold read is the one reader
/// that meets every dead value of the retention window (the catalog seed reads every file at
/// startup), so pooling it filled the pool at boot and left every series started afterwards to
/// be ingested at the uninterned cost.</para>
///
/// <para>Every fact but the one about <see cref="MetricLabelInterner.Shared"/> itself runs on a
/// PRIVATE interner, so none depends on how full the process-wide pool is by the time it runs.</para>
/// </summary>
public sealed class MetricReaderInterningTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-mrintern-" + Guid.NewGuid().ToString("N"));

    public MetricReaderInterningTests() => Directory.CreateDirectory(_dir);
    public void Dispose() { try { Directory.Delete(_dir, true); } catch { } }

    private string WriteOne(LabelSet labels, string unit)
    {
        var items = new List<(SeriesKey, HotSeries)>
        {
            (new SeriesKey("intern.m", MetricKind.Gauge, unit, labels),
             new HotSeries([new MetricDataPoint { TimestampUnixNano = 1_784_800_020_000_000_000L, Value = 1 }])),
        };
        return Assert.Single(MetricWriter.Write(_dir, items, MetricGranularity.Raw)).FilePath;
    }

    /// <summary>A copy of <paramref name="s"/> — how a series written long ago holds its text.</summary>
    private static string Copy(string s) => new(s.AsSpan());

    [Fact]
    public void A_cold_read_hands_back_the_strings_and_the_label_set_live_ingest_holds()
    {
        var interner = new MetricLabelInterner(1_024, 64);
        // What a parser does for a live series: pool its strings and publish its label set.
        int kId = interner.Intern("http.route", out string key);
        int vId = interner.Intern("/orders/{id}", out string value);
        interner.Intern("s", out string unit);
        var live = interner.GetLabelSet(new[] { key, value }, new[] { kId, vId });
        int claimed = interner.Strings.ClaimedCount;

        string file = WriteOne(new LabelSet([new(Copy(key), Copy(value))]), Copy(unit));

        var first  = Assert.Single(MetricReader.ReadAllSync(file, interner));
        var second = Assert.Single(MetricReader.ReadAllSync(file, interner));

        Assert.Same(key,   first.Labels.KeyAt(0));
        Assert.Same(value, first.Labels.ValueAt(0));
        Assert.Same(unit,  first.Unit);
        Assert.Same(live,  first.Labels);           // the set the table holds, not a new one
        Assert.Same(live,  second.Labels);
        Assert.Equal(claimed, interner.Strings.ClaimedCount);
    }

    [Fact]
    public void A_cold_read_of_text_the_pool_does_not_hold_adds_none_of_it()
    {
        var interner = new MetricLabelInterner(1_024, 64);
        interner.Intern("warm", out _);
        int claimed = interner.Strings.ClaimedCount;

        string tag = Guid.NewGuid().ToString("N")[..12];
        var labels = new LabelSet([new("dead.key." + tag, "dead.value." + tag), new("pod", "pod-" + tag)]);
        string file = WriteOne(labels, "unit." + tag);

        var first  = Assert.Single(MetricReader.ReadAllSync(file, interner));
        var second = Assert.Single(MetricReader.ReadAllSync(file, interner));

        Assert.Equal(claimed, interner.Strings.ClaimedCount);     // nothing pooled
        Assert.Equal(labels, first.Labels);                        // the same text, by value
        Assert.Equal("unit." + tag, first.Unit);
        Assert.NotSame(first.Labels, second.Labels);               // nothing published to the table either
        Assert.NotSame(first.Labels.ValueAt(0), second.Labels.ValueAt(0));
        Assert.Equal(first.Labels.GetHashCode(), second.Labels.GetHashCode());

        // A string that is live now, beside one that is not: the live one is shared, the set is new.
        interner.Intern("pod", out string podKey);
        var third = Assert.Single(MetricReader.ReadAllSync(file, interner));
        Assert.Same(podKey, third.Labels.KeyAt(1));                // pairs sort by key: "dead.key.…" < "pod"
        Assert.Equal(labels, third.Labels);
    }

    [Fact]
    public void A_cold_read_through_the_shared_interner_leaves_its_pool_as_it_found_it()
    {
        // The process-wide pool itself: N strings it has never seen, read cold, and its fill does
        // not move. (Storage.Tests runs its classes one at a time, so nothing else interns meanwhile.)
        string tag = Guid.NewGuid().ToString("N");
        var pairs = new List<KeyValuePair<string, string>>();
        for (int i = 0; i < 20; i++) pairs.Add(new("never.seen." + i.ToString(System.Globalization.CultureInfo.InvariantCulture) + "." + tag, "value." + i + "." + tag));
        string file = WriteOne(new LabelSet(pairs), "unit." + tag);

        int before = MetricLabelInterner.Shared.Strings.ClaimedCount;
        var got = Assert.Single(MetricReader.ReadAllSync(file));
        Assert.Equal(20, got.Labels.Count);
        Assert.Equal(before, MetricLabelInterner.Shared.Strings.ClaimedCount);

        // A count check alone passes against an inserting read once Shared is full (Claim refuses
        // and ClaimedCount is capped), so also ask for the text itself: a lookup-only read left
        // none of it in the pool, whatever the pool's fill.
        Assert.False(MetricLabelInterner.Shared.Strings.TryGet(got.Labels.KeyAt(0), out _, out _));
        Assert.False(MetricLabelInterner.Shared.Strings.TryGet(got.Labels.ValueAt(0), out _, out _));
    }

    [Fact]
    public void Text_the_pool_will_not_hold_is_read_as_before_equal_by_value()
    {
        // Over MetricLabelInterner.MaxInternedUtf8Bytes: never pooled, so each read builds its own
        // string and its own label set — equal, as a read always was.
        var interner = new MetricLabelInterner(1_024, 64);
        string longValue = new('x', MetricLabelInterner.MaxInternedUtf8Bytes + 1);
        string file = WriteOne(new LabelSet([new("k", longValue)]), "");

        var first  = Assert.Single(MetricReader.ReadAllSync(file, interner));
        var second = Assert.Single(MetricReader.ReadAllSync(file, interner));

        Assert.Equal(longValue, first.Labels.ValueAt(0));
        Assert.NotSame(first.Labels.ValueAt(0), second.Labels.ValueAt(0));
        Assert.Equal(first.Labels, second.Labels);
        Assert.Equal(first.Labels.GetHashCode(), second.Labels.GetHashCode());
        Assert.Same(string.Empty, first.Unit);
    }

    /// <summary>
    /// ONE LENGTH RULE, IN UTF-8 BYTES, ON EVERY PATH (#94). The JSON mapper interns strings, and
    /// that path counted CHARS, while a cold read and the protobuf parser count BYTES: a Cyrillic
    /// value of 100 chars (200 bytes) was pooled through JSON and then never found by a cold read —
    /// a pool slot no lookup could match. Now the string path measures the bytes the string encodes
    /// to: the 100-char value is pooled by no path, and a 64-char one (128 bytes, the limit) by every
    /// path, where the cold read hands back the very instance the JSON path pooled.
    /// </summary>
    [Fact]
    public void The_JSON_path_and_a_cold_read_measure_a_label_by_the_same_bytes()
    {
        var interner = new MetricLabelInterner(1_024, 64);
        string fits = new('Ж', 64);            // 128 UTF-8 bytes: the longest Cyrillic value the pool takes
        string over = new('Ж', 100);           // 200 bytes, 100 chars: the char count used to take it

        Assert.True(interner.Intern(fits, out string pooled) >= 0);        // what the JSON mapper calls
        Assert.Equal(-1, interner.Intern(over, out string kept));
        Assert.Same(over, kept);
        int claimed = interner.Strings.ClaimedCount;

        // The protobuf parser decides the same, from the bytes on the wire.
        Assert.Equal(-1, interner.Intern(System.Text.Encoding.UTF8.GetBytes(over), out _));
        Assert.True(interner.Intern(System.Text.Encoding.UTF8.GetBytes(fits), out string viaBytes) >= 0);
        Assert.Same(pooled, viaBytes);

        // And a cold read: the pooled value comes back as the JSON path's instance, the other by value.
        string file = WriteOne(new LabelSet([new("fits", Copy(fits)), new("over", Copy(over))]), "");
        var got = Assert.Single(MetricReader.ReadAllSync(file, interner));
        Assert.Same(pooled, got.Labels.ValueAt(0));                           // "fits" < "over"
        Assert.Equal(over, got.Labels.ValueAt(1));
        Assert.Equal(claimed, interner.Strings.ClaimedCount);
    }

    [Fact]
    public void A_label_set_too_large_to_intern_is_decoded_whole()
    {
        var pairs = new List<KeyValuePair<string, string>>();
        for (int i = 0; i < 80; i++) pairs.Add(new("k" + i.ToString("D3", System.Globalization.CultureInfo.InvariantCulture), "v" + i));
        var labels = new LabelSet(pairs);
        string file = WriteOne(labels, "u");

        var got = Assert.Single(MetricReader.ReadAllSync(file, new MetricLabelInterner(1_024, 64)));
        Assert.Equal(labels, got.Labels);
        Assert.Equal(80, got.Labels.Count);
    }
}
