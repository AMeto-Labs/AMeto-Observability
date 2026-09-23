using Ameto.Metrics;
using Ameto.Metrics.Storage;

namespace Ameto.Storage.Tests;

/// <summary>
/// <see cref="LabelSet"/> over one interleaved array, compared by reference first; and the
/// <see cref="MetricLabelInterner"/> that makes the references worth comparing. What must not
/// move: a label set built from uninterned copies — the metric WAL, <c>MetricReader</c>, any
/// caller — is equal to, and hashes like, the interned one with the same text; and an interner
/// past its bounds hands back the right text, never an empty or a missing one.
/// </summary>
public sealed class MetricLabelInterningTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-mintern-" + Guid.NewGuid().ToString("N"));

    public MetricLabelInterningTests() => Directory.CreateDirectory(_dir);

    public void Dispose()
    {
        try { Directory.Delete(_dir, true); } catch { }
    }

    /// <summary>A copy no pool, literal table or interner has ever seen.</summary>
    private static string Fresh(string s) => new(s.AsSpan());

    private static LabelSet Build(params (string K, string V)[] pairs)
    {
        var kv = new KeyValuePair<string, string>[pairs.Length];
        for (int i = 0; i < pairs.Length; i++) kv[i] = new(pairs[i].K, pairs[i].V);
        return new LabelSet(kv);
    }

    private static LabelSet ViaInterner(MetricLabelInterner interner, params (string K, string V)[] pairs)
    {
        var kv  = new string[pairs.Length * 2];
        var ids = new int[pairs.Length * 2];
        for (int i = 0; i < pairs.Length; i++)
        {
            ids[2 * i]     = interner.Intern(System.Text.Encoding.UTF8.GetBytes(pairs[i].K), out kv[2 * i]);
            ids[2 * i + 1] = interner.Intern(System.Text.Encoding.UTF8.GetBytes(pairs[i].V), out kv[2 * i + 1]);
        }
        return interner.GetLabelSet(kv, ids);
    }

    [Fact]
    public void Equality_and_hash_stay_value_based_for_uninterned_strings()
    {
        var interner = new MetricLabelInterner(1024, 64);
        var interned = ViaInterner(interner, ("service.name", "checkout"), ("http.route", "/api/v1/orders"));

        // Same text, every string a private copy, pairs in the other order: what the WAL replay
        // and MetricReader build from disk.
        var fromDisk = Build((Fresh("http.route"), Fresh("/api/v1/orders")),
                             (Fresh("service.name"), Fresh("checkout")));

        Assert.NotSame(interned, fromDisk);
        Assert.NotSame(interned.KeyAt(0), fromDisk.KeyAt(0));
        Assert.True(interned.Equals(fromDisk));
        Assert.True(fromDisk.Equals(interned));
        Assert.Equal(interned.GetHashCode(), fromDisk.GetHashCode());

        // Which is what the hot tier's and the log's dictionaries rely on: a series keyed by one
        // is found by the other.
        var map = new Dictionary<LabelSet, int> { [fromDisk] = 7 };
        Assert.True(map.TryGetValue(interned, out int found));
        Assert.Equal(7, found);

        // A different value is still a different set, even with every key shared.
        var other = Build(("http.route", "/api/v1/orders"), ("service.name", Fresh("checkouT")));
        Assert.False(interned.Equals(other));
    }

    [Fact]
    public void The_interleaved_layout_reads_back_as_the_pairs_it_was_given()
    {
        var set = Build(("b", "2"), ("a", "1"), ("a", "0"), ("c", ""));

        // Key, then value, ordinal — the canonical order the pair-array layout had.
        (string, string)[] expected = [("a", "0"), ("a", "1"), ("b", "2"), ("c", "")];
        Assert.Equal(expected, set.Pairs.ToArray());
        Assert.Equal(4, set.Count);
        Assert.Equal(4, set.Pairs.Count);

        var viaEnumerator = new List<(string, string)>();
        foreach (var (k, v) in set) viaEnumerator.Add((k, v));
        Assert.Equal(expected, viaEnumerator);

        for (int i = 0; i < set.Count; i++)
        {
            Assert.Equal(expected[i].Item1, set.KeyAt(i));
            Assert.Equal(expected[i].Item2, set.ValueAt(i));
            Assert.Equal(expected[i], set.Pairs[i]);
        }

        Assert.Empty(LabelSet.Empty.Pairs);
        Assert.Equal(0, LabelSet.Empty.Count);
        Assert.Equal("{a=\"0\",a=\"1\",b=\"2\",c=\"\"}", set.ToString());
    }

    [Fact]
    public void Pooled_labels_come_back_as_the_label_set_built_the_first_time()
    {
        var interner = new MetricLabelInterner(1024, 64);

        var first  = ViaInterner(interner, ("service.name", "checkout"), ("http.route", "/a"));
        var second = ViaInterner(interner, ("http.route", "/a"), ("service.name", "checkout"));   // other order
        Assert.Same(first, second);

        // A string the interner will not pool (past MaxInternedUtf8Bytes) has no id to key on:
        // the set is built fresh every time — equal, never shared, never wrong.
        string longValue = new('x', MetricLabelInterner.MaxInternedUtf8Bytes + 1);
        var a = ViaInterner(interner, ("service.name", "checkout"), ("payload", longValue));
        var b = ViaInterner(interner, ("service.name", "checkout"), ("payload", longValue));
        Assert.NotSame(a, b);
        Assert.Equal(a, b);
        Assert.Equal(("payload", longValue), a.Pairs[0]);          // "payload" sorts first

        // An empty value is canonical without a pool entry and keeps the set cacheable.
        var e1 = ViaInterner(interner, ("service.name", "checkout"), ("empty", ""));
        var e2 = ViaInterner(interner, ("service.name", "checkout"), ("empty", ""));
        Assert.Same(e1, e2);
    }

    [Fact]
    public void Past_its_cap_the_interner_answers_the_text_itself_and_never_an_empty_string()
    {
        var interner  = new MetricLabelInterner(maxStrings: 4, labelSetSlots: 2);
        int exhausted = 0;
        interner.Strings.PoolExhausted += _ => exhausted++;

        var seen = new List<(int Id, string Value)>();
        for (int i = 0; i < 10; i++)
        {
            int id = interner.Intern(System.Text.Encoding.UTF8.GetBytes("value-" + i), out string v);
            seen.Add((id, v));
        }

        for (int i = 0; i < 10; i++)
        {
            Assert.Equal("value-" + i, seen[i].Value);          // right text, pooled or not
            if (i < 4) Assert.True(seen[i].Id >= 0);
            else       Assert.Equal(-1, seen[i].Id);             // degraded: a plain new string
        }
        Assert.Equal(1, exhausted);                               // said once, not per miss

        // The pooled ones still hit, allocation-free, with their own instance.
        interner.Intern("value-2"u8, out string again);
        Assert.Same(seen[2].Value, again);

        // And a label set holding an unpooled string is still a correct label set.
        var set = ViaInterner(interner, ("value-0", "value-9"));
        Assert.Equal(Build(("value-0", "value-9")), set);
    }

    [Fact]
    public void A_replayed_series_holds_the_strings_the_live_path_interns()
    {
        string walPath = Path.Combine(_dir, "metrics.wal");

        // Appended from private copies, so nothing about the WRITE side made them canonical.
        using (var wal = MetricWriteAheadLog.Open(walPath, 1L * 1024 * 1024))
        {
            wal.Append(new MetricIngestItem
            {
                Name              = Fresh("replayed.metric"),
                Unit              = Fresh("ms"),
                Kind              = MetricKind.Gauge,
                Labels            = Build((Fresh("replay.key"), Fresh("replay-value"))),
                TimestampUnixNano = 1_785_300_000_000_000_000L,
                ScalarValue       = 1,
            }, new MetricDataPoint { TimestampUnixNano = 1_785_300_000_000_000_000L, Value = 1 });
        }

        using var reopened = MetricWriteAheadLog.Open(walPath, 1L * 1024 * 1024);
        var points = reopened.ReadAll(out int unresolved);
        Assert.Equal(0, unresolved);
        var p = Assert.Single(points);

        var shared = MetricLabelInterner.Shared;
        Assert.Same(shared.Intern("replayed.metric"), p.Name);
        Assert.Same(shared.Intern("replay.key"),      p.Labels.KeyAt(0));
        Assert.Same(shared.Intern("replay-value"),    p.Labels.ValueAt(0));
    }
}
