using Ameto.Metrics;

namespace Ameto.Otel;

/// <summary>A resolved label string and its interner id (see <see cref="MetricLabelInterner.Intern(ReadOnlySpan{byte}, out string)"/>).</summary>
internal readonly record struct InternedText(string Text, int Id);

/// <summary>
/// THE one builder of a metric data point's label set, for both OTLP encodings: the JSON mapper
/// (<see cref="OtlpMetricMapper"/>) and the protobuf parser (<see cref="OtlpMetricProtoParser"/>).
///
/// <para><b>Why one.</b> The rule this class applies — the resource's service name, the point's own
/// attributes, then every resource label the point does not already carry — decides a series'
/// IDENTITY. It used to be written twice, once per encoding, each copy with its own interleaved
/// kv/ids scratch and its own doubling resize; a fix made in one copy and not the other would split
/// every series by the encoding its exporter happened to use, and nothing but
/// <c>OtlpMetricProtoParityTests</c> would notice.</para>
///
/// <para><b>Use.</b> Per resource: <see cref="BeginResource"/>, then <see cref="SetServiceName"/> and
/// <see cref="AddResourceLabel"/> for what the resource carries (the caller applies the exclusions).
/// Per data point: <see cref="BeginPoint"/>, <see cref="Add"/> per attribute, <see cref="Build"/>, then
/// <see cref="Overflowed"/>: a point of more than <see cref="MaxLabels"/> labels is REFUSED, never
/// built (#132), and the caller counts it as rejected. One instance per request; not thread-safe.</para>
///
/// <para>The labels are kept interleaved k0, v0, k1, v1, … with each string's interner id beside it,
/// and handed to <see cref="MetricLabelInterner.GetLabelSet"/>, which sorts both in place and copies
/// only on a miss — a point whose labels are all pooled gets the label set built the first time.</para>
/// </summary>
internal sealed class MetricLabelSetBuilder
{
    /// <summary>
    /// The most labels a point may carry — its own attributes, the resource's service name and the
    /// resource labels merged in — and the most a resource may state. A point past it is refused whole
    /// rather than cut: cutting would silently re-key the series, the identity this class exists to
    /// protect, and no honest exporter puts anything like 256 attributes on one point. The bound is
    /// also what keeps the dedupe affordable: <see cref="Add"/> scans the keys already present, so a
    /// point was O(n²) in its attribute count — 20 000 attributes cost half a second of one core, and
    /// the point budget, which counts points, let an 8 MiB body of them through (#132). Below
    /// <see cref="MaxLabels"/> the scan is at most 256 compares an attribute in <see cref="Add"/>,
    /// and up to ~32 600 compares and a 256-pair sort a point in <see cref="Build"/>, where the
    /// resource's labels are merged in again for every point. What a request may cost is the point
    /// budget's to bound: <c>OtlpMetricPointBudget</c> charges a resource's attributes past its first
    /// 32 on every point under them (#132).
    /// </summary>
    internal const int MaxLabels = 256;

    public readonly MetricLabelInterner Interner;

    /// <summary>The point's labels, interleaved; <see cref="_used"/> strings.</summary>
    private string[] _kv  = new string[32];
    private int[]    _ids = new int[32];
    private int      _used;

    /// <summary>The resource's labels, interleaved like <see cref="_kv"/>; <see cref="_resUsed"/> strings.</summary>
    private string[] _resKv  = new string[16];
    private int[]    _resIds = new int[16];
    private int      _resUsed;

    /// <summary>The point has more than <see cref="MaxLabels"/> distinct keys: it is refused, nothing more is kept.</summary>
    private bool _overflow;

    /// <summary>The resource stated more than <see cref="MaxLabels"/> labels: every point below it is refused.</summary>
    private bool _resOverflow;

    private readonly InternedText _serviceNameKey;
    private InternedText?         _serviceName;

    public MetricLabelSetBuilder(MetricLabelInterner interner)
    {
        Interner        = interner;
        int id          = interner.Intern("service.name", out string key);
        _serviceNameKey = new InternedText(key, id);
    }

    /// <summary>The canonical string for these UTF-8 bytes, and its id.</summary>
    public InternedText Intern(ReadOnlySpan<byte> utf8)
    {
        int id = Interner.Intern(utf8, out string s);
        return new InternedText(s, id);
    }

    /// <summary>The canonical instance of <paramref name="s"/> (or <paramref name="s"/> itself), and its id.</summary>
    public InternedText Intern(string s)
    {
        int id = Interner.Intern(s, out string canonical);
        return new InternedText(canonical, id);
    }

    /// <summary>A new resource: nothing of the previous one's service name or labels carries over.</summary>
    public void BeginResource()
    {
        _serviceName = null;
        _resUsed     = 0;
        _resOverflow = false;
    }

    /// <summary>
    /// The resource's service name, stamped as <c>service.name</c> onto every point below it. A
    /// resource that states it twice keeps the FIRST — the rule for every other key a resource repeats
    /// (see <see cref="Build"/>). The two encodings used to disagree here, the JSON mapper keeping the
    /// first and the protobuf parser the last, so one exporter's series forked by encoding.
    /// </summary>
    public void SetServiceName(InternedText value) => _serviceName ??= value;

    /// <summary>Whether the resource already stated its service name — so a later statement is not even interned.</summary>
    public bool HasServiceName => _serviceName is not null;

    /// <summary>
    /// Whether the resource stated more than <see cref="MaxLabels"/> labels. Every point below it is
    /// refused (<see cref="Overflowed"/> from <see cref="BeginPoint"/> on), so a caller stops reading
    /// the resource's attributes here: nothing more of them is kept or interned.
    /// </summary>
    public bool ResourceOverflowed => _resOverflow;

    /// <summary>
    /// A resource label, stamped onto every point below it that does not carry the key itself. The
    /// one past <see cref="MaxLabels"/> is not kept: it sets <see cref="ResourceOverflowed"/> instead.
    /// </summary>
    public void AddResourceLabel(InternedText key, InternedText value)
    {
        if (_resOverflow) return;
        if (_resUsed >= 2 * MaxLabels)
        {
            _resOverflow = true;
            return;
        }
        Append(ref _resKv, ref _resIds, ref _resUsed, key, value);
    }

    /// <summary>
    /// A new data point: its labels start as the resource's service name, if any. A point under a
    /// resource whose labels alone — with the service name — pass <see cref="MaxLabels"/> is refused
    /// here, before its attributes are read: no attribute of its own can bring the merged set under the
    /// cap (an attribute adds a key or overwrites one), and <see cref="Build"/> would otherwise pay the
    /// whole merge before refusing it. The resource is counted as it was stated, repeats included, as
    /// <see cref="AddResourceLabel"/> counts it.
    /// </summary>
    public void BeginPoint()
    {
        _used     = 0;
        _overflow = _resOverflow || (_resUsed >> 1) + (_serviceName is null ? 0 : 1) > MaxLabels;
        if (_serviceName is { } service)
            Append(ref _kv, ref _ids, ref _used, _serviceNameKey, service);
    }

    /// <summary>
    /// Whether the point being built has more than <see cref="MaxLabels"/> labels (its own, the
    /// service name and the resource's merged in by <see cref="Build"/>) — or sits under a resource
    /// that has. Such a point is refused: <see cref="Add"/> keeps nothing more of it, <see cref="Build"/>
    /// answers <see cref="LabelSet.Empty"/>, and the caller counts it as rejected and stops reading its
    /// attributes. Read it after <see cref="Build"/>, and after any <see cref="Add"/> to stop early.
    /// </summary>
    public bool Overflowed => _overflow;

    /// <summary>
    /// One of the point's own attributes. <b>A key the point already carries is overwritten: the last
    /// value wins</b> — over an earlier attribute of the point, and over the resource's service name.
    ///
    /// <para>OTLP says an attribute key MUST be unique, and exporters break it anyway: an attribute
    /// set twice on the point, or <c>service.name</c> set on the point as well as the resource. A
    /// label set carrying a key twice is a series no answer can write (a JSON object cannot hold the
    /// key twice — every metrics response failed on it, #92), so none may reach storage. Last wins
    /// because that is what an OTel SDK's own attribute set does with a repeated key (Go's
    /// <c>attribute.NewSet</c>, a Java/.NET builder's later <c>put</c>) and what protobuf does with a
    /// repeated map key — and what <c>DedupeByTimestamp</c> does with a repeated timestamp here. A
    /// point attribute winning over the resource's service name is the rule every other resource
    /// label already followed (see <see cref="Build"/>).</para>
    ///
    /// <para>A linear scan, bounded by <see cref="MaxLabels"/>: a point carries a handful of labels,
    /// and the merge in <see cref="Build"/> already scans the same way per resource label. The
    /// attribute that would be the 257th DISTINCT key sets <see cref="Overflowed"/> and is not kept; a
    /// repeat of a key already present still overwrites, taking no slot.</para>
    /// </summary>
    public void Add(InternedText key, InternedText value)
    {
        if (_overflow) return;
        int at = IndexOfKey(key.Text);
        if (at < 0)
        {
            if (_used >= 2 * MaxLabels)
            {
                _overflow = true;
                return;
            }
            Append(ref _kv, ref _ids, ref _used, key, value);
            return;
        }
        _kv[at + 1]  = value.Text;
        _ids[at + 1] = value.Id;
    }

    /// <summary>
    /// The point's label set: what <see cref="BeginPoint"/> and <see cref="Add"/> gathered, then every
    /// resource label whose key is not already present — point attributes win on key collision. So
    /// no key is ever in the set twice. <see cref="LabelSet.Empty"/>, and <see cref="Overflowed"/>
    /// set, when the merged set would pass <see cref="MaxLabels"/>: the point is the caller's to refuse.
    /// </summary>
    public LabelSet Build()
    {
        if (_overflow) return LabelSet.Empty;

        var res = _resKv;
        for (int i = 0; i < _resUsed; i += 2)
        {
            // Against everything added so far, resource labels included: a resource that repeats a
            // key keeps its first value, as the pair-list shape always did. Deliberately NOT "last
            // wins" like a point's own attributes: that set was always storable, and changing its
            // rule would re-key the series of every exporter that sends one.
            if (IndexOfKey(res[i]) < 0)
            {
                if (_used >= 2 * MaxLabels)
                {
                    _overflow = true;
                    return LabelSet.Empty;
                }
                Append(ref _kv, ref _ids, ref _used,
                       new InternedText(res[i], _resIds[i]), new InternedText(res[i + 1], _resIds[i + 1]));
            }
        }

        return _used == 0
            ? LabelSet.Empty
            : Interner.GetLabelSet(_kv.AsSpan(0, _used), _ids.AsSpan(0, _used));
    }

    /// <summary>Where <paramref name="key"/> sits among the point's labels (an even index), or -1.</summary>
    private int IndexOfKey(string key)
    {
        var kv = _kv;
        for (int j = 0; j < _used; j += 2)
            if (kv[j] == key) return j;
        return -1;
    }

    private static void Append(ref string[] kv, ref int[] ids, ref int used, InternedText key, InternedText value)
    {
        if (used + 2 > kv.Length)
        {
            Array.Resize(ref kv,  kv.Length * 2);
            Array.Resize(ref ids, ids.Length * 2);
        }
        kv[used] = key.Text;   ids[used] = key.Id;   used++;
        kv[used] = value.Text; ids[used] = value.Id; used++;
    }
}
