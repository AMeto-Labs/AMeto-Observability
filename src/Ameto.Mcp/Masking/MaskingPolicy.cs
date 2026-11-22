using System.Buffers;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.RegularExpressions;
using Ameto.Core;
using Ameto.Query.Filtering;
using MessagePack;

namespace Ameto.Mcp.Masking;

/// <summary>What hid a value.</summary>
public enum MaskReason : byte
{
    /// <summary>A field rule: hash, keep the last four, or remove.</summary>
    Rule = 0,
    /// <summary><see cref="McpMaskingMode.Strict"/>: the field is not on the allow-list.</summary>
    Strict = 1,
    /// <summary>A value detector — or a text that was hidden whole because it could not be looked through.</summary>
    Detector = 2,
}

/// <summary>
/// What one answer hid, and what hid it — the audit record's breakdown. Three counts kept apart
/// rather than one keyed by a name, because the names are not from one namespace: a property
/// called <c>email</c> and the email detector would otherwise be one line.
/// </summary>
public sealed class MaskStats
{
    private static readonly IReadOnlyDictionary<string, int> None =
        System.Collections.Immutable.ImmutableDictionary<string, int>.Empty;

    private Dictionary<string, int>? _rules, _strict, _detectors, _ruleHits;

    public int Total { get; private set; }

    /// <summary>Values hidden by a field rule, by the path of the field they were in.</summary>
    public IReadOnlyDictionary<string, int> ByRule => _rules ?? None;

    /// <summary>Values hidden because the profile is in Strict mode, by field path.</summary>
    public IReadOnlyDictionary<string, int> ByStrict => _strict ?? None;

    /// <summary>
    /// Values hidden by a detector, by its id (<see cref="SensitiveValueDetectors.IdOf"/>), with
    /// <see cref="Unscannable"/> and <see cref="TooDeep"/> for what was hidden unread.
    /// </summary>
    public IReadOnlyDictionary<string, int> ByDetector => _detectors ?? None;

    /// <summary>The same values as <see cref="ByRule"/>, by the id of the rule that decided.</summary>
    public IReadOnlyDictionary<string, int> RuleHits => _ruleHits ?? None;

    /// <summary>A text one of the patterns took too long over: hidden whole.</summary>
    public const string Unscannable = "unscannable";

    /// <summary>A value nested past what the walk follows: hidden whole.</summary>
    public const string TooDeep = "tooDeep";

    public int Count(MaskReason reason)
    {
        int sum = 0;
        foreach (int n in (reason switch { MaskReason.Rule => ByRule, MaskReason.Strict => ByStrict, _ => ByDetector }).Values) sum += n;
        return sum;
    }

    /// <summary>
    /// One hidden value. <paramref name="what"/> is the field's path for a rule or Strict mode
    /// and the detector's id otherwise; <paramref name="ruleId"/> names the rule that decided.
    /// </summary>
    public void Add(MaskReason reason, string what, string? ruleId = null)
    {
        Total++;
        Bump(ref Of(reason), what, 1, FieldNames(reason));
        if (ruleId is not null) Bump(ref _ruleHits, ruleId, 1, StringComparer.Ordinal);
    }

    /// <summary>Adds what another count holds — an event's own, once the event is in the answer.</summary>
    public void Merge(MaskStats other)
    {
        Total += other.Total;
        Merge(ref _rules,     other._rules,     StringComparer.OrdinalIgnoreCase);
        Merge(ref _strict,    other._strict,    StringComparer.OrdinalIgnoreCase);
        Merge(ref _detectors, other._detectors, StringComparer.Ordinal);
        Merge(ref _ruleHits,  other._ruleHits,  StringComparer.Ordinal);
    }

    private ref Dictionary<string, int>? Of(MaskReason reason)
    {
        if (reason == MaskReason.Rule)   return ref _rules;
        if (reason == MaskReason.Strict) return ref _strict;
        return ref _detectors;
    }

    // A field is the same field whatever its case — the rules match that way — and an id is itself.
    private static StringComparer FieldNames(MaskReason reason) =>
        reason == MaskReason.Detector ? StringComparer.Ordinal : StringComparer.OrdinalIgnoreCase;

    private static void Bump(ref Dictionary<string, int>? counts, string key, int by, StringComparer comparer)
    {
        counts ??= new Dictionary<string, int>(comparer);
        counts[key] = counts.GetValueOrDefault(key) + by;
    }

    private static void Merge(ref Dictionary<string, int>? into, Dictionary<string, int>? from, StringComparer comparer)
    {
        if (from is null) return;
        foreach (var (key, count) in from) Bump(ref into, key, count, comparer);
    }
}

/// <summary>An exception as the AI gets it: the type as stored, the texts through the detectors.</summary>
public sealed record MaskedException(string Type, string? Message, string? Stack, MaskedException? Inner);

/// <summary>
/// One event as the AI gets it. The message is NOT rendered — the server never stores a rendered
/// message — so hiding a property hides it everywhere it would have appeared: the AI reads the
/// template and the properties side by side, as the event was written.
/// </summary>
public sealed class MaskedEvent
{
    public required LogEvent Source   { get; init; }
    public required string   Template { get; init; }

    /// <summary>Top-level properties in stored order; a nested value is compact JSON.</summary>
    public required List<KeyValuePair<string, string>> Properties { get; init; }

    /// <summary>
    /// The name each of <see cref="Properties"/> is stored under, in the same order. It is the
    /// name sent, except where the name itself held a value and was replaced by a token.
    /// </summary>
    public required List<string> StoredNames { get; init; }

    public MaskedException? Exception { get; init; }
}

/// <summary>
/// Decides what the AI may see of a value, and writes the value accordingly. Built once per
/// change of the rules and shared by every call; it holds no per-call state.
///
/// <para><b>Where it runs.</b> On the way out, over the events an answer returns: the store,
/// its indexes and the UI are untouched, and a rule added now applies to events stored last
/// year. The cost is per returned event, never per scanned one.</para>
///
/// <para><b>The token</b> is the first six bytes of HMAC-SHA256(secret, value). Keyed, because
/// the values worth hiding are the ones with little entropy: an unkeyed hash of a card number
/// or a phone number is undone by hashing every candidate, which for a 16-digit number with a
/// known prefix is minutes of work. Deterministic, because that is what makes a hidden value
/// still useful: the same customer is the same token in every answer, and the AI can count,
/// group and search by it (see <see cref="TokenVault"/>).</para>
/// </summary>
public sealed class MaskingPolicy
{
    /// <summary>
    /// What the rules do to a value. Ordered so that of two matching rules the greater decides:
    /// the more protective of two that hide, and of two <c>keep</c>s the one on the path itself.
    /// </summary>
    internal enum Decision : byte
    {
        /// <summary>No rule names the path or a path above it: the mode decides — the detectors in Standard, a hash in Strict.</summary>
        None = 0,

        /// <summary>
        /// A <c>keep</c> on a path above: Strict mode does not hide the value, and the detectors
        /// look through it as they do through a value no rule names in Standard mode.
        /// </summary>
        Open = 1,

        /// <summary>A <c>keep</c> on the path itself: sent as stored, the detectors leave it alone.</summary>
        Keep = 2,

        KeepLast4 = 3,
        Hash      = 4,
        Remove    = 5,
    }

    private sealed record Rule(string Id, string Pattern, bool LeafToo, string[] Services, Decision Decision);

    /// <summary>Nesting past this is hidden whole rather than walked.</summary>
    private const int MaxDepth = 24;

    private readonly Rule[]        _rules;

    /// <summary>
    /// The services the rules tell apart: each one some rule is limited to, and null for every
    /// service no rule names. An event of any service is judged as one of these is, so asking
    /// about each of them is asking about every service there is or will be.
    /// </summary>
    private readonly string?[]     _serviceKinds;
    private readonly MaskDetectors _detectors;
    private readonly byte[]        _secret;
    private readonly TokenVault    _vault;

    [ThreadStatic] private static List<SensitiveValueDetectors.Hit>? t_hits;

    public McpMaskingMode Mode { get; }

    public MaskingPolicy(McpMaskingMode mode, IReadOnlyList<McpMaskRule> rules, MaskDetectors detectors, byte[] secret, TokenVault vault)
    {
        Mode       = mode;
        _detectors = detectors;
        _secret    = secret;
        _vault     = vault;
        _rules     = [.. rules.Where(static r => !string.IsNullOrWhiteSpace(r.Field)).Select(static r => new Rule(
            r.Id,
            r.Field.Trim(),
            LeafToo:  !r.Field.Contains('.'),
            Services: [.. r.Services],
            Decision: r.Action switch
            {
                MaskAction.Remove    => Decision.Remove,
                MaskAction.KeepLast4 => Decision.KeepLast4,
                MaskAction.Keep      => Decision.Keep,
                _                    => Decision.Hash,
            }))];
        _serviceKinds = [null, .. _rules.SelectMany(static r => r.Services).Distinct(StringComparer.OrdinalIgnoreCase)];
    }

    // ── Decisions ─────────────────────────────────────────────────────────────

    /// <summary>
    /// What the rules do to the value at a path in an event of one service: the most protective
    /// rule in scope that names the path or a path above it.
    ///
    /// <para><b>This is the one definition.</b> The answer asks it of every value it writes
    /// (<see cref="Cell"/>, <see cref="WriteJson"/>), and everything that judges a QUESTION — a
    /// filter's field, a table's key, a token's comparison — asks it again, for every way the
    /// path can be read and every kind of service (<see cref="Judge"/>). They were two
    /// definitions once, and a value walked between them.</para>
    ///
    /// <para><b>"Above" is by the dots of the path as it is spelled</b>, whether a dot stands
    /// between an object and its property or inside one name. A rule on <c>client</c> covers
    /// the object <c>client</c>, <c>client.id</c> under it, and a property NAMED
    /// <c>client.id</c> — which is how OTLP stores every attribute, what a filter spells the
    /// same way, and what the engine reads either way. The answer used to look above only
    /// where it had walked down through an object: under a rule on <c>client</c> the attribute
    /// <c>client.id</c> went out as stored, while a filter on it was judged as one on a hidden
    /// field — and a token compared with it found its value in the open (the fourth review).</para>
    ///
    /// <para><b>A rule from above hides, or lets through; it does not cut, and it does not
    /// vouch.</b> <c>keepLast4</c> and <c>keep</c> are about the value at their own path.</para>
    /// <list type="bullet">
    /// <item>Of an object there are no last four characters, so under a <c>keepLast4</c> the
    /// object is hashed whole, and so is everything spelled under the rule's path: one answer
    /// for <c>Card: { Pan }</c> and for a property named <c>Card.Pan</c>, and the same for a
    /// table grouped by <c>Card.Pan</c> — which once showed the last four characters of what a
    /// search hid whole.</item>
    /// <item>A <c>keep</c> says of the value at its path that it is safe as stored. Of what is
    /// spelled under that path it says only that Strict mode is not to hide it
    /// (<see cref="Decision.Open"/>): the detectors go on looking through it. A <c>keep</c> on
    /// <c>card</c>, written for the property that holds a card's brand, sent
    /// <c>payment.card.number</c> as stored otherwise — and since a pattern without a dot is
    /// tried against the last segment at every level, any part of a dotted name would do (the
    /// fifth review). To send what is under a path as stored, a rule names it:
    /// <c>payment.card.number</c>, or <c>payment.*</c>.</item>
    /// </list>
    ///
    /// <para>Of several rules as protective as each other the nearest decides: the one on the
    /// path itself before one above it. Its id is what the count of a rule's own hits is kept
    /// under.</para>
    /// </summary>
    internal Decision Decide(ReadOnlySpan<char> path, string? service, out string? ruleId)
    {
        Decision best = Decision.None;
        ruleId = null;
        for (ReadOnlySpan<char> at = path; ; )
        {
            bool above  = at.Length < path.Length;
            int lastDot = at.LastIndexOf('.');
            ReadOnlySpan<char> leaf = lastDot < 0 ? default : at[(lastDot + 1)..];
            foreach (var rule in _rules)
            {
                Decision does = !above ? rule.Decision : rule.Decision switch
                {
                    Decision.KeepLast4 => Decision.Hash,
                    Decision.Keep      => Decision.Open,
                    _                  => rule.Decision,
                };
                if (does <= best) continue;
                if (rule.Services.Length > 0 && !InScope(rule.Services, service)) continue;
                if (Glob(rule.Pattern, at) || (rule.LeafToo && lastDot >= 0 && Glob(rule.Pattern, leaf)))
                {
                    best   = does;
                    ruleId = rule.Id;
                }
            }
            if (lastDot < 0) return best;
            at = at[..lastDot];
        }
    }

    /// <summary>
    /// The path of the property <paramref name="name"/> under <paramref name="path"/>: the two
    /// with a dot between them, whatever either holds — an empty name keeps its dot.
    ///
    /// <para>The one spelling of "under". The answer makes a path of it as it walks down an
    /// object, and a question's path is made of the same call, name by name
    /// (<c>FilterGuard.RulePath</c>): the two were spelled apart once, and a property under an
    /// empty name was <c>.Pin</c> to the answer and <c>Pin</c> to the guard.</para>
    /// </summary>
    internal static string Under(string path, ReadOnlySpan<char> name) => string.Concat(path.AsSpan(), ".", name);

    /// <summary>
    /// What the rules do to the value at a path over everything a question can be about: every
    /// way the path can be read (<see cref="Readings"/>) and every kind of service.
    /// </summary>
    /// <param name="Hides">
    /// The most protective thing done to it anywhere — cut, hashed or removed by a rule, or hashed
    /// by Strict mode. <see cref="Decision.None"/> when no rule and no mode hides it wherever it
    /// is read: it is the detectors' to look through, or a <c>keep</c>'s to send as stored.
    /// </param>
    /// <param name="RuleId">The rule that does it, when a rule does.</param>
    /// <param name="KeptEverywhere">
    /// A <c>keep</c> on the path itself decides it in every reading and every service: it is sent
    /// as stored wherever it is read. Not so under a <c>keep</c> on a path above
    /// (<see cref="Decision.Open"/>), where a detector may still hide it.
    /// </param>
    /// <param name="HiddenWholeEverywhere">Hashed or removed, in every reading and every service.</param>
    /// <param name="ShownSomewhere">
    /// Somewhere a rule shows it whatever it is: whole (a <c>keep</c> on the path itself), or its
    /// last four characters (<c>keepLast4</c>). A value let through from above is not shown in
    /// this sense — what a detector knows is hidden there as it is in a field no rule names.
    /// </param>
    /// <param name="LooseSomewhere">
    /// Somewhere it is the detectors' to decide: no rule names it in Standard mode, or a
    /// <c>keep</c> above it let it through. What they find in it is hidden there, whatever a rule
    /// shows of it elsewhere.
    /// </param>
    /// <param name="Unread">The path can be read more ways than are tried: everything above is then the answer on the safe side.</param>
    private readonly record struct Verdict(
        Decision Hides, string? RuleId, bool KeptEverywhere, bool HiddenWholeEverywhere, bool ShownSomewhere,
        bool LooseSomewhere = false, bool Unread = false);

    /// <summary>
    /// Asks <see cref="Decide"/> about every reading of <paramref name="path"/> in every kind of
    /// service among <paramref name="services"/> — the services a profile reads; null or empty
    /// for all of them, which the rules tell apart only as far as <see cref="_serviceKinds"/>.
    ///
    /// <para>A question is not tied to one event, so nothing here is "the" decision: what
    /// restricts a filter is the most protective one (<see cref="Verdict.Hides"/>), what lets an
    /// aggregate take every value is a keep that holds throughout, and what lets a token be
    /// compared is a hash or a remove that holds throughout.</para>
    /// </summary>
    private Verdict Judge(string path, IReadOnlyList<string>? services = null)
    {
        if (Readings(path) is not { } readings)
            return new Verdict(Decision.Hash, null, KeptEverywhere: false, HiddenWholeEverywhere: false, ShownSomewhere: true,
                               LooseSomewhere: true, Unread: true);

        IReadOnlyList<string?> kinds = _serviceKinds;
        if (services is { Count: > 0 }) kinds = services;
        bool strict = Mode == McpMaskingMode.Strict;
        Decision worst = Decision.None;
        string? ruleId = null;
        bool allKept = true, allHiddenWhole = true, shown = false, loose = false;
        foreach (string reading in readings)
        {
            foreach (string? service in kinds)
            {
                Decision d = Decide(reading, service, out string? rule);
                // What no rule names, Strict hashes: as much a part of what an answer does to
                // the value as a rule is (Loose, LooseNumber). What a keep above has let through
                // it does not: that stays Open, a field like any in Standard mode.
                if (d == Decision.None && strict) { d = Decision.Hash; rule = null; }

                if (d > Decision.Keep && (d > worst || (d == worst && ruleId is null && rule is not null))) { worst = d; ruleId = rule; }
                if (d != Decision.Keep) allKept = false;
                if (d is Decision.Keep or Decision.KeepLast4) shown = true;
                if (d is not (Decision.Hash or Decision.Remove)) allHiddenWhole = false;
                if (d is Decision.None or Decision.Open) loose = true;
            }
        }
        return new Verdict(worst, ruleId, allKept, allHiddenWhole, shown, loose);
    }

    /// <summary>More segments that read as positions than this, and a path is not read out every way (see <see cref="Readings"/>).</summary>
    private const int MaxPositions = 6;

    /// <summary>
    /// The paths the answer may judge the value at <paramref name="path"/> by.
    ///
    /// <para>The rules name a list's elements by the list's own path — <c>Items.Sku</c> — and
    /// the answer judges them so. A filter's path carries the position: <c>Items.0.Sku</c>. But
    /// a segment that reads as a position is one only where it meets a list; where it meets an
    /// object it is the name of a property (<c>"0"</c>), and the answer judges
    /// <c>Items.0.Sku</c>. Nothing in the path says which, and a path may hold several such
    /// segments, so each is taken both ways: 2^k readings for k of them.</para>
    ///
    /// <para>"Reads as a position" is the engine's own test, asked of the engine's own function
    /// (<see cref="PropertyPath.TryReadPosition"/>). A copy of the rule kept here — "a run of
    /// digits" — was narrower than what the engine accepts, and <c>Items[' 0'].Sku</c> walked
    /// between the two. The first segment is always a property's name. A dot inside a name
    /// splits the name here into more segments than it is, which only adds readings: none the
    /// engine has is left out.</para>
    ///
    /// <para>Null when the path holds more such segments than <see cref="MaxPositions"/>.</para>
    /// </summary>
    private static List<string>? Readings(string path)
    {
        string[] segments = path.Split('.');
        List<int>? positions = null;
        for (int i = 1; i < segments.Length; i++)
            if (PropertyPath.TryReadPosition(segments[i], out _)) (positions ??= []).Add(i);

        if (positions is null) return [path];
        if (positions.Count > MaxPositions) return null;

        var readings = new List<string>(1 << positions.Count);
        var sb = new StringBuilder(path.Length);
        for (int dropped = 0; dropped < 1 << positions.Count; dropped++)
        {
            sb.Clear();
            bool any = false;
            for (int i = 0, next = 0; i < segments.Length; i++)
            {
                if (next < positions.Count && positions[next] == i && (dropped & (1 << next++)) != 0) continue;
                if (any) sb.Append('.');
                sb.Append(segments[i]);
                any = true;
            }
            readings.Add(sb.ToString());
        }
        return readings;
    }

    /// <summary>
    /// True when a property's value never leaves as stored in SOME event the question could be
    /// about — hashed, cut or removed by a rule, or, in <see cref="McpMaskingMode.Strict"/>,
    /// simply not on the allow-list. A filter may test such a field for equality only, and an
    /// aggregation may only count it.
    ///
    /// <para>A filter is not tied to one service or to one reading of a path, so this is the
    /// widest reading of the rules, not the one an event gets (see <see cref="Judge"/>):</para>
    /// <list type="bullet">
    /// <item>A rule on a path hides everything spelled under it. <c>Buyer</c> → hash sends the
    /// whole object as one token, so <c>Buyer.Name</c> is as hidden as <c>Buyer</c> is.</item>
    /// <item>A rule for one service counts for every service the question reaches: the field
    /// is hidden somewhere.</item>
    /// <item>A <c>keep</c> for one service counts for none: the field is still hidden in the
    /// others, and a question about it is a question about those too.</item>
    /// </list>
    /// <para>The services a question reaches are the ones its profile reads
    /// (<paramref name="services"/>; null or empty for every service): the profile's scope is
    /// part of every filter that runs. A rule for a service the profile does not read hides
    /// nothing it can be shown, and restricts nothing it may ask — as it decides nothing about a
    /// token (<see cref="TokenMayMeet"/>).</para>
    ///
    /// The path is a property path as a filter spells it; a built-in field (<c>@mt</c>) is the
    /// caller's to tell apart — none of them is ever hidden.
    /// </summary>
    public bool HidesField(string path, IReadOnlyList<string>? services = null) => Judge(path, services).Hides != Decision.None;

    /// <summary>
    /// True when a <c>keep</c> on the path itself sends the field as it is stored in every
    /// service, however the path is read. A <c>keep</c> on a path above it does not make it so:
    /// what a detector knows is still hidden there.
    /// </summary>
    public bool KeepsEverywhere(string path, IReadOnlyList<string>? services = null) => Judge(path, services).KeptEverywhere;

    /// <summary>
    /// Whether a token for <paramref name="text"/> may be compared with the field at
    /// <paramref name="path"/> — the question a filter asks with <c>Field = 'h:…'</c> — by a
    /// profile that reads <paramref name="services"/> (null or empty: every service).
    ///
    /// <para>A token is a question about where a hidden value is, and the events it finds are
    /// shown, read again and added up by every call after it. Compared with a field that shows
    /// its values as they are stored — in any service the profile reads, in any reading of the
    /// path, whole or the last four characters — it would find the value lying in the open:
    /// <c>sum(Attempt) / count(Attempt) where Attempt = 'h:…'</c> is the value, and so is the
    /// next <c>logs_context</c> of an event it found. So a token may meet only a field whose
    /// every value is hidden whole wherever the profile can read it
    /// (<see cref="HidesEverywhere"/>), or — for a value a detector hides whole by its shape, a
    /// card number, an address — any field no <c>keep</c> or <c>keepLast4</c> rule shows there,
    /// because the value is then hidden whatever field it is in.</para>
    /// </summary>
    public bool TokenMayMeet(string path, string text, bool isNumber, IReadOnlyList<string>? services = null)
    {
        // A true or a false stands against nothing. Strict mode lets booleans pass, so a field
        // it alone hides shows them as they are; and an equality with the text of one finds
        // them. Its token — a table gives one, for `group by` a field Strict mode hides — would
        // read its value off the first such field. There are two of them: nothing is lost.
        if (!isNumber && IsBooleanText(text)) return false;

        Verdict field = Judge(path, services);
        return field.HiddenWholeEverywhere || (!field.ShownSomewhere && DetectorsHide(text, isNumber));
    }

    /// <summary>
    /// True when the field's values are hidden whole in every event a profile that reads
    /// <paramref name="services"/> can be shown: in each of those services — in every service,
    /// when the profile reads them all — a <c>hash</c> or a <c>remove</c> decides the path,
    /// however it is read, or the mode is Strict and no rule shows it. A rule limited to other
    /// services leaves the field open in these; a <c>keep</c> shows it as stored, and a
    /// <c>keepLast4</c> its last four characters — the whole of a short value.
    /// </summary>
    public bool HidesEverywhere(string path, IReadOnlyList<string>? services = null) =>
        Judge(path, services).HiddenWholeEverywhere;

    /// <summary>
    /// True when the detectors hide this value WHOLE wherever an equality could find it — as
    /// the engine's equality finds it:
    /// <list type="bullet">
    /// <item>a text in any case of its letters: texts are compared without regard to case, and
    /// an IBAN or a JWT is known by the case of its letters, so the same value in small letters
    /// would be shown as it is;</item>
    /// <item>the whole text, not a part: <c>card 4111… failed</c> under a hash rule, found in an
    /// open field, would be shown with only the card number gone;</item>
    /// <item>a number, in a numeric field, wherever the value reads as one: the engine compares
    /// a text with a number as numbers, and a number is hidden only when its digits are a card
    /// number or an IIN — <c>"+77015550123"</c> equals 77015550123, shown as it is, and
    /// <c>"050312501234"</c> equals 50312501234. Past 2^53 a double holds no whole number
    /// exactly: a property stored as one equals every literal that rounds to it, and the digits
    /// it is shown by are not the value's own.</item>
    /// </list>
    /// </summary>
    private bool DetectorsHide(string text, bool isNumber)
    {
        if (_detectors == MaskDetectors.None || string.IsNullOrEmpty(text)) return false;
        if (isNumber)
            return long.TryParse(text, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out long whole)
                && Math.Abs((double)whole) <= ExactIntegers
                && SensitiveValueDetectors.IsSensitiveNumber(whole, _detectors, out _);

        // The patterns either take letters of both cases or need the capitals; a value they
        // find whole in capitals and in small letters they find whole in any mix of the two.
        if (!FoundWhole(text) || !FoundWhole(text.ToUpperInvariant()) || !FoundWhole(text.ToLowerInvariant())) return false;

        // As the engine reads a text as a number: NumberStyles.Any in the invariant culture. It
        // was one reading, through a double (FilterEvaluator.ToDouble); the comparison that
        // became exact reads a whole number by Int128.TryParse and anything else by
        // double.TryParse, with the same styles (IndexValueForms.TryReadInteger and
        // TryReadDouble). The first reads no text the second does not, and as the same number up
        // to 2^53, so this one parse answers for both: a text that is no number to it equals no
        // number in a field.
        if (!double.TryParse(text, NumberStyles.Any, CultureInfo.InvariantCulture, out double asNumber)) return true;
        return asNumber == Math.Floor(asNumber) && Math.Abs(asNumber) <= ExactIntegers
            && SensitiveValueDetectors.IsSensitiveNumber((long)asNumber, _detectors, out _);
    }

    /// <summary>
    /// The text of a boolean as the engine's equality takes it: <c>true</c> or <c>false</c>, in
    /// any case of their letters — a stored true equals <c>'TRUE'</c>.
    /// </summary>
    private static bool IsBooleanText(string text) =>
        text.Equals("true", StringComparison.OrdinalIgnoreCase) || text.Equals("false", StringComparison.OrdinalIgnoreCase);

    /// <summary>Whether the detectors find the text as one value, from its first character to its last.</summary>
    private bool FoundWhole(string text)
    {
        var hits = new List<SensitiveValueDetectors.Hit>(1);
        try { SensitiveValueDetectors.Find(text, _detectors, hits); }
        catch (RegexMatchTimeoutException) { return false; }   // not known to be hidden: the token is not let near it
        return hits.Count == 1 && hits[0].Start == 0 && hits[0].Length == text.Length;
    }

    private static bool InScope(string[] services, string? service)
    {
        if (service is null) return false;
        foreach (var s in services)
            if (s.Equals(service, StringComparison.OrdinalIgnoreCase)) return true;
        return false;
    }

    /// <summary><c>*</c> is any run of characters; everything else matches itself, whatever its case.</summary>
    internal static bool Glob(ReadOnlySpan<char> pattern, ReadOnlySpan<char> text)
    {
        int p = 0, t = 0, star = -1, mark = 0;
        while (t < text.Length)
        {
            if (p < pattern.Length && pattern[p] == '*') { star = p++; mark = t; }
            else if (p < pattern.Length && char.ToUpperInvariant(pattern[p]) == char.ToUpperInvariant(text[t])) { p++; t++; }
            else if (star >= 0) { p = star + 1; t = ++mark; }
            else return false;
        }
        while (p < pattern.Length && pattern[p] == '*') p++;
        return p == pattern.Length;
    }

    // ── Tokens ────────────────────────────────────────────────────────────────

    private ulong TokenBits(ReadOnlySpan<byte> canonical)
    {
        Span<byte> mac = stackalloc byte[32];
        HMACSHA256.HashData(_secret, canonical, mac);
        return ((ulong)mac[0] << 40) | ((ulong)mac[1] << 32) | ((ulong)mac[2] << 24)
             | ((ulong)mac[3] << 16) | ((ulong)mac[4] << 8)  | mac[5];
    }

    /// <summary>
    /// The token of a value given as text. A number is hashed as its invariant text, so a
    /// merchant id stored as <c>6456116363</c> in one service and <c>"6456116363"</c> in another
    /// is one token. <paramref name="remember"/> false keeps it out of the vault: a value nobody
    /// could usefully search for.
    /// </summary>
    internal string TokenOfText(string text, bool isNumber, bool remember = true)
    {
        int max = Encoding.UTF8.GetMaxByteCount(text.Length);
        byte[]? rented = null;
        Span<byte> utf8 = max <= 512 ? stackalloc byte[512] : (rented = ArrayPool<byte>.Shared.Rent(max));
        try
        {
            ulong bits = TokenBits(utf8[..Encoding.UTF8.GetBytes(text, utf8)]);
            if (remember) _vault.Remember(bits, text, isNumber);
            return MaskToken.Format(bits);
        }
        finally
        {
            if (rented is not null) ArrayPool<byte>.Shared.Return(rented);
        }
    }

    // ── Texts ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// A text with every value the detectors find replaced by its token: the message template,
    /// an exception's message and stack, a string property no rule names.
    /// </summary>
    public string MaskText(string? text, MaskStats stats)
    {
        if (string.IsNullOrEmpty(text)) return text ?? "";
        if (_detectors == MaskDetectors.None) return text;

        var hits = t_hits ??= [];
        hits.Clear();
        try
        {
            SensitiveValueDetectors.Find(text, _detectors, hits);
        }
        catch (RegexMatchTimeoutException)
        {
            // A text that takes a pattern this long is not one to send on trust.
            stats.Add(MaskReason.Detector, MaskStats.Unscannable);
            return TokenOfText(text, isNumber: false, remember: false);
        }
        if (hits.Count == 0) return text;

        var sb = new StringBuilder(text.Length + 8);
        int pos = 0;
        foreach (var hit in hits)
        {
            sb.Append(text, pos, hit.Start - pos);
            sb.Append(TokenOfText(SensitiveValueDetectors.Normalize(hit.Kind, text.AsSpan(hit.Start, hit.Length)), isNumber: false));
            stats.Add(MaskReason.Detector, SensitiveValueDetectors.IdOf(hit.Kind));
            pos = hit.Start + hit.Length;
        }
        sb.Append(text, pos, text.Length - pos);
        return sb.ToString();
    }

    /// <summary>
    /// A group key of an aggregation, which is a property value like any other: <c>group by
    /// CardNumber</c> must not hand out the card numbers one row at a time. The path is a
    /// PROPERTY's — whatever its name begins with. Which keys are built-in fields is the
    /// caller's to know, from the engine's own table; guessing it from an at sign here would
    /// send a property called <c>@anything</c> past every rule.
    /// </summary>
    public string? MaskGroupKey(string path, string? value, MaskStats stats, IReadOnlyList<string>? services = null)
    {
        if (value is null) return null;

        // A row of a table is not an event of one service, so the key is judged as a filter's
        // field is: by what is done to it in any service the profile reads, under any path above it.
        Verdict field = Judge(path, services);
        if (field.Unread)
        {
            // More readings than are tried: hidden, and not as any rule's or Strict mode's doing.
            stats.Add(MaskReason.Detector, MaskStats.TooDeep);
            return TokenOfText(value, isNumber: false, remember: false);
        }
        string? rule = field.RuleId;
        MaskReason reason = rule is null ? MaskReason.Strict : MaskReason.Rule;
        switch (field.Hides)
        {
            case Decision.Remove:    stats.Add(reason, path, rule); return "(hidden)";
            case Decision.Hash:      stats.Add(reason, path, rule); return TokenOfText(value, isNumber: false);
            case Decision.KeepLast4:
                stats.Add(reason, path, rule);
                // Its last four characters, where every event the row can be made of shows as
                // much. Where one of them leaves the value to the detectors — a service the rule
                // does not name, another reading of the path — and they find something in it, a
                // search there shows less than that: `group by Pan`, under a keepLast4 for one
                // service, gave the last four digits of the cards of all the others.
                return field.LooseSomewhere && DetectorsFindIn(value) ? TokenOfText(value, isNumber: false) : LastFour(value);
        }
        return field.KeptEverywhere ? value : MaskText(value, stats);
    }

    /// <summary>Whether the detectors would hide anything of the text. A text they cannot get through counts: it is not known to be clean.</summary>
    private bool DetectorsFindIn(string text)
    {
        if (_detectors == MaskDetectors.None || text.Length == 0) return false;
        var hits = new List<SensitiveValueDetectors.Hit>(1);
        try { SensitiveValueDetectors.Find(text, _detectors, hits); }
        catch (RegexMatchTimeoutException) { return true; }
        return hits.Count > 0;
    }

    /// <summary>The largest whole number a double holds exactly: past it, digits are not the value's own.</summary>
    private const double ExactIntegers = 9_007_199_254_740_992d;

    /// <summary>
    /// A number an aggregate came to — a minimum, a maximum, a sum, an average — as the cell of
    /// a table. <c>max(ClientIin)</c> is somebody's IIN and <c>min(Pan)</c> a card number, and a
    /// sum or an average over a group of one event is that event's value: so the number goes
    /// past the same detectors a value does in a search, as a number and as the digits it is
    /// written with. A field that a rule keeps in every service is sent as it is.
    /// </summary>
    public string MaskNumber(string path, double value, MaskStats stats, IReadOnlyList<string>? services = null)
    {
        if (value != Math.Floor(value) || double.IsInfinity(value))
            return value.ToString("0.###", CultureInfo.InvariantCulture);

        if (Math.Abs(value) > ExactIntegers)
        {
            // Its leading digits are right and its last ones are not: most of a long identifier,
            // shown as if it were a measurement. Not shown at all.
            stats.Add(MaskReason.Detector, MaskStats.Unscannable);
            return "(hidden: too long to show exactly)";
        }

        long whole  = (long)value;
        string text = whole.ToString(CultureInfo.InvariantCulture);
        if (KeepsEverywhere(path, services)) return text;

        if (SensitiveValueDetectors.IsSensitiveNumber(whole, _detectors, out var kind))
        {
            stats.Add(MaskReason.Detector, SensitiveValueDetectors.IdOf(kind));
            return TokenOfText(text, isNumber: true);
        }
        return MaskText(text, stats);
    }

    /// <summary>
    /// Whether <c>sum</c>, <c>min</c>, <c>max</c> or <c>avg</c> may take a stored value — asked
    /// of each one by the aggregation, for a field no rule names.
    ///
    /// <para>Not a text: a string that reads as a number is an identifier more often than a
    /// measurement, and comes back with its spelling gone. And not a number a search would hide
    /// — a card number, an IIN. Hiding the RESULT is not enough for those: the sum of two IINs
    /// is no IIN, and three sums of pairs are three equations in three unknowns. A value that
    /// is left out is counted in <paramref name="leftOut"/>, by the detector that knows it.</para>
    /// </summary>
    public bool AdmitsToAggregate(object value, MaskStats? leftOut)
    {
        long whole;
        switch (value)
        {
            case string:  return false;
            case long l:  whole = l; break;
            case int i:   whole = i; break;
            case ulong u when u <= long.MaxValue: whole = (long)u; break;
            case uint ui: whole = ui; break;
            case double d when d == Math.Floor(d) && Math.Abs(d) <= ExactIntegers: whole = (long)d; break;
            case float f when f == MathF.Floor(f) && MathF.Abs(f) <= 16_777_216f:  whole = (long)f; break;
            case decimal m when m == decimal.Truncate(m) && Math.Abs(m) <= long.MaxValue: whole = (long)m; break;
            default:      return true;    // a fraction, a boolean: nothing a detector knows by its digits
        }
        if (!SensitiveValueDetectors.IsSensitiveNumber(whole, _detectors, out var kind)) return true;
        leftOut?.Add(MaskReason.Detector, SensitiveValueDetectors.IdOf(kind));
        return false;
    }

    // ── Events ────────────────────────────────────────────────────────────────

    /// <summary>Everything the walk of one event carries with it.</summary>
    private readonly record struct Ctx(ReadOnlyMemory<byte> Raw, string? Service, MaskStats Stats);

    /// <summary>
    /// <paramref name="ev"/> as the AI gets it. <paramref name="onlyFields"/> narrows the
    /// properties to the named top-level ones; the rules apply to whatever is left.
    ///
    /// <para>An event is masked by the rules alone, never by the question that found it. An
    /// answer that showed an open field differently because the filter named a token — as that
    /// token, say — would tell which open value the hidden one is, and the next call, which
    /// shows the field as stored, would read it.</para>
    /// </summary>
    public MaskedEvent Mask(LogEvent ev, MaskStats stats, IReadOnlySet<string>? onlyFields = null)
    {
        var props  = new List<KeyValuePair<string, string>>();
        var stored = new List<string>();
        Properties(ev, stats, onlyFields, props, stored);

        MaskedException? exception = null;
        if (ev.HasException)
        {
            // The tree is built here, for a returned row only. A payload that cannot be read is
            // said to be so rather than failing the whole answer over one row.
            try { exception = ev.Exception is { } x ? MaskException(x, stats, depth: 1) : null; }
            catch (Exception e) when (e is MessagePackSerializationException or InvalidOperationException or EndOfStreamException)
            {
                exception = new MaskedException("(unreadable exception)", null, null, null);
            }
        }

        return new MaskedEvent
        {
            Source      = ev,
            Template    = MaskText(ev.MessageTemplate, stats),
            Properties  = props,
            StoredNames = stored,
            Exception   = exception,
        };
    }

    /// <summary>
    /// The properties of <paramref name="ev"/> alone, as the AI gets them — no template, no
    /// exception — and beside each the name it is stored under, which is the name
    /// <paramref name="onlyFields"/> speaks of and can differ from the name that is sent
    /// (see <see cref="Name"/>). For an answer that shows properties and nothing else of an event.
    /// </summary>
    public (List<KeyValuePair<string, string>> Properties, List<string> StoredNames) MaskProperties(
        LogEvent ev, MaskStats stats, IReadOnlySet<string>? onlyFields = null)
    {
        var props  = new List<KeyValuePair<string, string>>();
        var stored = new List<string>();
        Properties(ev, stats, onlyFields, props, stored);
        return (props, stored);
    }

    private void Properties(
        LogEvent ev, MaskStats stats, IReadOnlySet<string>? onlyFields, List<KeyValuePair<string, string>> props, List<string> stored)
    {
        ReadOnlyMemory<byte> raw = ev.RawProperties;
        if (raw.IsEmpty && ev.Properties is { Count: > 0 } map) raw = PropertyBytes.From(map);
        if (raw.IsEmpty) return;

        var reader = new MessagePackReader(raw);
        if (reader.NextMessagePackType != MessagePackType.Map) return;

        var ctx = new Ctx(raw, ev.ServiceName, stats);
        int count = reader.ReadMapHeader();
        for (int i = 0; i < count; i++)
        {
            string key = ReadKey(ref reader);
            if (onlyFields is not null && !onlyFields.Contains(key)) { reader.Skip(); continue; }
            if (Cell(ref reader, key, in ctx) is not { } cell) continue;
            props.Add(new(Name(key, stats), cell));
            stored.Add(key);
        }
    }

    private MaskedException MaskException(ExceptionInfo x, MaskStats stats, int depth) => new(
        x.Type,
        x.Message    is null ? null : MaskText(x.Message, stats),
        x.StackTrace is null ? null : MaskText(x.StackTrace, stats),
        x.Inner is { } inner && depth < ExceptionInfo.MaxDepth ? MaskException(inner, stats, depth + 1) : null);

    /// <summary>The value of one top-level property as cell text; null when a rule removes it.</summary>
    private string? Cell(ref MessagePackReader r, string path, in Ctx c)
    {
        Decision decision = Decide(path, c.Service, out string? rule);
        switch (decision)
        {
            case Decision.Remove:    r.Skip(); c.Stats.Add(MaskReason.Rule, path, rule); return null;
            case Decision.Hash:      return Hashed(ref r, path, in c, MaskReason.Rule, rule);
            case Decision.KeepLast4:
            {
                if (r.NextMessagePackType != MessagePackType.Array) return Cut(ref r, path, in c, rule);
                var cut = new ArrayBufferWriter<byte>(256);
                using (var w = new Utf8JsonWriter(cut, JsonOptions))
                    WriteCut(w, ref r, path, in c, rule, depth: 1);
                return Encoding.UTF8.GetString(cut.WrittenSpan);
            }
        }

        // Not hidden by a rule: kept, let through from above, or named by none.
        switch (r.NextMessagePackType)
        {
            case MessagePackType.String:
                return Loose(r.ReadString() ?? "", path, decision, in c);

            case MessagePackType.Integer:
            case MessagePackType.Float:
                return LooseNumber(ref r, path, decision, in c, out _);

            case MessagePackType.Boolean: return r.ReadBoolean() ? "true" : "false";
            case MessagePackType.Nil:     r.ReadNil(); return "null";

            case MessagePackType.Map:
            case MessagePackType.Array:
            {
                var buffer = new ArrayBufferWriter<byte>(256);
                using (var w = new Utf8JsonWriter(buffer, JsonOptions))
                    WriteJson(w, ref r, path, decision, in c, depth: 1);
                return Encoding.UTF8.GetString(buffer.WrittenSpan);
            }

            default: r.Skip(); return "null";
        }
    }

    private static readonly JsonWriterOptions JsonOptions = new()
    {
        // Text outside ASCII goes out as itself: an escape per Cyrillic letter is six bytes the
        // reader pays for and gains nothing from.
        Encoder        = JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
        SkipValidation = true,
    };

    /// <summary>
    /// A nested value as compact JSON, each property under it judged by its own path.
    /// <paramref name="decision"/> is what <see cref="Decide"/> said of <paramref name="path"/>,
    /// when it hid nothing: kept, let through from above, or named by no rule. It is the value's
    /// own and the elements' of a list at that path — the rules name those by the list's path —
    /// and nobody else's: a property under the path is asked about again, by its own path, and
    /// what stands above it is <see cref="Decide"/>'s to find, not the walk's to carry down.
    /// </summary>
    private void WriteJson(Utf8JsonWriter w, ref MessagePackReader r, string path, Decision decision, in Ctx c, int depth)
    {
        switch (r.NextMessagePackType)
        {
            case MessagePackType.Map when depth <= MaxDepth:
            {
                int count = r.ReadMapHeader();
                w.WriteStartObject();
                for (int i = 0; i < count; i++)
                {
                    string key   = ReadKey(ref r);
                    string child = Under(path, key);
                    Decision d   = Decide(child, c.Service, out string? rule);
                    if (d == Decision.Remove) { r.Skip(); c.Stats.Add(MaskReason.Rule, child, rule); continue; }

                    w.WritePropertyName(Name(key, c.Stats));
                    if (r.TryReadNil())            w.WriteNullValue();
                    else if (d == Decision.Hash)   w.WriteStringValue(Hashed(ref r, child, in c, MaskReason.Rule, rule));
                    else if (d == Decision.KeepLast4) WriteCut(w, ref r, child, in c, rule, depth + 1);
                    else WriteJson(w, ref r, child, d, in c, depth + 1);
                }
                w.WriteEndObject();
                break;
            }

            case MessagePackType.Array when depth <= MaxDepth:
            {
                int count = r.ReadArrayHeader();
                w.WriteStartArray();
                for (int i = 0; i < count; i++) WriteJson(w, ref r, path, decision, in c, depth + 1);
                w.WriteEndArray();
                break;
            }

            case MessagePackType.Map:
            case MessagePackType.Array:
                // Too deep to walk: hidden whole, and counted as that rather than as a field's rule.
                w.WriteStringValue(Hashed(ref r, MaskStats.TooDeep, in c, MaskReason.Detector, ruleId: null));
                break;

            case MessagePackType.String:
                w.WriteStringValue(Loose(r.ReadString() ?? "", path, decision, in c));
                break;

            case MessagePackType.Integer:
            case MessagePackType.Float:
                string text = LooseNumber(ref r, path, decision, in c, out bool hidden);
                if (hidden || !double.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out double d0) || !double.IsFinite(d0))
                    w.WriteStringValue(text);
                else
                    w.WriteRawValue(text, skipInputValidation: true);
                break;

            case MessagePackType.Boolean: w.WriteBooleanValue(r.ReadBoolean()); break;
            case MessagePackType.Nil:     r.ReadNil(); w.WriteNullValue(); break;
            default:                      r.Skip(); w.WriteNullValue(); break;
        }
    }

    /// <summary>
    /// A string no rule hides. Under a <c>keep</c> on its own path: as stored. Named by no rule
    /// in Strict mode: hidden. Otherwise — Standard mode, or let through by a <c>keep</c> above
    /// it — looked through by the detectors.
    /// </summary>
    private string Loose(string s, string path, Decision decision, in Ctx c)
    {
        if (decision == Decision.Keep) return s;
        if (decision == Decision.Open || Mode != McpMaskingMode.Strict) return MaskText(s, c.Stats);
        c.Stats.Add(MaskReason.Strict, path);
        return TokenOfText(s, isNumber: false);
    }

    /// <summary>
    /// A number no rule hides, as text, by the same three roads as <see cref="Loose"/>: as stored
    /// under a <c>keep</c> on its own path, hidden where Strict mode has it and no rule names it,
    /// and otherwise hidden only when it is a card number or an IIN that happens to be stored as
    /// a number.
    /// </summary>
    private string LooseNumber(ref MessagePackReader r, string path, Decision decision, in Ctx c, out bool hidden)
    {
        hidden = false;
        string text = ReadNumber(ref r, out long? integer);
        if (decision == Decision.Keep) return text;

        if (decision != Decision.Open && Mode == McpMaskingMode.Strict)
        {
            hidden = true;
            c.Stats.Add(MaskReason.Strict, path);
            return TokenOfText(text, isNumber: true);
        }
        if (integer is { } n && SensitiveValueDetectors.IsSensitiveNumber(n, _detectors, out var kind))
        {
            hidden = true;
            c.Stats.Add(MaskReason.Detector, SensitiveValueDetectors.IdOf(kind));
            return TokenOfText(text, isNumber: true);
        }
        return text;
    }

    /// <summary>
    /// The token of the value at the reader, whatever its shape. <paramref name="what"/> and
    /// <paramref name="reason"/> are what the value is counted under: the field's path and the
    /// rule that named it, or — for a value hidden unread — <see cref="MaskStats.TooDeep"/>.
    /// </summary>
    private string Hashed(ref MessagePackReader r, string what, in Ctx c, MaskReason reason, string? ruleId)
    {
        switch (r.NextMessagePackType)
        {
            case MessagePackType.Nil:
                r.ReadNil();
                return "null";   // an absent value hides nothing

            case MessagePackType.String:
                c.Stats.Add(reason, what, ruleId);
                return TokenOfText(r.ReadString() ?? "", isNumber: false);

            case MessagePackType.Integer:
            case MessagePackType.Float:
                c.Stats.Add(reason, what, ruleId);
                return TokenOfText(ReadNumber(ref r, out _), isNumber: true);

            case MessagePackType.Boolean:
                c.Stats.Add(reason, what, ruleId);
                return TokenOfText(r.ReadBoolean() ? "true" : "false", isNumber: false, remember: false);

            default:
            {
                // A map, an array, bytes: one token for the value as it was stored.
                c.Stats.Add(reason, what, ruleId);
                int start = (int)r.Consumed;
                r.Skip();
                return MaskToken.Format(TokenBits(c.Raw.Span[start..(int)r.Consumed]));
            }
        }
    }

    /// <summary>
    /// What a <c>keepLast4</c> on a value's own path sends of it, as JSON: of a list, each of its
    /// elements cut — the rules name those by the list's path, and so does a table, whose
    /// <c>group by Cards[0]</c> is judged by <c>Cards</c> and cut to its last four
    /// (<see cref="MaskGroupKey"/>). While the list was hashed whole here, that table showed four
    /// characters of every element a search showed none of (the sixth review).
    /// </summary>
    private void WriteCut(Utf8JsonWriter w, ref MessagePackReader r, string path, in Ctx c, string? ruleId, int depth)
    {
        switch (r.NextMessagePackType)
        {
            case MessagePackType.Array when depth <= MaxDepth:
            {
                int count = r.ReadArrayHeader();
                w.WriteStartArray();
                for (int i = 0; i < count; i++) WriteCut(w, ref r, path, in c, ruleId, depth + 1);
                w.WriteEndArray();
                break;
            }

            case MessagePackType.Nil: r.ReadNil(); w.WriteNullValue(); break;
            default:                  w.WriteStringValue(Cut(ref r, path, in c, ruleId)); break;
        }
    }

    /// <summary>
    /// A scalar cut as it is written (<see cref="LastFour"/>) — a text, a number, a true or a
    /// false: whatever the engine makes a table's key of, by the same text and the same cut, so
    /// that the key and this agree by construction. A true and a false are shorter than anything
    /// four characters are shown of, and go out as the mark alone, here and there. Anything else
    /// — a map, bytes, a list too deep to walk — has no last four characters and is hashed whole.
    /// </summary>
    private string Cut(ref MessagePackReader r, string path, in Ctx c, string? ruleId)
    {
        switch (r.NextMessagePackType)
        {
            case MessagePackType.String:
                c.Stats.Add(MaskReason.Rule, path, ruleId);
                return LastFour(r.ReadString() ?? "");
            case MessagePackType.Integer:
            case MessagePackType.Float:
                c.Stats.Add(MaskReason.Rule, path, ruleId);
                return LastFour(ReadNumber(ref r, out _));
            case MessagePackType.Boolean:
                c.Stats.Add(MaskReason.Rule, path, ruleId);
                return LastFour(r.ReadBoolean() ? "true" : "false");
            default:
                return Hashed(ref r, path, in c, MaskReason.Rule, ruleId);
        }
    }

    /// <summary>
    /// The shortest value a <c>keepLast4</c> shows four characters of. Of a shorter one they are
    /// the value or most of it — <c>false</c> was <c>****alse</c>, a one-time code
    /// <c>123456</c> was <c>****3456</c> — and it goes out as the mark alone. A card number, a
    /// phone number, an IIN, an IBAN are all longer.
    /// </summary>
    internal const int ShortestCut = 8;

    /// <summary>
    /// What a <c>keepLast4</c> leaves of a value as it is written: the mark and its last four
    /// characters, or the mark alone of a value too short for four characters to be a part of
    /// it. The one cut there is: the answer's and a table's key's (<see cref="MaskGroupKey"/>).
    /// </summary>
    private static string LastFour(string value) =>
        value.Length < ShortestCut ? "****" : string.Concat("****", value.AsSpan(value.Length - 4));

    /// <summary>
    /// A property's name as it is sent. A name is a name nearly always — but a dictionary keyed
    /// by an address or a card number has its values where its names should be, so a name that
    /// could hold one goes past the detectors like any other text. The rules still judge the
    /// name as stored.
    /// </summary>
    private string Name(string name, MaskStats stats) => CouldHoldAValue(name) ? MaskText(name, stats) : name;

    /// <summary>
    /// The cheap question before the detectors: an at sign, the start of a JWT, or six digits.
    /// Nearly every name fails all three, and is sent without a pattern being run over it.
    /// </summary>
    private static bool CouldHoldAValue(string name)
    {
        if (name.Length < 6) return false;
        if (name.AsSpan(1).Contains('@') || name.Contains("eyJ", StringComparison.Ordinal)) return true;
        int digits = 0;
        foreach (char c in name)
            if (char.IsAsciiDigit(c) && ++digits == 6) return true;
        return false;
    }

    private static string ReadNumber(ref MessagePackReader r, out long? integer)
    {
        integer = null;
        if (r.NextMessagePackType == MessagePackType.Float)
        {
            // A whole number that happens to be stored as a double is the number it is: the same
            // digits, the same token, and a card number all the same. Only up to where a double
            // still holds every digit.
            double d = r.ReadDouble();
            if (d == Math.Floor(d) && Math.Abs(d) <= ExactIntegers)
            {
                integer = (long)d;
                return integer.Value.ToString(CultureInfo.InvariantCulture);
            }
            return d.ToString("R", CultureInfo.InvariantCulture);
        }

        if (r.NextCode == MessagePackCode.UInt64)
        {
            ulong u = r.ReadUInt64();
            if (u <= long.MaxValue) integer = (long)u;
            return u.ToString(CultureInfo.InvariantCulture);
        }
        long l = r.ReadInt64();
        integer = l;
        return l.ToString(CultureInfo.InvariantCulture);
    }

    private static string ReadKey(ref MessagePackReader r)
    {
        if (r.NextMessagePackType == MessagePackType.String) return r.ReadString() ?? "";
        // MessagePack allows a key that is not a string; the dictionary road stringifies it.
        switch (r.NextMessagePackType)
        {
            case MessagePackType.Integer: return r.ReadInt64().ToString(CultureInfo.InvariantCulture);
            case MessagePackType.Boolean: return r.ReadBoolean() ? "True" : "False";
            case MessagePackType.Float:   return r.ReadDouble().ToString(CultureInfo.InvariantCulture);
            default:                      r.Skip(); return "";
        }
    }
}

/// <summary>A property map as MessagePack, for the events that carry a dictionary and no bytes.</summary>
internal static class PropertyBytes
{
    public static byte[] From(IReadOnlyDictionary<string, object?> map)
    {
        var buffer = new ArrayBufferWriter<byte>(256);
        var writer = new MessagePackWriter(buffer);
        WriteMap(ref writer, map);
        writer.Flush();
        return buffer.WrittenSpan.ToArray();
    }

    private static void WriteMap(ref MessagePackWriter w, IReadOnlyDictionary<string, object?> map)
    {
        w.WriteMapHeader(map.Count);
        foreach (var (key, value) in map)
        {
            w.Write(key);
            WriteValue(ref w, value);
        }
    }

    private static void WriteValue(ref MessagePackWriter w, object? value)
    {
        switch (value)
        {
            case null:      w.WriteNil(); break;
            case string s:  w.Write(s); break;
            case bool b:    w.Write(b); break;
            case int i:     w.Write(i); break;
            case long l:    w.Write(l); break;
            case uint ui:   w.Write(ui); break;
            case ulong ul:  w.Write(ul); break;
            case double d:  w.Write(d); break;
            case float f:   w.Write(f); break;
            case decimal m: w.Write((double)m); break;
            case IReadOnlyDictionary<string, object?> nested: WriteMap(ref w, nested); break;
            case System.Collections.IEnumerable items:
            {
                var list = items.Cast<object?>().ToList();
                w.WriteArrayHeader(list.Count);
                foreach (var item in list) WriteValue(ref w, item);
                break;
            }
            default: w.Write(value.ToString()); break;
        }
    }
}
