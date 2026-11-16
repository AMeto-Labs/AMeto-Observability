using System.Buffers;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.RegularExpressions;
using Ameto.Core;
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
    /// <summary>Ordered so that the more protective of two matching rules is the greater.</summary>
    internal enum Decision : byte { None = 0, Keep = 1, KeepLast4 = 2, Hash = 3, Remove = 4 }

    private sealed record Rule(string Id, string Pattern, bool LeafToo, string[] Services, Decision Decision);

    /// <summary>Nesting past this is hidden whole rather than walked.</summary>
    private const int MaxDepth = 24;

    private readonly Rule[]        _rules;
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
    }

    // ── Decisions ─────────────────────────────────────────────────────────────

    /// <summary>
    /// What the rules say about a property path (<c>Customer.Iin</c>). With
    /// <paramref name="anyService"/> the service scope of a rule is ignored: the question is then
    /// "could this field be hidden in any event", which is what a filter or an aggregation over
    /// several services has to be judged by.
    /// </summary>
    internal Decision Decide(ReadOnlySpan<char> path, string? service, bool anyService = false) =>
        Decide(path, service, anyService, out _);

    /// <summary>
    /// The same, and which rule it was: the first of the most protective ones that match, by
    /// its id — what the count of a rule's own hits is kept under.
    /// </summary>
    internal Decision Decide(ReadOnlySpan<char> path, string? service, bool anyService, out string? ruleId)
    {
        Decision best = Decision.None;
        ruleId = null;
        int lastDot = path.LastIndexOf('.');
        ReadOnlySpan<char> leaf = lastDot < 0 ? default : path[(lastDot + 1)..];
        foreach (var rule in _rules)
        {
            if (rule.Decision <= best) continue;
            if (!anyService && rule.Services.Length > 0 && !InScope(rule.Services, service)) continue;
            if (Glob(rule.Pattern, path) || (rule.LeafToo && lastDot >= 0 && Glob(rule.Pattern, leaf)))
            {
                best   = rule.Decision;
                ruleId = rule.Id;
            }
        }
        return best;
    }

    /// <summary>
    /// True when a property's value never leaves as stored in SOME event the question could be
    /// about — hashed, cut or removed by a rule, or, in <see cref="McpMaskingMode.Strict"/>,
    /// simply not on the allow-list. A filter may test such a field for equality only, and an
    /// aggregation may only count it.
    ///
    /// <para>A filter is not tied to one service or to one spelling of a path, so this is the
    /// widest reading of the rules, not the reading an event of one service gets:</para>
    /// <list type="bullet">
    /// <item>A rule on a path hides everything under it. <c>Buyer</c> → hash sends the whole
    /// object as one token, so <c>Buyer.Name</c> is as hidden as <c>Buyer</c> is.</item>
    /// <item>A rule for one service counts for every service: the field is hidden somewhere.</item>
    /// <item>A <c>keep</c> for one service counts for none: the field is still hidden in the
    /// others, and a question about it is a question about those too.</item>
    /// </list>
    /// The path is a property path as a filter spells it; a built-in field (<c>@mt</c>) is the
    /// caller's to tell apart — none of them is ever hidden.
    /// </summary>
    public bool HidesField(string path) => HiddenAnywhere(path, out _, out _) != Decision.None;

    /// <summary>
    /// What is done, somewhere, to the value at a path: the most protective decision any rule of
    /// any service takes on the path or on anything above it — or, in Strict with no such rule,
    /// a hash unless a <c>keep</c> that holds in every service covers it.
    /// </summary>
    private Decision HiddenAnywhere(ReadOnlySpan<char> path, out string? ruleId, out bool keptEverywhere)
    {
        Decision best = OnPathOrAbove(path, out ruleId, out keptEverywhere);

        // `Items.0.Sku` is `Items[0].Sku` to the engine wherever Items is a list, and the rules
        // name a list's elements by the list's own path — `Items.Sku`. A path with a number in
        // it is therefore judged both ways, and has to pass both.
        if (WithoutNumbers(path) is { } elements)
        {
            Decision other = OnPathOrAbove(elements, out string? otherRule, out bool otherKept);
            keptEverywhere &= otherKept;
            if (other > best) { best = other; ruleId = otherRule; }
        }

        if (best != Decision.None) return best;
        return Mode == McpMaskingMode.Strict && !keptEverywhere ? Decision.Hash : Decision.None;
    }

    /// <summary>The most protective rule on a path or on any path above it — Buyer.Name, then Buyer.</summary>
    private Decision OnPathOrAbove(ReadOnlySpan<char> path, out string? ruleId, out bool keptEverywhere)
    {
        Decision best = Decision.None;
        keptEverywhere = false;
        ruleId = null;
        for (ReadOnlySpan<char> at = path; ; )
        {
            int lastDot = at.LastIndexOf('.');
            ReadOnlySpan<char> leaf = lastDot < 0 ? default : at[(lastDot + 1)..];
            foreach (var rule in _rules)
            {
                if (!Glob(rule.Pattern, at) && !(rule.LeafToo && lastDot >= 0 && Glob(rule.Pattern, leaf))) continue;
                if (rule.Decision == Decision.Keep) keptEverywhere |= rule.Services.Length == 0;
                else if (rule.Decision > best) { best = rule.Decision; ruleId = rule.Id; }
            }
            if (lastDot < 0) return best;
            at = at[..lastDot];
        }
    }

    /// <summary>
    /// The path with every all-digit segment after the first taken out, or null when it has none:
    /// <c>Items.0.Sku</c> → <c>Items.Sku</c>.
    /// </summary>
    private static string? WithoutNumbers(ReadOnlySpan<char> path)
    {
        StringBuilder? sb = null;
        int start = 0, kept = 0;
        for (int i = 0; i <= path.Length; i++)
        {
            if (i < path.Length && path[i] != '.') continue;
            ReadOnlySpan<char> segment = path[start..i];
            bool number = start > 0 && !segment.IsEmpty && !segment.ContainsAnyExceptInRange('0', '9');
            if (number) sb ??= new StringBuilder(path.Length).Append(path[..(start - 1)]);
            else if (sb is not null) { if (kept > 0) sb.Append('.'); sb.Append(segment); }
            if (!number) kept++;
            start = i + 1;
        }
        return sb?.ToString();
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
    public string? MaskGroupKey(string path, string? value, MaskStats stats)
    {
        if (value is null) return null;

        // A row of a table is not an event of one service, so the key is judged as a filter's
        // field is: by what is done to it anywhere, under any path above it.
        Decision decision = HiddenAnywhere(path, out string? rule, out bool kept);
        MaskReason reason = rule is null ? MaskReason.Strict : MaskReason.Rule;
        switch (decision)
        {
            case Decision.Remove:    stats.Add(reason, path, rule); return "(hidden)";
            case Decision.KeepLast4: stats.Add(reason, path, rule); return LastFour(value);
            case Decision.Hash:      stats.Add(reason, path, rule); return TokenOfText(value, isNumber: false);
        }
        return kept ? value : MaskText(value, stats);
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
    public string MaskNumber(string path, double value, MaskStats stats)
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
        HiddenAnywhere(path, out _, out bool kept);
        if (kept) return text;

        if (SensitiveValueDetectors.IsSensitiveNumber(whole, _detectors, out var kind))
        {
            stats.Add(MaskReason.Detector, SensitiveValueDetectors.IdOf(kind));
            return TokenOfText(text, isNumber: true);
        }
        return MaskText(text, stats);
    }

    // ── Events ────────────────────────────────────────────────────────────────

    /// <summary>Everything the walk of one event carries with it.</summary>
    private readonly record struct Ctx(ReadOnlyMemory<byte> Raw, string? Service, MaskStats Stats);

    /// <summary>
    /// <paramref name="ev"/> as the AI gets it. <paramref name="onlyFields"/> narrows the
    /// properties to the named top-level ones; the rules apply to whatever is left.
    /// </summary>
    public MaskedEvent Mask(LogEvent ev, MaskStats stats, IReadOnlySet<string>? onlyFields = null)
    {
        var props = new List<KeyValuePair<string, string>>();
        Properties(ev, stats, onlyFields, props, stored: null);

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
            Source     = ev,
            Template   = MaskText(ev.MessageTemplate, stats),
            Properties = props,
            Exception  = exception,
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
        LogEvent ev, MaskStats stats, IReadOnlySet<string>? onlyFields, List<KeyValuePair<string, string>> props, List<string>? stored)
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
            stored?.Add(key);
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
        Decision decision = Decide(path, c.Service, anyService: false, out string? rule);
        switch (decision)
        {
            case Decision.Remove:    r.Skip(); c.Stats.Add(MaskReason.Rule, path, rule); return null;
            case Decision.Hash:      return Hashed(ref r, path, in c, MaskReason.Rule, rule);
            case Decision.KeepLast4: return Cut(ref r, path, in c, rule);
        }

        bool keep = decision == Decision.Keep;
        switch (r.NextMessagePackType)
        {
            case MessagePackType.String:
                string s = r.ReadString() ?? "";
                return keep ? s : Loose(s, path, in c);

            case MessagePackType.Integer:
            case MessagePackType.Float:
                return LooseNumber(ref r, path, keep, in c, out _);

            case MessagePackType.Boolean: return r.ReadBoolean() ? "true" : "false";
            case MessagePackType.Nil:     r.ReadNil(); return "null";

            case MessagePackType.Map:
            case MessagePackType.Array:
            {
                var buffer = new ArrayBufferWriter<byte>(256);
                using (var w = new Utf8JsonWriter(buffer, JsonOptions))
                    WriteJson(w, ref r, path, keep, in c, depth: 1);
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

    /// <summary>A nested value as compact JSON, each property under it judged by its own path.</summary>
    private void WriteJson(Utf8JsonWriter w, ref MessagePackReader r, string path, bool keep, in Ctx c, int depth)
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
                    string child = string.Concat(path, ".", key);
                    Decision d   = Decide(child, c.Service, anyService: false, out string? rule);
                    if (d == Decision.Remove) { r.Skip(); c.Stats.Add(MaskReason.Rule, child, rule); continue; }

                    w.WritePropertyName(Name(key, c.Stats));
                    if (r.TryReadNil())            w.WriteNullValue();
                    else if (d == Decision.Hash)   w.WriteStringValue(Hashed(ref r, child, in c, MaskReason.Rule, rule));
                    else if (d == Decision.KeepLast4) w.WriteStringValue(Cut(ref r, child, in c, rule));
                    else WriteJson(w, ref r, child, keep || d == Decision.Keep, in c, depth + 1);
                }
                w.WriteEndObject();
                break;
            }

            case MessagePackType.Array when depth <= MaxDepth:
            {
                int count = r.ReadArrayHeader();
                w.WriteStartArray();
                for (int i = 0; i < count; i++) WriteJson(w, ref r, path, keep, in c, depth + 1);
                w.WriteEndArray();
                break;
            }

            case MessagePackType.Map:
            case MessagePackType.Array:
                // Too deep to walk: hidden whole, and counted as that rather than as a field's rule.
                w.WriteStringValue(Hashed(ref r, MaskStats.TooDeep, in c, MaskReason.Detector, ruleId: null));
                break;

            case MessagePackType.String:
                string s = r.ReadString() ?? "";
                w.WriteStringValue(keep ? s : Loose(s, path, in c));
                break;

            case MessagePackType.Integer:
            case MessagePackType.Float:
                string text = LooseNumber(ref r, path, keep, in c, out bool hidden);
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

    /// <summary>A string no rule names: hidden in Strict, looked through by the detectors otherwise.</summary>
    private string Loose(string s, string path, in Ctx c)
    {
        if (Mode != McpMaskingMode.Strict) return MaskText(s, c.Stats);
        c.Stats.Add(MaskReason.Strict, path);
        return TokenOfText(s, isNumber: false);
    }

    /// <summary>
    /// A number no rule names, as text. Hidden in Strict; otherwise hidden only when it is a
    /// card number or an IIN that happens to be stored as an integer.
    /// </summary>
    private string LooseNumber(ref MessagePackReader r, string path, bool keep, in Ctx c, out bool hidden)
    {
        hidden = false;
        string text = ReadNumber(ref r, out long? integer);
        if (keep) return text;

        if (Mode == McpMaskingMode.Strict)
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

    /// <summary>The last four characters of a scalar; anything else is hashed.</summary>
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
            default:
                return Hashed(ref r, path, in c, MaskReason.Rule, ruleId);
        }
    }

    private static string LastFour(string value) =>
        value.Length <= 4 ? "****" : string.Concat("****", value.AsSpan(value.Length - 4));

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
