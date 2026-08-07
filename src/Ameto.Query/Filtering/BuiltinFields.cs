using System.Collections.Frozen;
using Ameto.Core;

namespace Ameto.Query.Filtering;

/// <summary>
/// Canonical identity of a built-in (CLEF header) filter field, independent of which
/// of its spellings the user typed.
/// </summary>
public enum BuiltinField : byte
{
    Level,
    MessageTemplate,
    ExceptionType,
    ExceptionMessage,
    ExceptionStack,
    InnerExceptionType,
    InnerExceptionMessage,
    Timestamp,
    TraceId,
    SpanId,
    EventId,
    ServiceName,
}

/// <summary>
/// The one place that knows what a built-in field is called on each side of the query.
///
/// <para>Three facts live together per field, and they must, because separating them is
/// exactly how the index-hint bug class arose: which filter-side spellings mean the field
/// (<see cref="TryResolve"/>, used by <see cref="FilterEvaluator"/>), which inverted-index
/// bucket <c>SegmentIndexBuilder</c> writes it to (<see cref="InvertedKey"/>, used by
/// <see cref="CompiledFilter"/> when it builds index hints), and whether the field's VALUE
/// reaches the segment bloom filter (<see cref="IsBloomIndexed"/>).</para>
///
/// <para>A field whose <see cref="InvertedKey"/> is null is stored in no inverted bucket at
/// any spelling — <c>@mt</c>, <c>@m</c>, the exception message/stack, <c>@t</c> and
/// <c>@id</c>. Emitting a hint for one of those asks the index a question it cannot answer,
/// and the index's "no such bucket" answer used to be read as "no matching events", which
/// silently dropped every cold segment. Null means "emit no hint, scan instead".</para>
///
/// <para>Adding an alias here automatically teaches the evaluator to accept it AND teaches
/// the hint builder where it lives in the index, so the two cannot disagree by
/// construction.</para>
/// </summary>
public static class BuiltinFields
{
    /// <summary>
    /// <see cref="PropertyPath.Separator"/> as a string, so encoded paths can be written
    /// as compile-time constants below. Kept in lock-step by
    /// <c>BuiltinFieldTableTests.Separator_MatchesPropertyPath</c>.
    /// </summary>
    private const string Sep = "\u0001";

    private readonly record struct Entry(
        BuiltinField Field,
        string?      InvertedKey,
        bool         BloomIndexed,
        string[]     Aliases);

    /// <summary>
    /// Aliases are matched ordinally and case-sensitively, matching the C# switch this
    /// table replaced. Each field lists the <c>@</c>-form, the PascalCase form, the encoded
    /// path form the parser produces for a dotted spelling (<c>@x.type</c> →
    /// <c>@x</c> U+0001 <c>type</c>) and — for the exception sub-fields — the literal-dot form the
    /// bracket escape hatch produces (<c>['@x.type']</c>), which is also the index's own
    /// bucket name.
    /// </summary>
    private static readonly Entry[] Table =
    [
        new(BuiltinField.Level, ClefFields.Level, BloomIndexed: true,
            [ClefFields.Level, "Level"]),

        // Template reaches the trigram index and the bloom, but no inverted bucket.
        new(BuiltinField.MessageTemplate, null, BloomIndexed: true,
            [ClefFields.MessageTemplate, "MessageTemplate", ClefFields.Message, "Message"]),

        new(BuiltinField.ExceptionType, ClefFields.ExceptionType, BloomIndexed: true,
            [ClefFields.Exception, "Exception",
             "@x" + Sep + "type", "Exception" + Sep + "Type",
             ClefFields.ExceptionType]),

        // Trigram-indexed only — and not even bloomed, so a hint here is doubly fatal.
        new(BuiltinField.ExceptionMessage, null, BloomIndexed: false,
            ["@x" + Sep + "message", "Exception" + Sep + "Message", "@x.message"]),

        // Persisted but touched by no index structure at all.
        new(BuiltinField.ExceptionStack, null, BloomIndexed: false,
            ["@x" + Sep + "stack", "Exception" + Sep + "StackTrace", "@x.stack"]),

        new(BuiltinField.InnerExceptionType, ClefFields.ExceptionInnerType, BloomIndexed: true,
            ["@x" + Sep + "inner" + Sep + "type", "Exception" + Sep + "Inner" + Sep + "Type",
             ClefFields.ExceptionInnerType]),

        new(BuiltinField.InnerExceptionMessage, null, BloomIndexed: false,
            ["@x" + Sep + "inner" + Sep + "message", "Exception" + Sep + "Inner" + Sep + "Message",
             "@x.inner.message"]),

        // Time pruning is segment min/max only; @t reaches no index structure.
        new(BuiltinField.Timestamp, null, BloomIndexed: false,
            [ClefFields.Timestamp, "Timestamp"]),

        new(BuiltinField.TraceId, ClefFields.TraceId, BloomIndexed: true,
            [ClefFields.TraceId, "TraceId"]),

        new(BuiltinField.SpanId, ClefFields.SpanId, BloomIndexed: true,
            [ClefFields.SpanId, "SpanId"]),

        // The Snowflake event id is written to no index of any spelling.
        new(BuiltinField.EventId, null, BloomIndexed: false,
            ["@id", "Id"]),

        // "service.name" is one opaque key on both sides; the parser turns the dotted
        // spelling into an encoded path, so both forms are listed.
        new(BuiltinField.ServiceName, ClefFields.ServiceName, BloomIndexed: true,
            [ClefFields.ServiceName, "service" + Sep + "name"]),
    ];

    // Declaration order matters: static initialisers run top-to-bottom, and the two
    // builders below index by FieldCount.
    /// <summary>Number of distinct <see cref="BuiltinField"/> values — sizes the lookup arrays.</summary>
    private static readonly int FieldCount = Enum.GetValues<BuiltinField>().Length;

    private static readonly FrozenDictionary<string, BuiltinField> ByAlias = BuildAliasMap();
    private static readonly string?[] InvertedKeys = BuildInvertedKeys();
    private static readonly bool[]    BloomFlags   = BuildBloomFlags();

    private static FrozenDictionary<string, BuiltinField> BuildAliasMap()
    {
        var map = new Dictionary<string, BuiltinField>(48, StringComparer.Ordinal);
        foreach (var entry in Table)
            foreach (var alias in entry.Aliases)
                map[alias] = entry.Field;
        return map.ToFrozenDictionary(StringComparer.Ordinal);
    }

    private static string?[] BuildInvertedKeys()
    {
        var keys = new string?[FieldCount];
        foreach (var entry in Table) keys[(int)entry.Field] = entry.InvertedKey;
        return keys;
    }

    private static bool[] BuildBloomFlags()
    {
        var flags = new bool[FieldCount];
        foreach (var entry in Table) flags[(int)entry.Field] = entry.BloomIndexed;
        return flags;
    }

    /// <summary>
    /// Resolves a filter-side property name to its built-in field. Returns false for user
    /// properties. Allocation-free: a frozen-dictionary probe, called per event by the
    /// evaluator and once per predicate at compile time by the hint builder.
    /// </summary>
    public static bool TryResolve(string property, out BuiltinField field) =>
        ByAlias.TryGetValue(property, out field);

    /// <summary>
    /// The inverted-index bucket <c>SegmentIndexBuilder</c> writes this field to, or null
    /// when the field is stored in no bucket (⇒ emit no hint and let the scan decide).
    /// </summary>
    public static string? InvertedKey(BuiltinField field) => InvertedKeys[(int)field];

    /// <summary>True when the field's VALUE is added to the segment bloom filter.</summary>
    public static bool IsBloomIndexed(BuiltinField field) => BloomFlags[(int)field];

    /// <summary>Every recognised spelling — for tests that pin the filter/index contract.</summary>
    internal static IEnumerable<string> AllAliases()
    {
        foreach (var entry in Table)
            foreach (var alias in entry.Aliases)
                yield return alias;
    }
}
