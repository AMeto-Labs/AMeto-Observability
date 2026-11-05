using System.Buffers;
using System.Text;
using System.Text.Json;
using Ameto.Core;
using MessagePack;
using Xunit;
using static Ameto.Testing.ExceptionShapes;

namespace Ameto.Core.Tests;

/// <summary>
/// THE BYTES ARE WHAT IS STORED. An event's <c>@x</c> is kept as it arrived — in the batch, the
/// tier, the WAL entry and the segment column — and every consumer reads the bytes where it used
/// to read the tree. That holds only if each byte-level reader answers exactly as the tree does,
/// over every shape the wire accepts, so these tests pin each one against <see cref="ExceptionInfo.Read"/>:
/// <see cref="ExceptionInfo.Validate"/> (what ingest accepts, refuses and reads as none),
/// <see cref="ExceptionInfo.ReadField"/> (what a filter reads per row),
/// <see cref="ExceptionInfo.TryReadIndexFields"/> (what the index files per row) and the JSON writer
/// (what a client receives), over the canonical shape and the odd ones — keys out of order, unknown
/// keys, repeated keys (to nil included), nil fields, empty strings, a chain deeper than
/// <see cref="ExceptionInfo.MaxDepth"/>, a legacy plain string, invalid UTF-8
/// (<see cref="Ameto.Testing.ExceptionShapes"/>, shared with the index parity tests).
/// </summary>
public sealed class ExceptionBytesContractTests
{
    /// <summary>Every shape a reader must agree on, by name (<see cref="ExceptionShapes.Payloads"/>).</summary>
    public static TheoryData<string> Shapes() => [.. Payloads.Keys];

    // ── Validate: what ingest keeps, drops and refuses ─────────────────────

    [Theory]
    [MemberData(nameof(Shapes))]
    public void Validate_answers_as_Read_does(string shape)
    {
        byte[] bytes = Payloads[shape];
        var tree = ExceptionInfo.FromBytes(bytes.AsMemory());
        Assert.Equal(tree is not null, ExceptionInfo.Validate(bytes));
    }

    public static TheoryData<string, byte[]> Malformed() => new()
    {
        { "integer type",            MapBytes(("type", 5)) },
        { "bool message",            MapBytes(("type", "T"), ("msg", true)) },
        { "integer stack",           MapBytes(("type", "T"), ("stk", 1)) },
        { "map type",                MapBytes(("type", MapBytes(("x", "y")))) },
        { "integer key",             Pack((ref MessagePackWriter w) => { w.WriteMapHeader(1); w.Write(1); w.Write("v"); }) },
        { "inner integer type",      MapBytes(("type", "T"), ("inner", MapBytes(("type", 5)))) },
        { "depth 3 integer type",    MapBytes(("type", "T"), ("inner", MapBytes(("type", "I"), ("inner", MapBytes(("type", 5)))))) },
        { "truncated map",           MapBytes(("type", "T"), ("msg", "m"))[..^3] },
        { "map claiming 15 fields",  [0x8F, 0xA4, (byte)'t', (byte)'y', (byte)'p', (byte)'e', 0xA1, (byte)'T'] },
        { "truncated legacy string", Pack((ref MessagePackWriter w) => w.Write("a string cut short"))[..^4] },
    };

    [Theory]
    [MemberData(nameof(Malformed))]
    public void Validate_throws_where_Read_throws(string shape, byte[] bytes)
    {
        var fromRead     = Record.Exception(() => ExceptionInfo.FromBytes(bytes.AsMemory()));
        var fromValidate = Record.Exception(() => ExceptionInfo.Validate(bytes));
        Assert.NotNull(fromRead);
        Assert.NotNull(fromValidate);
        Assert.Equal(fromRead.GetType(), fromValidate.GetType());
        _ = shape;
    }

    /// <summary>Read skips the inner below MaxDepth without looking at it, so a fault there is no fault at all.</summary>
    [Fact]
    public void Validate_does_not_look_below_the_depth_cap_either()
    {
        byte[] bytes = MapBytes(("type", "L1"), ("inner", MapBytes(("type", "L2"), ("inner", MapBytes(("type", "L3"), ("inner", MapBytes(("type", 5))))))));
        Assert.NotNull(ExceptionInfo.FromBytes(bytes.AsMemory()));
        Assert.True(ExceptionInfo.Validate(bytes));
    }

    [Fact]
    public void Validate_allocates_nothing()
    {
        byte[] bytes = Payloads["canonical"];
        for (int i = 0; i < 100; i++) ExceptionInfo.Validate(bytes);
        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < 1_000; i++) ExceptionInfo.Validate(bytes);
        Assert.Equal(0, GC.GetAllocatedBytesForCurrentThread() - before);
    }

    // ── ReadField: what a filter reads per row ─────────────────────────────

    [Theory]
    [MemberData(nameof(Shapes))]
    public void ReadField_answers_as_the_tree_does(string shape)
    {
        byte[] bytes = Payloads[shape];
        var tree = ExceptionInfo.FromBytes(bytes.AsMemory());
        Assert.Equal(tree?.Type,           ExceptionInfo.ReadField(bytes, ExceptionInfo.Field.Type));
        Assert.Equal(tree?.Message,        ExceptionInfo.ReadField(bytes, ExceptionInfo.Field.Message));
        Assert.Equal(tree?.StackTrace,     ExceptionInfo.ReadField(bytes, ExceptionInfo.Field.StackTrace));
        Assert.Equal(tree?.Inner?.Type,    ExceptionInfo.ReadField(bytes, ExceptionInfo.Field.InnerType));
        Assert.Equal(tree?.Inner?.Message, ExceptionInfo.ReadField(bytes, ExceptionInfo.Field.InnerMessage));
    }

    [Fact]
    public void ReadField_builds_the_one_string_it_answers_with()
    {
        byte[] bytes = Payloads["long stack"];
        for (int i = 0; i < 100; i++) ExceptionInfo.ReadField(bytes, ExceptionInfo.Field.Type);
        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < 1_000; i++) ExceptionInfo.ReadField(bytes, ExceptionInfo.Field.Type);
        long perRead = (GC.GetAllocatedBytesForCurrentThread() - before) / 1_000;
        Assert.True(perRead < 64, $"reading @x.type allocated {perRead} B: the stack trace is being decoded with it");
    }

    // ── TryReadIndexFields: what the index files per row ───────────────────

    [Theory]
    [MemberData(nameof(Shapes))]
    public void TryReadIndexFields_answers_as_the_tree_does(string shape)
    {
        byte[] bytes = Payloads[shape];
        var tree = ExceptionInfo.FromBytes(bytes.AsMemory());
        bool ok  = ExceptionInfo.TryReadIndexFields(bytes, out var type, out var message, out var innerType);
        Assert.Equal(tree is not null, ok);
        if (tree is null) return;
        // The bytes as stored (the index builder re-encodes ill-formed UTF-8 before it files them), read as the tree's strings were.
        Assert.Equal(tree.Type,              Encoding.UTF8.GetString(type));
        Assert.Equal(tree.Message ?? "",     Encoding.UTF8.GetString(message));
        Assert.Equal(tree.Inner?.Type ?? "", Encoding.UTF8.GetString(innerType));
    }

    // ── RootTextContains: last value wins, as the tree's does ──────────────

    [Fact]
    public void RootTextContains_searches_the_values_the_tree_holds()
    {
        byte[] bytes = Payloads["repeated keys"];
        var tree = ExceptionInfo.FromBytes(bytes.AsMemory())!;
        Assert.Equal("Last", tree.Type);
        Assert.True(ExceptionInfo.RootTextContains(bytes, "last"));
        Assert.False(ExceptionInfo.RootTextContains(bytes, "first"));
    }

    // ── The JSON writer: what a client receives ─────────────────────────────

    private static string Json(LogEvent ev, bool relaxed)
    {
        var buf = new ArrayBufferWriter<byte>(4096);
        using var w = new Utf8JsonWriter(buf, relaxed ? new JsonWriterOptions { Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping } : default);
        LogEventJsonWriter.Write(w, ev);
        w.Flush();
        return Encoding.UTF8.GetString(buf.WrittenSpan);
    }

    private static LogEvent Event(ReadOnlyMemory<byte> raw, ExceptionInfo? tree, bool viaTree) => new()
    {
        Id = new EventId(3u, 9u), Timestamp = new DateTimeOffset(2026, 11, 5, 10, 0, 0, TimeSpan.Zero),
        Level = LogLevel.Error, MessageTemplate = "failed {Id}",
        RawException = viaTree ? default : raw,
        Exception    = viaTree ? tree : null,
    };

    [Theory]
    [MemberData(nameof(Shapes))]
    public void Json_from_the_bytes_is_the_json_from_the_tree(string shape)
    {
        byte[] bytes = Payloads[shape];
        var tree = ExceptionInfo.FromBytes(bytes.AsMemory());

        foreach (bool relaxed in new[] { false, true })
        {
            var fromBytes = Event(bytes, tree, viaTree: false);
            string viaBytes = Json(fromBytes, relaxed);
            string viaTree  = Json(Event(bytes, tree, viaTree: true), relaxed);
            Assert.Equal(viaTree, viaBytes);
            Assert.False(fromBytes.ExceptionMaterialised, "the writer built the tree it exists not to build");
            Assert.Equal(tree is not null, viaBytes.Contains("\"@x\":", StringComparison.Ordinal));
        }
    }

    [Fact]
    public void Json_from_the_bytes_allocates_no_tree()
    {
        byte[] bytes = Payloads["long stack"];
        var buf = new ArrayBufferWriter<byte>(8192);
        using var w = new Utf8JsonWriter(buf);
        void Write()
        {
            buf.ResetWrittenCount();
            w.Reset(buf);
            LogEventJsonWriter.Write(w, Event(bytes, null, viaTree: false));
            w.Flush();
        }
        for (int i = 0; i < 100; i++) Write();
        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < 1_000; i++) Write();
        long perRow = (GC.GetAllocatedBytesForCurrentThread() - before) / 1_000;
        // The LogEvent itself is built per call (~120 B); the 700-byte stack trace must not be.
        Assert.True(perRow < 400, $"writing a row with a long stack trace allocated {perRow} B per row");
    }

    /// <summary>The tree road threw at the decode, before any JSON; the bytes road throws the same type from inside the object.</summary>
    [Fact]
    public void Json_from_malformed_bytes_throws_as_the_tree_did()
    {
        byte[] bytes = MapBytes(("type", 5));
        var fromTree  = Record.Exception(() => Event(bytes, ExceptionInfo.FromBytes(bytes.AsMemory()), viaTree: true));
        var fromBytes = Record.Exception(() => Json(Event(bytes, null, viaTree: false), relaxed: false));
        Assert.NotNull(fromTree);
        Assert.NotNull(fromBytes);
        Assert.Equal(fromTree.GetType(), fromBytes.GetType());
        Assert.IsType<MessagePackSerializationException>(fromBytes);
    }
}
