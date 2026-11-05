using System.Buffers;
using System.Text;
using System.Text.Json;
using Ameto.Core;
using MessagePack;
using Xunit;

namespace Ameto.Core.Tests;

/// <summary>
/// THE BYTES ARE WHAT IS STORED. An event's <c>@x</c> is kept as it arrived — in the batch, the
/// tier, the WAL entry and the segment column — and every consumer reads the bytes where it used
/// to read the tree. That holds only if each byte-level reader answers exactly as the tree does,
/// over every shape the wire accepts, so these tests pin each one against <see cref="ExceptionInfo.Read"/>:
/// <see cref="ExceptionInfo.Validate"/> (what ingest accepts, refuses and reads as none),
/// <see cref="ExceptionInfo.ReadField"/> (what a filter reads per row) and the JSON writer (what a
/// client receives), over the canonical shape and the odd ones — keys out of order, unknown keys,
/// repeated keys, nil fields, empty strings, a chain deeper than <see cref="ExceptionInfo.MaxDepth"/>,
/// a legacy plain string, invalid UTF-8.
/// </summary>
public sealed class ExceptionBytesContractTests
{
    private delegate void PackAction(ref MessagePackWriter writer);

    private static byte[] Pack(PackAction write)
    {
        var buf = new ArrayBufferWriter<byte>(256);
        var w   = new MessagePackWriter(buf);
        write(ref w);
        w.Flush();
        return buf.WrittenSpan.ToArray();
    }

    private static void Map(ref MessagePackWriter w, params (string Key, object? Value)[] fields)
    {
        w.WriteMapHeader(fields.Length);
        foreach (var (key, value) in fields)
        {
            w.Write(key);
            switch (value)
            {
                case null:            w.WriteNil(); break;
                case string s:        w.Write(s); break;
                case byte[] raw:      w.WriteRaw(raw); break;
                case int i:           w.Write(i); break;
                case bool b:          w.Write(b); break;
                default: throw new InvalidOperationException(value.GetType().Name);
            }
        }
    }

    private static byte[] MapBytes(params (string Key, object? Value)[] fields) => Pack((ref MessagePackWriter w) => Map(ref w, fields));

    /// <summary>A UTF-8 string with invalid sequences in it, as a client that mishandles encodings sends them.</summary>
    private static byte[] InvalidUtf8String()
    {
        byte[] text = [(byte)'b', (byte)'a', (byte)'d', 0xC3, 0x28, (byte)' ', 0xE2, 0x82, (byte)' ', 0xF0, 0x90, 0x80, (byte)' ', 0xFF, (byte)'!'];
        var buf = new ArrayBufferWriter<byte>(32);
        var w   = new MessagePackWriter(buf);
        w.WriteStringHeader(text.Length);
        w.WriteRaw(text);
        w.Flush();
        return buf.WrittenSpan.ToArray();
    }

    private static readonly string Stack = string.Join('\n', Enumerable.Repeat("   at A.B() in /src/A.cs:line 1", 20));

    /// <summary>Every shape a reader must agree on, by name.</summary>
    public static TheoryData<string> Shapes() => [.. Payloads.Keys];

    private static readonly Dictionary<string, byte[]> Payloads = new()
    {
        ["canonical"]      = new ExceptionInfo { Type = "System.InvalidOperationException", Message = "boom \"quoted\"\n\ttab", StackTrace = "   at A.B()\n   at C.D()",
                                                 Inner = new ExceptionInfo { Type = "System.IO.IOException", Message = "disk", Inner = new ExceptionInfo { Type = "System.Exception" } } }.ToBytes(),
        ["type only"]      = new ExceptionInfo { Type = "System.TimeoutException" }.ToBytes(),
        ["empty map"]      = Pack((ref MessagePackWriter w) => w.WriteMapHeader(0)),
        ["no type"]        = MapBytes(("msg", "only a message"), ("stk", "   at X()")),
        ["nil type"]       = MapBytes(("type", null), ("msg", "typed nil")),
        ["empty type"]     = MapBytes(("type", ""), ("msg", "typed empty")),
        ["empty message"]  = MapBytes(("type", "T"), ("msg", ""), ("stk", "")),
        ["nil fields"]     = MapBytes(("type", "T"), ("msg", null), ("stk", null), ("inner", null)),
        ["out of order"]   = MapBytes(("inner", MapBytes(("msg", "inner first"), ("type", "I"))), ("stk", "s"), ("msg", "m"), ("type", "T")),
        ["unknown keys"]   = MapBytes(("zzz", "ignored"), ("type", "T"), ("extra", MapBytes(("type", "not an inner"))), ("msg", "m"), ("n", 5), ("b", true)),
        ["repeated keys"]  = MapBytes(("type", "First"), ("msg", "first"), ("type", "Last"), ("msg", "last"), ("inner", MapBytes(("type", "I1"))), ("inner", MapBytes(("type", "I2")))),
        ["repeated to nil"] = MapBytes(("type", "First"), ("type", null), ("msg", "m"), ("msg", null), ("inner", MapBytes(("type", "I1"))), ("inner", null)),
        ["too deep"]       = MapBytes(("type", "L1"), ("inner", MapBytes(("type", "L2"), ("inner", MapBytes(("type", "L3"), ("inner", MapBytes(("type", "L4"), ("inner", MapBytes(("type", "L5")))))))))),
        ["deep legacy"]    = MapBytes(("type", "L1"), ("inner", MapBytes(("type", "L2"), ("inner", "a plain string three down")))),
        ["inner string"]   = MapBytes(("type", "T"), ("inner", "inner as a plain string")),
        ["inner empty"]    = MapBytes(("type", "T"), ("inner", "")),
        ["inner scalar"]   = MapBytes(("type", "T"), ("inner", 7)),
        ["legacy string"]  = Pack((ref MessagePackWriter w) => w.Write("System.Exception: plain text")),
        ["legacy invalid"] = InvalidUtf8String(),
        ["legacy empty"]   = Pack((ref MessagePackWriter w) => w.Write("")),
        ["nil"]            = Pack((ref MessagePackWriter w) => w.WriteNil()),
        ["scalar"]         = Pack((ref MessagePackWriter w) => w.Write(42)),
        ["array"]          = Pack((ref MessagePackWriter w) => { w.WriteArrayHeader(2); w.Write("a"); w.Write("b"); }),
        ["unicode"]        = MapBytes(("type", "Платежи.Ошибка"), ("msg", "ключ 😀 \"q\" tab\there nbsp here <&>"), ("stk", "   в X.Y()")),
        ["invalid utf8"]   = MapBytes(("type", InvalidUtf8String()), ("msg", InvalidUtf8String()), ("stk", InvalidUtf8String()), ("inner", MapBytes(("type", InvalidUtf8String())))),
        ["long stack"]     = MapBytes(("type", "T"), ("stk", Stack)),
        ["nil key"]        = Pack((ref MessagePackWriter w) => { w.WriteMapHeader(2); w.WriteNil(); w.Write("value of a nil key"); w.Write("type"); w.Write("T"); }),
    };

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

    [Fact]
    public void Json_from_malformed_bytes_throws_as_the_tree_did()
    {
        byte[] bytes = MapBytes(("type", 5));
        var fromTree  = Record.Exception(() => Json(Event(bytes, null, viaTree: true), relaxed: false));
        var fromBytes = Record.Exception(() => Json(Event(bytes, null, viaTree: false), relaxed: false));
        Assert.NotNull(fromBytes);
        Assert.IsType<MessagePackSerializationException>(fromBytes);
        _ = fromTree;   // the tree road has no bytes to decode: it writes no @x at all, which is the laziness, not the contract
    }
}
