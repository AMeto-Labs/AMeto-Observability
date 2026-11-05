using System.Buffers;
using Ameto.Core;
using MessagePack;

namespace Ameto.Testing;

/// <summary>
/// Every shape of a CLEF <c>@x</c> a byte-level reader must agree on with the decoded tree, by name:
/// the canonical map this server used to write, and the ones a client sends — keys out of order,
/// unknown keys, repeated keys (the last wins, to nil included), nil fields, empty strings, a chain
/// deeper than <see cref="ExceptionInfo.MaxDepth"/>, a legacy plain string, invalid UTF-8, a nil key —
/// and the shapes that decode to no exception at all. Linked into the Core tests (every reader
/// against the tree) and the Indexing tests (the index built from the bytes against the one built
/// from the tree), so the two cannot drift apart on what "every shape" means.
/// </summary>
public static class ExceptionShapes
{
    public delegate void PackAction(ref MessagePackWriter writer);

    public static byte[] Pack(PackAction write)
    {
        var buf = new ArrayBufferWriter<byte>(256);
        var w   = new MessagePackWriter(buf);
        write(ref w);
        w.Flush();
        return buf.WrittenSpan.ToArray();
    }

    public static void Map(ref MessagePackWriter w, params (string Key, object? Value)[] fields)
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

    public static byte[] MapBytes(params (string Key, object? Value)[] fields) => Pack((ref MessagePackWriter w) => Map(ref w, fields));

    /// <summary>A UTF-8 string with invalid sequences in it, as a client that mishandles encodings sends them.</summary>
    public static byte[] InvalidUtf8String()
    {
        byte[] text = [(byte)'b', (byte)'a', (byte)'d', 0xC3, 0x28, (byte)' ', 0xE2, 0x82, (byte)' ', 0xF0, 0x90, 0x80, (byte)' ', 0xFF, (byte)'!'];
        var buf = new ArrayBufferWriter<byte>(32);
        var w   = new MessagePackWriter(buf);
        w.WriteStringHeader(text.Length);
        w.WriteRaw(text);
        w.Flush();
        return buf.WrittenSpan.ToArray();
    }

    public static readonly string Stack = string.Join('\n', Enumerable.Repeat("   at A.B() in /src/A.cs:line 1", 20));

    public static readonly IReadOnlyDictionary<string, byte[]> Payloads = new Dictionary<string, byte[]>
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
        ["inner to empty"] = MapBytes(("type", "T"), ("inner", MapBytes(("type", "I1"))), ("inner", "")),
        ["inner to scalar"] = MapBytes(("type", "T"), ("inner", MapBytes(("type", "I1"))), ("inner", 7)),
        ["inner to string"] = MapBytes(("type", "T"), ("inner", MapBytes(("type", "I1"))), ("inner", "legacy inner last")),
        ["too deep"]       = MapBytes(("type", "L1"), ("inner", MapBytes(("type", "L2"), ("inner", MapBytes(("type", "L3"), ("inner", MapBytes(("type", "L4"), ("inner", MapBytes(("type", "L5")))))))))),
        ["deep legacy"]    = MapBytes(("type", "L1"), ("inner", MapBytes(("type", "L2"), ("inner", "a plain string three down")))),
        ["inner string"]   = MapBytes(("type", "T"), ("inner", "inner as a plain string")),
        ["inner empty"]    = MapBytes(("type", "T"), ("inner", "")),
        ["inner scalar"]   = MapBytes(("type", "T"), ("inner", 7)),
        ["inner no type"]  = MapBytes(("type", "T"), ("inner", MapBytes(("msg", "typeless inner")))),
        ["inner nil type"] = MapBytes(("type", "T"), ("inner", MapBytes(("type", null), ("msg", "nil-typed inner")))),
        ["inner empty type"] = MapBytes(("type", "T"), ("inner", MapBytes(("type", ""), ("msg", "empty-typed inner")))),
        ["legacy string"]  = Pack((ref MessagePackWriter w) => w.Write("System.Exception: plain text")),
        ["legacy invalid"] = InvalidUtf8String(),
        ["legacy empty"]   = Pack((ref MessagePackWriter w) => w.Write("")),
        ["nil"]            = Pack((ref MessagePackWriter w) => w.WriteNil()),
        ["scalar"]         = Pack((ref MessagePackWriter w) => w.Write(42)),
        ["array"]          = Pack((ref MessagePackWriter w) => { w.WriteArrayHeader(2); w.Write("a"); w.Write("b"); }),
        ["unicode"]        = MapBytes(("type", "Платежи.Ошибка"), ("msg", "ключ 😀 \"q\" tab\there nbsp here <&>"), ("stk", "   в X.Y()")),
        ["invalid utf8"]   = MapBytes(("type", InvalidUtf8String()), ("msg", InvalidUtf8String()), ("stk", InvalidUtf8String()), ("inner", MapBytes(("type", InvalidUtf8String())))),
        ["long stack"]     = MapBytes(("type", "T"), ("stk", Stack)),
        ["nil key"]        = Pack((ref MessagePackWriter w) => { w.WriteMapHeader(2); w.WriteNil(); w.Write("value of a nil key"); w.Write("type"); w.Write("T"); }),
    };
}
