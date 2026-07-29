using System.Buffers;
using System.Text;
using System.Text.Json;
using Ameto.Core.Serialization;
using MessagePack;
using Xunit;

namespace Ameto.Perf;

/// <summary>
/// The transcoder replaces "msgpack → Dictionary → System.Text.Json" with
/// "msgpack → System.Text.Json". Clients must not be able to tell: these compare the two
/// byte for byte, quirks included. Behaviour is preserved here, not corrected — the
/// odd cases (nil inside an array, binary values) are pinned exactly as the old path
/// rendered them, so this stays a pure performance change.
/// </summary>
public sealed class MsgPackJsonTranscoderParityTests
{
    private static readonly JsonSerializerOptions Options = new()
    {
        PropertyNamingPolicy   = System.Text.Json.JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull,
        WriteIndented          = false,
        Converters             = { new TestDynamicObjectConverter() },
    };

    /// <summary>Old path: decode to a dictionary, then serialise it.</summary>
    private static string ViaDictionary(byte[] msgpack)
    {
        var map = LogEventSerializer.DeserializePropertiesMap(msgpack);
        var buf = new ArrayBufferWriter<byte>(256);
        using (var w = new Utf8JsonWriter(buf))
        {
            if (map is null) { w.WriteStartObject(); w.WriteEndObject(); }
            else JsonSerializer.Serialize(w, (object)map, Options);
        }
        return Encoding.UTF8.GetString(buf.WrittenSpan);
    }

    /// <summary>New path: straight from msgpack.</summary>
    private static string ViaTranscoder(byte[] msgpack)
    {
        var buf = new ArrayBufferWriter<byte>(256);
        using (var w = new Utf8JsonWriter(buf))
            MsgPackJsonTranscoder.WriteMap(w, msgpack);
        return Encoding.UTF8.GetString(buf.WrittenSpan);
    }

    private static void AssertSame(byte[] msgpack)
        => Assert.Equal(ViaDictionary(msgpack), ViaTranscoder(msgpack));

    private static byte[] Pack(Action<IBufferWriter<byte>> build)
    {
        var buf = new ArrayBufferWriter<byte>(256);
        build(buf);
        return buf.WrittenSpan.ToArray();
    }

    [Fact]
    public void ScalarsAndStrings()
    {
        AssertSame(Pack(b =>
        {
            var w = new MessagePackWriter(b);
            w.WriteMapHeader(9);
            w.Write("str");      w.Write("hello world");
            w.Write("empty");    w.Write("");
            w.Write("unicode");  w.Write("тест ☃ \"quoted\" \\ back / slash\n\t");
            w.Write("int");      w.Write(42);
            w.Write("negative"); w.Write(-17);
            w.Write("big");      w.Write(long.MaxValue);
            w.Write("dbl");      w.Write(1.5d);
            w.Write("boolT");    w.Write(true);
            w.Write("nil");      w.WriteNil();
            w.Flush();
        }));
    }

    [Fact]
    public void UInt64AboveLongMaxStaysUnsigned()
    {
        AssertSame(Pack(b =>
        {
            var w = new MessagePackWriter(b);
            w.WriteMapHeader(2);
            w.Write("huge");   w.Write(ulong.MaxValue);
            w.Write("fits");   w.Write((ulong)123);
            w.Flush();
        }));
    }

    /// <summary>A nil array element rendered as the string "&lt;null&gt;" on the old path.</summary>
    [Fact]
    public void NilInsideArrayKeepsThePlaceholder()
    {
        string json = ViaTranscoder(Pack(b =>
        {
            var w = new MessagePackWriter(b);
            w.WriteMapHeader(1);
            w.Write("arr");
            w.WriteArrayHeader(3);
            w.Write("a"); w.WriteNil(); w.Write(7);
            w.Flush();
        }));
        // STJ's default encoder escapes < and >, so the placeholder reaches the wire as
        // "<null>" — asserted in that form because that is what clients receive.
        Assert.Contains("\\u003Cnull\\u003E", json);
        AssertSame(Pack(b =>
        {
            var w = new MessagePackWriter(b);
            w.WriteMapHeader(1);
            w.Write("arr");
            w.WriteArrayHeader(3);
            w.Write("a"); w.WriteNil(); w.Write(7);
            w.Flush();
        }));
    }

    [Fact]
    public void NestedMapsAndArrays()
    {
        AssertSame(Pack(b =>
        {
            var w = new MessagePackWriter(b);
            w.WriteMapHeader(3);
            w.Write("empty.map");   w.WriteMapHeader(0);
            w.Write("empty.arr");   w.WriteArrayHeader(0);
            w.Write("permissions"); w.WriteArrayHeader(2);
            for (int i = 0; i < 2; i++)
            {
                w.WriteMapHeader(3);
                w.Write("PermissionName"); w.Write($"BusStopInfo.View{i}");
                w.Write("DisplayName");    w.Write($"View BusStopInfo {i}");
                w.Write("Nested");         w.WriteArrayHeader(2);
                w.Write(1); w.Write(2);
            }
            w.Flush();
        }));
    }

    /// <summary>Binary rendered as "System.Byte[]" — byte[] had no converter case.</summary>
    [Fact]
    public void BinaryRendersAsTheOldPlaceholder()
    {
        AssertSame(Pack(b =>
        {
            var w = new MessagePackWriter(b);
            w.WriteMapHeader(1);
            w.Write("blob"); w.Write(new byte[] { 1, 2, 3 });
            w.Flush();
        }));
    }

    [Fact]
    public void EmptyPayloadAndEmptyMap()
    {
        Assert.Equal("{}", ViaTranscoder([]));
        AssertSame(Pack(b => { var w = new MessagePackWriter(b); w.WriteMapHeader(0); w.Flush(); }));
    }

    /// <summary>The shape that actually hurts: a realistic fat event from the sandbox.</summary>
    [Fact]
    public void RealisticFatEvent()
    {
        AssertSame(Pack(b =>
        {
            var w = new MessagePackWriter(b);
            w.WriteMapHeader(5);
            w.Write("SourceContext");      w.Write("Common.MediatR.LoggingBehavior");
            w.Write("ApplicationContext"); w.Write("Office.API");
            w.Write("Environment");        w.Write("Test");
            w.Write("0");                  w.Write("MergeCreateCommand");
            w.Write("1");
            w.WriteMapHeader(3);
            w.Write("$type");   w.Write("MergeCreateCommand");
            w.Write("AppName"); w.Write("KioskAgent");
            w.Write("Permissions");
            w.WriteArrayHeader(40);
            for (int i = 0; i < 40; i++)
            {
                w.WriteMapHeader(2);
                w.Write("PermissionName"); w.Write($"Resource.Action{i}");
                w.Write("DisplayName");    w.Write($"Do action {i} on resource");
            }
            w.Flush();
        }));
    }
}

/// <summary>Copy of the server's DynamicObjectConverter — the output being matched.</summary>
internal sealed class TestDynamicObjectConverter : System.Text.Json.Serialization.JsonConverter<object>
{
    public override object? Read(ref Utf8JsonReader reader, Type t, JsonSerializerOptions o)
        => throw new NotSupportedException();

    public override void Write(Utf8JsonWriter writer, object value, JsonSerializerOptions options)
    {
        switch (value)
        {
            case Dictionary<string, object?> d:
                writer.WriteStartObject();
                foreach (var (k, v) in d)
                {
                    writer.WritePropertyName(k);
                    if (v is null) writer.WriteNullValue();
                    else Write(writer, v, options);
                }
                writer.WriteEndObject();
                break;
            case object[] arr:
                writer.WriteStartArray();
                foreach (var item in arr)
                {
                    if (item is null) writer.WriteNullValue();
                    else Write(writer, item, options);
                }
                writer.WriteEndArray();
                break;
            case string s:  writer.WriteStringValue(s);     break;
            case bool b:    writer.WriteBooleanValue(b);    break;
            case long l:    writer.WriteNumberValue(l);     break;
            case int i:     writer.WriteNumberValue(i);     break;
            case double d2: writer.WriteNumberValue(d2);    break;
            case float f:   writer.WriteNumberValue(f);     break;
            case ulong u:   writer.WriteNumberValue(u);     break;
            default:        writer.WriteStringValue(value.ToString()); break;
        }
    }
}
