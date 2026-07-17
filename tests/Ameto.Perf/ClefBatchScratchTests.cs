using System.Buffers;
using MessagePack;
using Ameto.Core;
using Ameto.Core.Serialization;
using Xunit;

namespace Ameto.Perf;

/// <summary>
/// CLEF batch deserialisation reuses per-thread scratch buffers for the raw property
/// pairs; each event's RawProperties must be its own copy — parsing event N+1 must not
/// corrupt event N's bytes, and events without user props must stay empty.
/// </summary>
public sealed class ClefBatchScratchTests
{
    [Fact]
    public void DeserializeBatch_RawProperties_AreIndependentPerEvent()
    {
        // Three CLEF events: distinct props / no props / different-shaped props.
        var buf = new ArrayBufferWriter<byte>(1024);
        var w   = new MessagePackWriter(buf);
        w.WriteArrayHeader(3);

        w.WriteMapHeader(3);
        w.Write("@mt");     w.Write("first {orderId}");
        w.Write("orderId"); w.Write(111L);
        w.Write("region");  w.Write("kz");

        w.WriteMapHeader(1);
        w.Write("@mt");     w.Write("second — no props");

        w.WriteMapHeader(2);
        w.Write("@mt");     w.Write("third {flag}");
        w.Write("flag");    w.Write(true);
        w.Flush();

        var events  = new List<LogEvent>();
        uint seq    = 0;
        int  parsed = LogEventSerializer.DeserializeBatch(
            new ReadOnlySequence<byte>(buf.WrittenMemory), nodeId: 0, ref seq, events);

        Assert.Equal(3, parsed);

        // Decode AFTER the whole batch parsed — stale scratch references would surface here.
        var p1 = LogEventSerializer.DeserializePropertiesMap(events[0].RawProperties.Span)!;
        Assert.Equal(2, p1.Count);
        Assert.Equal(111L, p1["orderId"]);
        Assert.Equal("kz", p1["region"]);

        Assert.True(events[1].RawProperties.IsEmpty);

        var p3 = LogEventSerializer.DeserializePropertiesMap(events[2].RawProperties.Span)!;
        Assert.Equal(true, Assert.Single(p3).Value);
        Assert.Equal("third {flag}", events[2].MessageTemplate);
    }
}
