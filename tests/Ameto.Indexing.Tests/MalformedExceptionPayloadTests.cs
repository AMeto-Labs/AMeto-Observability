using System.Buffers;
using Ameto.Core;
using Ameto.Indexing;
using Ameto.Storage;
using MessagePack;

namespace Ameto.Indexing.Tests;

/// <summary>
/// A merge row whose exception column is not a readable exception map is still written; the
/// builder counts it (per group and process-wide) instead of failing the merge as the full
/// decode used to, or skipping it silently.
/// </summary>
public sealed class MalformedExceptionPayloadTests
{
    /// <summary>A merge-shaped source: exceptions travel as bytes, no decoded object.</summary>
    private sealed class BytesSource(byte[][] exceptions) : ISegmentEventSource
    {
        private int _i;
        private readonly byte[] _props = Props();
        public long RemainingEventHint => exceptions.Length - _i;
        public bool TryReadNext(out SegmentEventRef ev)
        {
            if (_i == exceptions.Length) { ev = default; return false; }
            int i = _i++;
            ev = new SegmentEventRef(new EventId(0u, (uint)i).RawValue, DateTimeOffset.UtcNow.UtcTicks + i, LogLevel.Error,
                                     0, 0, 0, "settlement failed", "Svc", exception: null, _props, exceptions[i]);
            return true;
        }
        private static byte[] Props()
        {
            var buf = new ArrayBufferWriter<byte>(32);
            var w = new MessagePackWriter(buf);
            w.WriteMapHeader(1); w.Write("n"); w.Write(1L); w.Flush();
            return buf.WrittenSpan.ToArray();
        }
    }

    private static byte[] NonStringType()
    {
        var buf = new ArrayBufferWriter<byte>(32);
        var w = new MessagePackWriter(buf);
        w.WriteMapHeader(2); w.Write("type"); w.Write(42L); w.Write("msg"); w.Write("m"); w.Flush();
        return buf.WrittenSpan.ToArray();
    }

    [Fact]
    public void MalformedPayload_IsCounted_AndTheRowIsStillWritten()
    {
        var good      = new ExceptionInfo { Type = "System.TimeoutException", Message = "ledger" }.ToBytes();
        var truncated = good.AsSpan(0, good.Length - 4).ToArray();
        var hints     = new IndexBuildHints();
        SegmentIndexBuilder? builder = null;

        string path = Path.Combine(Path.GetTempPath(), "Ameto-malformed-" + Guid.NewGuid().ToString("N") + ".seg");
        SegmentInfo info;
        try
        {
            using (var writer = new SegmentWriter(path))
            {
                writer.WriteEvents(new BytesSource([good, truncated, NonStringType(), good]),
                                   (events, terms) => builder = new SegmentIndexBuilder(events, 5, terms, hints));
                info = writer.Finalise(new NodeId(0), new SegmentId(1));
            }
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }

        Assert.Equal(4u, info.EventCount);                       // every row written
        Assert.Equal(2, builder!.MalformedExceptionPayloads);    // the truncated map and the non-string type
        Assert.Equal(2, hints.MalformedExceptionPayloads);
    }

    [Fact]
    public void WellFormedPayloads_CountNothing()
    {
        var hints = new IndexBuildHints();
        using var builder = new SegmentIndexBuilder(4, 5, 0, hints);
        var src = new BytesSource([new ExceptionInfo { Type = "A" }.ToBytes(), Legacy("legacy text")]);
        uint i = 0;
        while (src.TryReadNext(out var ev)) builder.Add(i++, in ev);
        Assert.Equal(0, builder.MalformedExceptionPayloads);
        Assert.Equal(0, hints.MalformedExceptionPayloads);

        static byte[] Legacy(string s)
        {
            var buf = new ArrayBufferWriter<byte>(32);
            var w = new MessagePackWriter(buf);
            w.Write(s); w.Flush();
            return buf.WrittenSpan.ToArray();
        }
    }
}
