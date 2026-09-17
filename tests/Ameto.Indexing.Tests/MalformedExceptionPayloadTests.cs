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

    /// <summary>Records what reaches the log at the production level (Information and up).</summary>
    private sealed class ListLogger : Microsoft.Extensions.Logging.ILogger
    {
        public readonly List<(Microsoft.Extensions.Logging.LogLevel Level, string Message)> Entries = new();
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(Microsoft.Extensions.Logging.LogLevel logLevel) => logLevel >= Microsoft.Extensions.Logging.LogLevel.Information;
        public void Log<TState>(Microsoft.Extensions.Logging.LogLevel logLevel, Microsoft.Extensions.Logging.EventId eventId,
                                TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (IsEnabled(logLevel)) Entries.Add((logLevel, formatter(state, exception)));
        }
    }

    [Fact]
    public void MalformedPayload_IsCounted_AndTheRowIsStillWritten()
    {
        var good      = new ExceptionInfo { Type = "System.TimeoutException", Message = "ledger" }.ToBytes();
        var truncated = good.AsSpan(0, good.Length - 4).ToArray();
        var hints     = new IndexBuildHints();
        var log       = new ListLogger();
        SegmentIndexBuilder? builder = null;

        string path = Path.Combine(Path.GetTempPath(), "Ameto-malformed-" + Guid.NewGuid().ToString("N") + ".seg");
        SegmentInfo info;
        try
        {
            using (var writer = new SegmentWriter(path))
            {
                writer.WriteEvents(new BytesSource([good, truncated, NonStringType(), good]),
                                   (events, terms) => builder = new SegmentIndexBuilder(events, 5, terms, hints, log));
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

        // An operator at the production log level sees it: one Warning for the group, with its
        // count and where the first one is — not a Debug line nobody prints, and not one per row.
        var entry = Assert.Single(log.Entries);
        Assert.Equal(Microsoft.Extensions.Logging.LogLevel.Warning, entry.Level);
        Assert.Contains("2 exception payload", entry.Message);
        Assert.Contains("file ordinal 1", entry.Message);
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

    /// <summary>
    /// nil, an empty legacy string and a non-string, non-map value are all "no exception" to
    /// <see cref="ExceptionInfo.FromBytes(ReadOnlyMemory{byte})"/>, so they are not malformed and
    /// must not reach the count or the group's Warning. A malformed MAP beside them still does.
    /// </summary>
    [Fact]
    public void NoExceptionPayloads_CountNothing_WhileMalformedMapsStillCount()
    {
        var good      = new ExceptionInfo { Type = "System.TimeoutException", Message = "ledger" }.ToBytes();
        var truncated = good.AsSpan(0, good.Length - 4).ToArray();
        byte[] nil         = [0xC0];
        byte[] emptyLegacy = [0xA0];
        byte[] integer     = [0x2A];
        Assert.Null(ExceptionInfo.FromBytes(nil.AsMemory()));
        Assert.Null(ExceptionInfo.FromBytes(emptyLegacy.AsMemory()));
        Assert.Null(ExceptionInfo.FromBytes(integer.AsMemory()));

        var hints = new IndexBuildHints();
        using var builder = new SegmentIndexBuilder(6, 5, 0, hints);
        var src = new BytesSource([nil, emptyLegacy, integer, truncated, NonStringType(), good]);
        uint i = 0;
        while (src.TryReadNext(out var ev)) builder.Add(i++, in ev);

        Assert.Equal(2, builder.MalformedExceptionPayloads);     // the truncated map and the non-string type only
        Assert.Equal(2, hints.MalformedExceptionPayloads);
    }

    /// <summary>
    /// <see cref="ExceptionInfo.IsPresent"/> answers from the header, so a string header too short
    /// to hold its own length (<c>D9</c>, <c>DA 00</c>, <c>DB 00 00 00</c>) and a scalar or array
    /// cut short are ABSENT to it — yet <see cref="ExceptionInfo.FromBytes(ReadOnlyMemory{byte})"/>
    /// throws on every one at delivery. They are counted and warned about like a truncated map.
    /// The readable no-exception shapes beside them, including empty str8/16/32 and a whole
    /// array, still count nothing.
    /// </summary>
    [Fact]
    public void CutHeaders_AbsentToIsPresent_AreStillCounted_WhileReadableAbsencesAreNot()
    {
        byte[][] readable =
        [
            [0xC0],                          // nil
            [0xD9, 0x00],                    // empty str8
            [0xDA, 0x00, 0x00],              // empty str16
            [0xDB, 0x00, 0x00, 0x00, 0x00],  // empty str32
            [0x2A],                          // positive fixint
            [0x91, 0x01],                    // a whole one-element array
        ];
        byte[][] cut =
        [
            [0xD9],                          // str8 without its length
            [0xDA, 0x00],                    // str16 with half its length
            [0xDB, 0x00, 0x00, 0x00],        // str32 one length byte short
            [0xCC],                          // uint8 without its byte
            [0xDC, 0x00, 0x01],              // array16 announcing one element, holding none
        ];

        // The premise, from the reader's side: all eleven are absent to the presence probe, and
        // delivery reads the first six as no exception and throws on the other five.
        foreach (var b in readable)
        {
            Assert.False(ExceptionInfo.IsPresent(b));
            Assert.Null(ExceptionInfo.FromBytes(b.AsMemory()));
        }
        foreach (var b in cut)
        {
            Assert.False(ExceptionInfo.IsPresent(b));
            Assert.Throws<EndOfStreamException>(() => ExceptionInfo.FromBytes(b.AsMemory()));
        }

        var hints = new IndexBuildHints();
        var log   = new ListLogger();
        using var builder = new SegmentIndexBuilder(readable.Length + cut.Length, 5, 0, hints, log);
        var src = new BytesSource([.. readable, .. cut]);
        uint i = 0;
        while (src.TryReadNext(out var ev)) builder.Add(i++, in ev);

        Assert.Equal(cut.Length, builder.MalformedExceptionPayloads);
        Assert.Equal(cut.Length, hints.MalformedExceptionPayloads);

        _ = builder.Serialise();
        var entry = Assert.Single(log.Entries);
        Assert.Equal(Microsoft.Extensions.Logging.LogLevel.Warning, entry.Level);
        Assert.Contains($"{cut.Length} exception payload", entry.Message);
        Assert.Contains($"file ordinal {readable.Length}", entry.Message);   // the first cut one
    }
}
