using System.Buffers;
using MessagePack;
using Ameto.Core;
using Xunit;

namespace Ameto.Storage.Tests;

/// <summary>
/// The exception column is copied, not serialised: the hot tier holds an exception as the msgpack
/// it arrived in (behind the event's properties, in its arena), and the writer copies those bytes
/// into the column as it copies every other column. It used to serialise the tier's object graph
/// here — straight into the column's scratch through an <c>IBufferWriter</c>, and before that
/// through <c>ExceptionInfo.ToBytes()</c> and a copy.
///
/// <para>Same bytes, or the format moved: these tests compare the column against
/// <c>ToBytes()</c> — the definition the pre-streaming writer, the WAL and every other producer
/// of this blob shared, and the shape every client that sends a structured <c>@x</c> sends — for a
/// single exception, for a chain of inner exceptions, for one with no message and no stack, and
/// for a tier that mixes exception-carrying rows with plain ones, where the column's offsets are
/// what say which is which. And, since the column now holds what the client sent, for the shapes
/// the old decode used to normalise away.</para>
/// </summary>
public sealed class SegmentExceptionColumnTests
{
    private static byte[] Props(int i)
    {
        var buf = new ArrayBufferWriter<byte>(64);
        var w = new MessagePackWriter(buf);
        w.WriteMapHeader(1);
        w.Write("n"); w.Write((long)i);
        w.Flush();
        return buf.WrittenSpan.ToArray();
    }

    private static ExceptionInfo? Sample(int i) => (i % 4) switch
    {
        0 => null,                                                    // no exception at all
        1 => new ExceptionInfo { Type = "System.TimeoutException" },  // type only
        2 => new ExceptionInfo
        {
            Type       = "System.InvalidOperationException",
            Message    = "payment " + i + " could not be settled",
            StackTrace = "   at Ameto.Wallet.Handler.Step(Int32 n)\n   at Ameto.Wallet.Handler.Run()\n",
        },
        _ => new ExceptionInfo
        {
            Type       = "System.AggregateException",
            Message    = "one or more errors (" + i + ")",
            StackTrace = "   at Ameto.Wallet.Batch.Flush()\n",
            Inner      = new ExceptionInfo
            {
                Type       = "System.Net.Http.HttpRequestException",
                Message    = "upstream refused the connection",
                Inner      = new ExceptionInfo { Type = "System.Net.Sockets.SocketException", Message = "ECONNREFUSED" },
            },
        },
    };

    private static HotTierSegment BuildTier(StringInternPool pool, int count)
    {
        var hot = new HotTierSegment(count + 1, (long)count * 4096 + 1024 * 1024);
        int tmplIdx = pool.Intern("request {n} failed");
        string tmpl = pool.Get(tmplIdx);
        int svcIdx  = pool.Intern("Wallet.API");
        long baseTicks = new DateTimeOffset(2026, 9, 11, 9, 0, 0, TimeSpan.Zero).UtcTicks;

        for (int i = 0; i < count; i++)
        {
            var h = new LogEventHeader
            {
                Id                       = new EventId(0u, (uint)i).RawValue,
                TimestampUtcTicks        = baseTicks + i * 10,
                Level                    = LogLevel.Error,
                MessageTemplatePoolIndex = tmplIdx,
                ServiceNamePoolIndex     = svcIdx,
            };
            Assert.True(hot.TryWrite(h, Props(i), tmpl, Sample(i)?.ToBytes()));
        }
        hot.Freeze();
        return hot;
    }

    private static List<LogEvent> ReadBack(string path)
    {
        using var reader = SegmentReader.Open(path);
        var list = new List<LogEvent>();
        var e = reader.ReadEventsAsync(null, null, null, false, default).GetAsyncEnumerator();
        try
        {
            while (e.MoveNextAsync().AsTask().GetAwaiter().GetResult()) list.Add(e.Current);
        }
        finally { e.DisposeAsync().AsTask().GetAwaiter().GetResult(); }
        return list;
    }

    /// <summary>
    /// Every exception survives the column unchanged — type, message, stack and the whole inner
    /// chain — and a row without one still reads back as none.
    /// </summary>
    [Fact]
    public void ExceptionColumn_RoundTripsEveryShape()
    {
        const int Count = 400;
        string path = Path.Combine(Path.GetTempPath(), $"Ameto-exccol-{Guid.NewGuid():N}.seg");
        try
        {
            var pool = new StringInternPool();
            using (var hot = BuildTier(pool, Count))
            using (var w = new SegmentWriter(path))
            {
                w.WriteEvents(hot, pool, SegmentWriter.ComputeSortOrder(hot));
                w.Finalise(new NodeId(3), new SegmentId(9UL));
            }

            var read = ReadBack(path);
            Assert.Equal(Count, read.Count);
            for (int i = 0; i < Count; i++)
            {
                var expected = Sample(i);
                var actual   = read[i].Exception;
                if (expected is null) { Assert.Null(actual); continue; }

                Assert.NotNull(actual);
                for (ExceptionInfo? x = expected, y = actual; x is not null || y is not null;
                     x = x!.Inner, y = y!.Inner)
                {
                    Assert.NotNull(x);
                    Assert.NotNull(y);
                    Assert.Equal(x.Type,       y.Type);
                    Assert.Equal(x.Message,    y.Message);
                    Assert.Equal(x.StackTrace, y.StackTrace);
                }
            }
        }
        finally { File.Delete(path); }
    }

    /// <summary>
    /// The column holds the client's bytes whatever their shape — a legacy plain string, a map with
    /// its keys in the client's order and unknown ones among them, a chain deeper than the decoder
    /// reads — unchanged through the flush, and a row read back carries them as
    /// <see cref="LogEvent.RawException"/>, decoding to what the decoder makes of them.
    /// </summary>
    [Fact]
    public void ExceptionColumn_KeepsTheClientsBytesWhateverTheirShape()
    {
        byte[][] shapes =
        [
            Pack((ref MessagePackWriter w) => w.Write("System.Exception: plain text")),
            Pack((ref MessagePackWriter w) =>
            {
                w.WriteMapHeader(4);
                w.Write("zzz");   w.Write("ignored");
                w.Write("inner"); w.WriteMapHeader(2); w.Write("msg"); w.Write("inner first"); w.Write("type"); w.Write("I");
                w.Write("msg");   w.Write("only a message");
                w.Write("type");  w.Write("Some.Type");
            }),
            Pack((ref MessagePackWriter w) =>
            {
                w.WriteMapHeader(2); w.Write("type"); w.Write("L1");
                w.Write("inner"); w.WriteMapHeader(2); w.Write("type"); w.Write("L2");
                w.Write("inner"); w.WriteMapHeader(2); w.Write("type"); w.Write("L3");
                w.Write("inner"); w.WriteMapHeader(1); w.Write("type"); w.Write("L4");
            }),
            [],   // a row without one, between the others
        ];
        string path = Path.Combine(Path.GetTempPath(), $"Ameto-exccol-{Guid.NewGuid():N}.seg");
        try
        {
            var pool = new StringInternPool();
            using (var hot = new HotTierSegment(16, 1 << 20))
            using (var w = new SegmentWriter(path))
            {
                int tmplIdx = pool.Intern("request {n} failed");
                for (int i = 0; i < shapes.Length; i++)
                    Assert.True(hot.TryWrite(new LogEventHeader
                    {
                        Id = new EventId(0u, (uint)i).RawValue, TimestampUtcTicks = 1_000 + i, Level = LogLevel.Error,
                        MessageTemplatePoolIndex = tmplIdx, ServiceNamePoolIndex = -1,
                    }, Props(i), pool.Get(tmplIdx), shapes[i]));
                hot.Freeze();
                for (int i = 0; i < shapes.Length; i++)
                    Assert.True(hot.GetExceptionPayload(i).SequenceEqual(shapes[i]));
                w.WriteEvents(hot, pool, SegmentWriter.ComputeSortOrder(hot));
                w.Finalise(new NodeId(3), new SegmentId(9UL));
            }

            using (var src = MergingSegmentEventSource.Open([path]))
            {
                int i = 0;
                while (src.TryReadNext(out var ev))
                    Assert.True(ev.ExceptionPayload.SequenceEqual(shapes[i++]), $"row {i - 1}");
                Assert.Equal(shapes.Length, i);
            }

            var read = ReadBack(path);
            Assert.Equal(shapes.Length, read.Count);
            for (int i = 0; i < shapes.Length; i++)
            {
                Assert.True(read[i].RawException.Span.SequenceEqual(shapes[i]), $"row {i}");
                Assert.Equal(ExceptionInfo.FromBytes(shapes[i].AsMemory())?.Type, read[i].Exception?.Type);
            }
            Assert.Equal("Exception", read[0].Exception?.Type);
            Assert.Equal("I",         read[1].Exception?.Inner?.Type);
            Assert.Null(read[2].Exception?.Inner?.Inner?.Inner);
            Assert.Null(read[3].Exception);
        }
        finally { File.Delete(path); }
    }

    private delegate void PackAction(ref MessagePackWriter writer);

    private static byte[] Pack(PackAction write)
    {
        var buf = new ArrayBufferWriter<byte>(64);
        var w   = new MessagePackWriter(buf);
        write(ref w);
        w.Flush();
        return buf.WrittenSpan.ToArray();
    }

    /// <summary>
    /// Byte parity with <c>ExceptionInfo.ToBytes()</c>, row by row. Round-tripping proves the
    /// blob is READABLE; this proves it is the same blob, which is what keeps a segment written
    /// today identical to one written before the writer stopped going through ToBytes().
    /// </summary>
    [Fact]
    public void ExceptionColumn_IsByteIdenticalToToBytes()
    {
        const int Count = 400;
        string path = Path.Combine(Path.GetTempPath(), $"Ameto-exccol-{Guid.NewGuid():N}.seg");
        try
        {
            var pool = new StringInternPool();
            using (var hot = BuildTier(pool, Count))
            using (var w = new SegmentWriter(path))
            {
                w.WriteEvents(hot, pool, SegmentWriter.ComputeSortOrder(hot));
                w.Finalise(new NodeId(3), new SegmentId(9UL));
            }

            // The merge path carries the column through as raw bytes, so reading it back
            // through a merging source is reading exactly what the writer emitted.
            using var src = MergingSegmentEventSource.Open([path]);
            int i = 0;
            while (src.TryReadNext(out var ev))
            {
                var expected = Sample(i);
                if (expected is null)
                {
                    Assert.True(ev.ExceptionPayload.IsEmpty, $"row {i} grew an exception");
                }
                else
                {
                    Assert.True(expected.ToBytes().AsSpan().SequenceEqual(ev.ExceptionPayload),
                                $"row {i}: the exception column is no longer what ToBytes() produces");
                }
                i++;
            }
            Assert.Equal(Count, i);
        }
        finally { File.Delete(path); }
    }
}
