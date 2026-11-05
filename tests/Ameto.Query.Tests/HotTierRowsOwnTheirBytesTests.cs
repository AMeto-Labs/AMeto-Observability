using System.Buffers;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;
using MessagePack;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Ameto.Core;
using Ameto.Indexing;
using Ameto.Storage;

namespace Ameto.Query.Tests;

/// <summary>
/// A hot-tier row's exception is a view into the tier's arena while the executor's reader snapshot
/// holds the tier — what lets a filter read <c>@x</c> on every candidate without a copy — and the
/// rows the executor yields own their bytes, because the page is serialised after the snapshot is
/// released, by when the tier may have flushed and freed its blocks.
/// </summary>
public sealed class HotTierRowsOwnTheirBytesTests : IDisposable
{
    private const int Events = 300;

    private readonly string        _dir = Path.Combine(Path.GetTempPath(), "ameto-hotrows-" + Guid.NewGuid().ToString("N"));
    private readonly StorageEngine _engine;
    private readonly QueryExecutor _query;

    public HotTierRowsOwnTheirBytesTests()
    {
        Directory.CreateDirectory(_dir);
        var opts = new ServerOptions { DataDirectory = _dir };
        _engine = new StorageEngine(
            Options.Create(opts),
            new RetentionStore(opts, NullLogger<RetentionStore>.Instance),
            NullLogger<StorageEngine>.Instance);
        _query = new QueryExecutor(_engine, new SegmentIndexReaderFactory(), NullLogger<QueryExecutor>.Instance);

        long baseTicks = new DateTimeOffset(2026, 11, 5, 0, 0, 0, TimeSpan.Zero).UtcTicks;
        int  tmpl      = _engine.TemplatePool.Intern("order {n} failed");
        var  buf       = new ArrayBufferWriter<byte>(64);
        for (int i = 0; i < Events; i++)
        {
            buf.ResetWrittenCount();
            var w = new MessagePackWriter(buf);
            w.WriteMapHeader(1);
            w.Write("n"); w.Write((long)i);
            w.Flush();
            byte[] exc = new ExceptionInfo
            {
                Type = i % 3 == 0 ? "System.TimeoutException" : "System.IO.IOException",
                Message = "m" + i, StackTrace = new string('s', 2000),
            }.ToBytes();
            Assert.True(_engine.TryWrite(new LogEventHeader
            {
                TimestampUtcTicks        = baseTicks + i * TimeSpan.TicksPerSecond,
                Level                    = LogLevel.Error,
                MessageTemplatePoolIndex = tmpl,
                ServiceNamePoolIndex     = -1,
            }, buf.WrittenSpan, "order {n} failed", exc));
        }
    }

    public void Dispose()
    {
        try { _engine.DisposeAsync().AsTask().GetAwaiter().GetResult(); } catch { }
        QuerySegmentFixtures.DeleteDataDirectory(_dir);
    }

    [Fact]
    public async Task The_rows_a_query_yields_own_their_exception_bytes_and_outlive_the_tier()
    {
        var page = new List<LogEvent>();
        await foreach (var ev in _query.ExecuteAsync(new QueryRequest { Filter = "@x.type = 'System.TimeoutException'", Count = 50 }))
            page.Add(ev);
        Assert.Equal(50, page.Count);
        Assert.All(page, ev =>
        {
            Assert.False(ev.ExceptionMaterialised, "the filter built the tree a field read exists not to build");
            Assert.True(MemoryMarshal.TryGetArray(ev.RawException, out _), "a yielded row still points into the hot tier");
        });

        // The tier flushes and goes; the page serialises after that, as the events list does after releasing its snapshot.
        await _engine.FlushHotTierAsync();
        foreach (var ev in page)
        {
            var sink = new ArrayBufferWriter<byte>(4096);
            using var json = new Utf8JsonWriter(sink);
            LogEventJsonWriter.Write(json, ev);
            json.Flush();
            Assert.Contains("\"type\":\"System.TimeoutException\"", Encoding.UTF8.GetString(sink.WrittenSpan));
        }
    }
}
