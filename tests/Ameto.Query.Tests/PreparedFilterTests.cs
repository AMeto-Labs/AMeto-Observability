using System.Buffers;
using MessagePack;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Ameto.Core;
using Ameto.Indexing;
using Ameto.Query.Filtering;
using Ameto.Storage;

namespace Ameto.Query.Tests;

/// <summary>
/// <see cref="QueryRequest.Prepared"/> lets the live tail compile its filter once. The
/// executor may only ever trust it when it was compiled from the request's own text —
/// a stale or foreign prepared filter must cost a compile, never change what the query
/// returns.
/// </summary>
public sealed class PreparedFilterTests : IAsyncLifetime
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-prepared-" + Guid.NewGuid().ToString("N"));
    private StorageEngine _engine = null!;
    private QueryExecutor _query  = null!;

    public Task InitializeAsync()
    {
        Directory.CreateDirectory(_dir);
        var opts = new ServerOptions { DataDirectory = _dir };
        _engine = new StorageEngine(
            Options.Create(opts),
            new RetentionStore(opts, NullLogger<RetentionStore>.Instance),
            NullLogger<StorageEngine>.Instance);
        _query = new QueryExecutor(_engine, new SegmentIndexReaderFactory(), NullLogger<QueryExecutor>.Instance);

        long b   = new DateTimeOffset(2026, 9, 1, 0, 0, 0, TimeSpan.Zero).UtcTicks;
        var  buf = new ArrayBufferWriter<byte>(32);
        var  w   = new MessagePackWriter(buf); w.WriteMapHeader(0); w.Flush();
        for (int i = 0; i < 30; i++)
        {
            Assert.True(_engine.TryWrite(new LogEventHeader
            {
                TimestampUtcTicks        = b + i * TimeSpan.TicksPerSecond,
                Level                    = (LogLevel)(i % 3 + 2),      // Information, Warning, Error
                MessageTemplatePoolIndex = _engine.TemplatePool.Intern("evt"),
            }, buf.WrittenSpan.ToArray()));
        }
        return Task.CompletedTask;
    }

    public async Task DisposeAsync()
    {
        await _engine.DisposeAsync();
        try { Directory.Delete(_dir, true); } catch { }
    }

    private async Task<List<LogLevel>> RunAsync(string? filter, IPreparedFilter? prepared)
    {
        var levels = new List<LogLevel>();
        await foreach (var ev in _query.ExecuteAsync(new QueryRequest
        {
            Filter = filter, Prepared = prepared, Count = 100, Direction = QueryDirection.Forward,
        }))
            levels.Add(ev.Level);
        return levels;
    }

    [Fact]
    public async Task A_prepared_filter_compiled_from_the_same_text_is_used_as_is()
    {
        var prepared = CompiledFilter.Compile("@l = 'Error'");
        var got = await RunAsync("@l = 'Error'", prepared);
        Assert.Equal(10, got.Count);
        Assert.All(got, l => Assert.Equal(LogLevel.Error, l));
    }

    [Fact]
    public async Task A_prepared_filter_from_different_text_is_ignored_and_the_text_wins()
    {
        // Prepared says Warning; the request text says Error. The text is the contract.
        var foreign = CompiledFilter.Compile("@l = 'Warning'");
        var got = await RunAsync("@l = 'Error'", foreign);
        Assert.Equal(10, got.Count);
        Assert.All(got, l => Assert.Equal(LogLevel.Error, l));

        // ...including when the request carries no filter at all.
        var all = await RunAsync(null, foreign);
        Assert.Equal(30, all.Count);
    }

    [Fact]
    public async Task A_prepared_filter_that_is_not_the_executors_own_type_is_ignored()
    {
        var got = await RunAsync("@l = 'Error'", new ForeignPrepared("@l = 'Error'"));
        Assert.Equal(10, got.Count);
        Assert.All(got, l => Assert.Equal(LogLevel.Error, l));
    }

    private sealed class ForeignPrepared(string? expression) : IPreparedFilter
    {
        public string? Expression => expression;
    }
}
