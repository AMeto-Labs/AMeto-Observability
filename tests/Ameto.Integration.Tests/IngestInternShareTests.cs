using System.Text;
using Ameto.Core;
using Ameto.Ingestion;
using Ameto.Storage;
using Microsoft.Extensions.Logging.Abstractions;

namespace Ameto.Integration.Tests;

/// <summary>
/// Round-6 review R5-M9 (round-5 M-E, only partly closed). The intern pool keeps every template and
/// service for the life of the process, within a budget of texts and of their characters — and any
/// request could spend that budget: three OTLP requests of distinct 16 000-character bodies took it all,
/// and every new template and service after them, for every source, stayed unpooled — carried in each
/// WAL entry (86 → 206 B an event for a 100-character template), a string per event (80 K events/s
/// against 561 K for a 1 000-character one). A request now adds no more to the pool than its share
/// (<see cref="StringInternPool.NewRequestShare"/>): the rest of what it brings is kept beside its
/// events, unpooled, as on a full pool, and charged to its batch.
/// </summary>
public sealed class IngestInternShareTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-intern-share-" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort */ }
    }

    /// <summary>One request of a hundred distinct 1 000-character templates: within the default budget, and past a request's share of it.</summary>
    [Fact]
    public async Task A_request_of_distinct_long_texts_adds_no_more_text_to_the_pool_than_its_share()
    {
        var (storage, endpoint) = Endpoint();
        await using var owned = storage;
        var  pool   = storage.TemplatePool;
        long before = pool.PooledChars;

        var texts = Enumerable.Range(0, 100).Select(i => i.ToString("D4") + new string('t', 996)).ToArray();
        using (var batch = endpoint.BeginBatch())
        {
            foreach (var t in texts) Assert.True(Stage(batch, t));
            await batch.CommitAsync();
        }

        long added = pool.PooledChars - before;
        Assert.True(added <= 16 * 1024, $"one request added {added:N0} units of new text to the pool, past a request's share of {16 * 1024:N0}");
        int pooled = texts.Count(t => pool.TryGet(t.AsSpan(), out _, out _));
        Assert.InRange(pooled, 1, 16);                                    // as many as the share holds, the rest kept beside their events

        // The next request has a share of its own: it pools the next of them.
        using (var next = endpoint.BeginBatch())
        {
            foreach (var t in texts) Assert.True(Stage(next, t));
            await next.CommitAsync();
        }
        Assert.True(texts.Count(t => pool.TryGet(t.AsSpan(), out _, out _)) > pooled, "a later request could add nothing to the pool");
    }

    /// <summary>One request of three hundred distinct short templates: past a request's share of the pool's count.</summary>
    [Fact]
    public async Task A_request_of_many_distinct_texts_adds_no_more_of_them_to_the_pool_than_its_share()
    {
        var (storage, endpoint) = Endpoint();
        await using var owned = storage;
        var pool = storage.TemplatePool;

        var texts = Enumerable.Range(0, 300).Select(i => "template " + i + " {n}").ToArray();
        using (var batch = endpoint.BeginBatch())
        {
            foreach (var t in texts) Assert.True(Stage(batch, t));
            await batch.CommitAsync();
        }

        int pooled = texts.Count(t => pool.TryGet(t.AsSpan(), out _, out _));
        Assert.True(pooled <= StringInternPool.DefaultMaxPoolSize / 256, $"one request added {pooled} new texts to the pool, past a request's share of {StringInternPool.DefaultMaxPoolSize / 256}");
    }

    /// <summary>
    /// Round-6 review (F4): only StageRaw's template intern was pinned to the share — a mutant that dropped it at every
    /// other site passed 450 tests. Each site a request's texts reach the pool through takes them under its share: three
    /// hundred distinct texts through one site, and no more of them are pooled than a request's share of the count.
    /// </summary>
    [Theory]
    [InlineData("raw service")]
    [InlineData("oversized service")]
    [InlineData("clef template")]
    [InlineData("clef service")]
    [InlineData("clef drop marker's service")]
    [InlineData("otlp block service")]
    public async Task Every_site_a_requests_texts_reach_the_pool_through_takes_them_under_its_share(string site)
    {
        var (storage, endpoint) = Endpoint(maxEventPayloadBytes: 1024);
        await using var owned = storage;
        var pool = storage.TemplatePool;

        var  texts = Enumerable.Range(0, 300).Select(i => site.Replace(' ', '.') + "." + i).ToArray();
        long ts    = DateTimeOffset.UtcNow.UtcTicks;
        byte info  = (byte)Ameto.Core.LogLevel.Information;
        byte[] props = [0x80], tooLong = new byte[2048];
        tooLong[0] = 0x80;
        using (var batch = endpoint.BeginBatch())
        {
            foreach (var t in texts)
            {
                byte[] text = Encoding.UTF8.GetBytes(t);
                switch (site)
                {
                    case "raw service":                 Assert.True(batch.TryIngestRaw(ts, info, "t"u8, props, 0, 0, 0, text)); break;
                    case "oversized service":           Assert.False(batch.TryIngestOversized(ts, info, "t"u8, 1L << 20, 0, 0, 0, text, serviceIdx: -1)); break;
                    case "clef template":               Assert.True(batch.TryIngestClef(ts, info, text, null, props, 0, 0, 0, "svc"u8)); break;
                    case "clef service":                Assert.True(batch.TryIngestClef(ts, info, "t"u8, null, props, 0, 0, 0, text)); break;
                    case "clef drop marker's service":  Assert.False(batch.TryIngestClef(ts, info, "t"u8, null, tooLong, 0, 0, 0, text)); break;
                    case "otlp block service":          batch.InternService(text); break;
                    default: throw new ArgumentOutOfRangeException(nameof(site));
                }
            }
            await batch.CommitAsync();
        }

        int pooled = texts.Count(t => pool.TryGet(t.AsSpan(), out _, out _));
        Assert.True(pooled <= StringInternPool.DefaultMaxPoolSize / 256,
                    $"{site}: one request added {pooled} of its 300 new texts to the pool, past a request's share of {StringInternPool.DefaultMaxPoolSize / 256}");
    }

    private (StorageEngine, IngestionEndpoint) Endpoint(int? maxEventPayloadBytes = null)
    {
        string dir  = Directory.CreateDirectory(Path.Combine(_dir, Guid.NewGuid().ToString("N"))).FullName;
        var opts    = maxEventPayloadBytes is { } max
            ? new ServerOptions { DataDirectory = dir, Ingestion = new IngestionOptions { MaxEventPayloadBytes = max } }
            : new ServerOptions { DataDirectory = dir };
        var storage = new StorageEngine(
            Microsoft.Extensions.Options.Options.Create(opts),
            new RetentionStore(opts, NullLogger<RetentionStore>.Instance),
            NullLogger<StorageEngine>.Instance);
        return (storage, new IngestionEndpoint(storage, storage.TemplatePool, opts, NullLogger<IngestionEndpoint>.Instance));
    }

    private static bool Stage(LogIngestBatch batch, string template) =>
        batch.TryIngestRaw(DateTimeOffset.UtcNow.UtcTicks, (byte)Ameto.Core.LogLevel.Information, Encoding.UTF8.GetBytes(template), [0x80], 0, 0, 0, "svc"u8);
}
