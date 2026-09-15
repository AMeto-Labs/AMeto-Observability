using System.Buffers;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using MessagePack;
using Ameto.Core;
using Ameto.Ingestion;
using Ameto.Storage;
using Microsoft.Extensions.DependencyInjection;

namespace Ameto.Integration.Tests;

/// <summary>
/// Its own host: this suite saturates the process's template pool, and every other suite
/// sharing that pool would then run past saturation too.
/// </summary>
public sealed class ExhaustedTemplatePoolFactory : AmetoWebAppFactory;

/// <summary>
/// Once the intern pool is full, Intern answers -1 and hands back the materialised template.
/// From then on the string attached to the event is the ONLY copy of its template: the header
/// carries -1, and the pool resolves -1 to "". A receiver that attached a template only for a
/// pooled index therefore lost @mt for every event ingested after saturation — in the hot tier
/// and, because the flush reads the same attached string, in the segment as well.
///
/// <para>Templates and service names share the pool (65 536 entries), so one interpolated
/// template per event, or a client sending only @m, gets there in 65k events.</para>
/// </summary>
public sealed class IngestTemplatePoolExhaustionTests : IClassFixture<ExhaustedTemplatePoolFactory>
{
    private readonly ExhaustedTemplatePoolFactory _factory;
    public IngestTemplatePoolExhaustionTests(ExhaustedTemplatePoolFactory factory) => _factory = factory;

    private const string ClefTemplate = "clef after exhaustion {N}";
    private const string RawTemplate  = "otlp json after exhaustion {N}";

    [Fact]
    public async Task EventsIngestedAfterThePoolIsFull_KeepTheirTemplate_InTheHotTierAndAfterFlush()
    {
        var client = _factory.CreateClient();
        var engine = _factory.Services.GetRequiredService<StorageEngine>();
        var pool   = _factory.Services.GetRequiredService<StringInternPool>();
        Assert.Same(engine.TemplatePool, pool);   // the receiver interns into the tier's pool

        for (int i = 0; i < 65_536; i++)
            pool.Intern("r1 filler " + i);
        Assert.Equal(-1, pool.Intern("proof the pool is full"));

        // CLEF over HTTP — TryIngestClef.
        var resp = await client.PostAsync("/api/events", ClefBody());
        resp.EnsureSuccessStatusCode();
        using (var reply = JsonDocument.Parse(await resp.Content.ReadAsStringAsync()))
            Assert.Equal(1, reply.RootElement.GetProperty("ingested").GetInt32());

        // The OTLP JSON streaming sink — TryIngestRaw.
        var endpoint = _factory.Services.GetRequiredService<IngestionEndpoint>();
        Assert.True(endpoint.TryIngestRaw(
            DateTimeOffset.UtcNow.UtcTicks, (byte)LogLevel.Information,
            Encoding.UTF8.GetBytes(RawTemplate), ProbeProps("r1-raw"),
            0, 0, 0, default));
        endpoint.NotifyBatchEnqueued();

        // Every read is checked before anything is asserted, so a failure names all four.
        var failures = new List<string>();

        // Hot tier.
        await CheckTemplateAsync(client, "r1-clef", ClefTemplate, "hot tier", failures);
        await CheckTemplateAsync(client, "r1-raw",  RawTemplate,  "hot tier", failures);

        // Segment.
        await engine.FlushHotTierAsync();
        await CheckTemplateAsync(client, "r1-clef", ClefTemplate, "after flush", failures);
        await CheckTemplateAsync(client, "r1-raw",  RawTemplate,  "after flush", failures);

        Assert.True(failures.Count == 0, string.Join(Environment.NewLine, failures));
    }

    private static async Task CheckTemplateAsync(
        HttpClient client, string probe, string expected, string where, List<string> failures)
    {
        var events = await TestHelpers.WaitForEventsAsync(client, expectedCount: 1, filter: $"Probe = '{probe}'");
        var ev     = Assert.Single(events);
        string? mt = ev.GetProperty("@mt").GetString();
        if (mt != expected)
            failures.Add($"{probe} ({where}): expected @mt '{expected}', got '{mt}' — {ev}");
    }

    private static ByteArrayContent ClefBody()
    {
        var buf = new ArrayBufferWriter<byte>(256);
        var w   = new MessagePackWriter(buf);
        w.WriteArrayHeader(1);
        w.WriteMapHeader(4);
        w.Write("@t");    w.Write(DateTimeOffset.UtcNow.ToString("O"));
        w.Write("@mt");   w.Write(ClefTemplate);
        w.Write("N");     w.Write(1L);
        w.Write("Probe"); w.Write("r1-clef");
        w.Flush();

        var c = new ByteArrayContent(buf.WrittenSpan.ToArray());
        c.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
        return c;
    }

    private static byte[] ProbeProps(string probe)
    {
        var buf = new ArrayBufferWriter<byte>(64);
        var w   = new MessagePackWriter(buf);
        w.WriteMapHeader(2);
        w.Write("N");     w.Write(1L);
        w.Write("Probe"); w.Write(probe);
        w.Flush();
        return buf.WrittenSpan.ToArray();
    }
}
