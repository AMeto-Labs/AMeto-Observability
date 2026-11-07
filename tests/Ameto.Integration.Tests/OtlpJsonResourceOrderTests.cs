using System.Text;
using Ameto.Core;
using Ameto.Otel;
using Ameto.Ingestion;
using MessagePack;

namespace Ameto.Integration.Tests;

/// <summary>
/// #138 items 2 and 3, the JSON log road: a <c>resource</c> written AFTER the block's <c>scopeLogs</c>
/// used to reach none of the block's records (no resource attributes, no <c>service.name</c> — the parser
/// is single-pass), and <c>severityText</c> was compared with two spellings per level, so <c>"Error"</c>
/// and <c>"Warning"</c> — a title-casing logger with no <c>severityNumber</c> — stored as Information
/// over JSON and as their level over protobuf.
/// </summary>
public sealed class OtlpJsonResourceOrderTests
{
    /// <summary>A sink that keeps what the parser hands it.</summary>
    private sealed class Sink : IOtlpLogSink
    {
        public readonly List<byte[]> Maps     = [];
        public readonly List<byte[]> Services = [];
        public readonly List<byte>   Levels   = [];

        public int MaxEventPayloadBytes => 64 * 1024;
        public int MaxServiceNameBytes  => int.MaxValue;

        public void ChargeScratch(long bytes) { }

        public bool TryIngestRaw(long tsTicks, byte level, ReadOnlySpan<byte> templateUtf8, ReadOnlySpan<byte> msgpackProps,
                                 ulong traceHi, ulong traceLo, ulong spanId, ReadOnlySpan<byte> serviceUtf8)
        {
            Maps.Add(msgpackProps.ToArray());
            Services.Add(serviceUtf8.ToArray());
            Levels.Add(level);
            return true;
        }

        public bool TryIngestOversized(long tsTicks, byte level, ReadOnlySpan<byte> templateUtf8, long payloadBytes,
                                       ulong traceHi, ulong traceLo, ulong spanId, ReadOnlySpan<byte> serviceUtf8, int serviceIdx)
            => false;

        public void NotifyBatchEnqueued() { }
    }

    private static byte[] Utf8(string s) => Encoding.UTF8.GetBytes(s);

    private const string Resource =
        "\"resource\":{\"attributes\":[{\"key\":\"service.name\",\"value\":{\"stringValue\":\"orders\"}}," +
        "{\"key\":\"deployment.environment\",\"value\":{\"stringValue\":\"prod\"}}]}";

    private const string Scopes =
        "\"scopeLogs\":[{\"logRecords\":[{\"body\":{\"stringValue\":\"first\"},\"attributes\":[{\"key\":\"a\",\"value\":{\"stringValue\":\"1\"}}]}," +
        "{\"body\":{\"stringValue\":\"second\"}}]}]";

    /// <summary>The map the sink got, as its keys.</summary>
    private static HashSet<string> Keys(byte[] map)
    {
        var r = new MessagePackReader(map);
        int n = r.ReadMapHeader();
        var keys = new HashSet<string>();
        for (int i = 0; i < n; i++)
        {
            keys.Add(r.ReadString()!);
            r.Skip();
        }
        return keys;
    }

    [Fact]
    public void A_resource_after_the_scopes_reaches_every_record_of_the_block()
    {
        var sink = new Sink();
        var (ingested, dropped) = OtlpLogStreamParser.Parse(Utf8("{\"resourceLogs\":[{" + Scopes + "," + Resource + "}]}"), sink);

        Assert.Equal((2, 0), (ingested, dropped));
        Assert.All(sink.Services, s => Assert.Equal("orders", Encoding.UTF8.GetString(s)));
        Assert.All(sink.Maps, m => Assert.Contains("deployment.environment", Keys(m)));
        Assert.Contains("a", Keys(sink.Maps[0]));
    }

    [Fact]
    public void A_resource_before_the_scopes_is_read_as_before()
    {
        var sink = new Sink();
        var (ingested, _) = OtlpLogStreamParser.Parse(Utf8("{\"resourceLogs\":[{" + Resource + "," + Scopes + "}]}"), sink);

        Assert.Equal(2, ingested);
        Assert.All(sink.Services, s => Assert.Equal("orders", Encoding.UTF8.GetString(s)));
        Assert.All(sink.Maps, m => Assert.Contains("deployment.environment", Keys(m)));
    }

    [Fact]
    public void A_block_with_no_resource_at_all_still_has_no_service()
    {
        var sink = new Sink();
        var (ingested, _) = OtlpLogStreamParser.Parse(Utf8("{\"resourceLogs\":[{" + Scopes + ",\"schemaUrl\":\"x\"}]}"), sink);

        Assert.Equal(2, ingested);
        Assert.All(sink.Services, s => Assert.Empty(s));
        Assert.All(sink.Maps, m => Assert.DoesNotContain("deployment.environment", Keys(m)));
    }

    private const string SecondResource = "\"resource\":{\"attributes\":[{\"key\":\"host.name\",\"value\":{\"stringValue\":\"h1\"}}]}";

    [Fact]
    public void A_second_resource_after_the_scopes_reaches_no_record()
    {
        // A resource is read in place before the scopes, so none is looked for: the one after them merges
        // into the scratch once the records are written, and reaches none. Protobuf merges both into every
        // record (its pass 1); full parity would cost the common resource-first block a second pass over its
        // scopes, for input OTLP does not produce (a "resource" key given twice).
        var sink = new Sink();
        var (ingested, _) = OtlpLogStreamParser.Parse(Utf8("{\"resourceLogs\":[{" + Resource + "," + Scopes + "," + SecondResource + "}]}"), sink);

        Assert.Equal(2, ingested);
        var keys = Keys(sink.Maps[1]);
        Assert.Contains("deployment.environment", keys);
        Assert.DoesNotContain("host.name", keys);
    }

    [Fact]
    public void A_second_resource_after_a_looked_ahead_one_reaches_no_record()
    {
        // scopeLogs, resource, resource: the look-ahead reads the first; the real reader skips it and reads
        // the second in place, counted on from the first's budget, and it merges after the records.
        var sink = new Sink();
        var (ingested, _) = OtlpLogStreamParser.Parse(Utf8("{\"resourceLogs\":[{" + Scopes + "," + Resource + "," + SecondResource + "}]}"), sink);

        Assert.Equal(2, ingested);
        Assert.All(sink.Services, s => Assert.Equal("orders", Encoding.UTF8.GetString(s)));
        Assert.All(sink.Maps, m => Assert.Contains("deployment.environment", Keys(m)));
        Assert.All(sink.Maps, m => Assert.DoesNotContain("host.name", Keys(m)));
    }

    /// <summary>
    /// A block of many "scopeLogs" keys and no resource (JSON allows the duplicates; OTLP does not send them):
    /// the look-ahead walked the rest of the block from EACH of them, N²/2 in all — 64 000 keys took 28 s, an
    /// 8 MiB body about forty minutes of one core. Once per block: a later key's rest is part of what the first
    /// one walked.
    /// </summary>
    [Fact]
    public void Many_scopeLogs_keys_with_no_resource_are_looked_past_once()
    {
        var json = "{\"resourceLogs\":[{" + string.Join(",", Enumerable.Repeat(Scopes, 1_000)) + "}]}";
        OtlpLogStreamParser.ResourceLookAheadsForTest = 0;

        var (ingested, _) = OtlpLogStreamParser.Parse(Utf8(json), new Sink());

        Assert.Equal(2_000, ingested);
        Assert.Equal(1, OtlpLogStreamParser.ResourceLookAheadsForTest);   // per key it was 1 000
    }

    [Fact]
    public void A_resource_after_several_scopes_reaches_all_their_records()
    {
        var sink = new Sink();
        var json = "{\"resourceLogs\":[{" + Scopes + "," + Scopes + "," + Scopes + "," + Resource + "}]}";

        Assert.Equal((6, 0), OtlpLogStreamParser.Parse(Utf8(json), sink));
        Assert.All(sink.Services, s => Assert.Equal("orders", Encoding.UTF8.GetString(s)));
        Assert.All(sink.Maps, m => Assert.Contains("deployment.environment", Keys(m)));
    }

    [Theory]
    [InlineData("Error",       LogLevel.Error)]
    [InlineData("ERROR",       LogLevel.Error)]
    [InlineData("error",       LogLevel.Error)]
    [InlineData("Warning",     LogLevel.Warning)]
    [InlineData("WARN",        LogLevel.Warning)]
    [InlineData("warn",        LogLevel.Warning)]
    [InlineData("Information", LogLevel.Information)]
    [InlineData("INFO",        LogLevel.Information)]
    [InlineData("Debug",       LogLevel.Debug)]
    [InlineData("Trace",       LogLevel.Verbose)]
    [InlineData("Fatal",       LogLevel.Fatal)]
    [InlineData("Critical",    LogLevel.Information)]   // no rule: the default, as over protobuf
    [InlineData("Err\\u006fr", LogLevel.Error)]         // escaped: unescaped before the comparison
    [InlineData("ERROR\\u0020in\\u0020payments", LogLevel.Error)]   // escaped and longer than the 16-byte buffer: the prefix still decides
    [InlineData("ERROR in payments", LogLevel.Error)]
    public void severityText_is_read_by_the_protobuf_rule(string text, LogLevel expected)
    {
        var sink = new Sink();
        var json = "{\"resourceLogs\":[{\"scopeLogs\":[{\"logRecords\":[{\"severityText\":\"" + text + "\",\"body\":{\"stringValue\":\"x\"}}]}]}]}";
        var (ingested, _) = OtlpLogStreamParser.Parse(Utf8(json), sink);

        Assert.Equal(1, ingested);
        Assert.Equal((byte)expected, Assert.Single(sink.Levels));
        Assert.Equal(expected, OtlpSeverity.Map(0, Utf8(System.Text.RegularExpressions.Regex.Unescape(text))));   // the shared rule says the same
    }

    [Fact]
    public void A_severityNumber_still_wins_over_the_text()
    {
        var sink = new Sink();
        var json = "{\"resourceLogs\":[{\"scopeLogs\":[{\"logRecords\":[{\"severityNumber\":9,\"severityText\":\"Error\",\"body\":{\"stringValue\":\"x\"}}]}]}]}";
        OtlpLogStreamParser.Parse(Utf8(json), sink);

        Assert.Equal((byte)LogLevel.Information, Assert.Single(sink.Levels));
    }
}
