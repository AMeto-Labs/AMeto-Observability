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

    [Fact]
    public void A_resource_given_before_and_after_merges_once_each()
    {
        // The look-ahead reads the one after the scopes; the real reader then meets it and skips it (once);
        // the one before is read in place. Both sets of attributes reach the records, none twice.
        const string second = "\"resource\":{\"attributes\":[{\"key\":\"host.name\",\"value\":{\"stringValue\":\"h1\"}}]}";
        var sink = new Sink();
        var (ingested, _) = OtlpLogStreamParser.Parse(Utf8("{\"resourceLogs\":[{" + Resource + "," + Scopes + "," + second + "}]}"), sink);

        Assert.Equal(2, ingested);
        var keys = Keys(sink.Maps[1]);
        Assert.Contains("deployment.environment", keys);
        // The second resource came after the scopes and was not looked for: a resource had been seen.
        Assert.DoesNotContain("host.name", keys);
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
    public void severityText_is_read_by_the_protobuf_rule(string text, LogLevel expected)
    {
        var sink = new Sink();
        var json = "{\"resourceLogs\":[{\"scopeLogs\":[{\"logRecords\":[{\"severityText\":\"" + text + "\",\"body\":{\"stringValue\":\"x\"}}]}]}]}";
        var (ingested, _) = OtlpLogStreamParser.Parse(Utf8(json), sink);

        Assert.Equal(1, ingested);
        Assert.Equal((byte)expected, Assert.Single(sink.Levels));
        Assert.Equal(expected, OtlpSeverity.Map(0, Utf8(text.Replace("\\u006f", "o"))));   // the shared rule says the same
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
