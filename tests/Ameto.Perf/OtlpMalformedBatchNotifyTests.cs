using System.Text;
using System.Text.Json;
using Ameto.Ingestion;
using Ameto.Otel;
using Xunit;

namespace Ameto.Perf;

/// <summary>
/// Both OTLP log parsers ingest AS THEY WALK, so a batch that turns malformed part way through
/// leaves its intact prefix in the ring and then throws. The receiver catches that and answers
/// 400 (HTTP) or INVALID_ARGUMENT (gRPC) without ever returning to the parser — so if the
/// drainer is only woken on the success path, those events sit in the ring until the drain
/// loop's 1 s missed-signal timeout notices them. They are not lost; they are invisible to
/// every query and to the live tail for up to a second, on an otherwise idle server.
///
/// <para>The CLEF receiver has held this invariant since I2 — <c>IngestionEndpoint</c> wakes the
/// drainer from inside its malformed-payload catch, and
/// <c>IngestMalformedBatchReportingTests.MalformedTail_WakesTheDrainerForThePrefix</c> pins it —
/// but that test posts to <c>/api/events</c> only, so neither OTLP road was covered. These are
/// that test's counterpart for the other two receivers, at the parser seam where the notify
/// lives.</para>
///
/// <para>The other direction matters just as much and is asserted too: a batch that ingested
/// NOTHING must leave the drainer alone, or every malformed upload becomes a wake-up.</para>
/// </summary>
public sealed class OtlpMalformedBatchNotifyTests
{
    private sealed class CountingSink : IOtlpLogSink
    {
        public int Ingested;
        public int Batches;

        public bool TryIngestRaw(long tsTicks, byte level, ReadOnlySpan<byte> templateUtf8,
            ReadOnlySpan<byte> msgpackProps, ulong traceHi, ulong traceLo, ulong spanId,
            ReadOnlySpan<byte> serviceUtf8)
        { Ingested++; return true; }

        public void NotifyBatchEnqueued() => Batches++;
    }

    // ── protobuf: what SDK exporters and the collector send ───────────────────

    [Fact]
    public void AProtobufBatchThatThrowsPastItsPrefix_StillWakesTheDrainer()
    {
        const int Good = 500;
        var sink = new CountingSink();

        // Records 0..499 are complete; the 500th carries a value nested one level past the
        // parser's cap, which throws out of EnterValue mid-walk.
        Assert.Throws<InvalidDataException>(
            () => OtlpLogProtoParser.Parse(OtlpProtoPayloads.Logs_PrefixThenOverDeepValue(Good), sink));

        Assert.Equal(Good, sink.Ingested);
        Assert.Equal(1, sink.Batches);
    }

    [Fact]
    public void AProtobufBatchThatIngestedNothing_LeavesTheDrainerAlone()
    {
        var sink = new CountingSink();

        // The outer length prefix overruns the buffer, so this throws before any record.
        Assert.Throws<InvalidDataException>(
            () => OtlpLogProtoParser.Parse(OtlpProtoPayloads.Logs_TruncatedLengthPrefix(), sink));

        Assert.Equal(0, sink.Ingested);
        Assert.Equal(0, sink.Batches);
    }

    // ── JSON ──────────────────────────────────────────────────────────────────

    [Fact]
    public void AJsonBatchThatThrowsPastItsPrefix_StillWakesTheDrainer()
    {
        const int Good = 5;
        var sink = new CountingSink();
        byte[] json = Encoding.UTF8.GetBytes(GoodPrefixThenCutInAString(Good));

        Assert.ThrowsAny<JsonException>(() => OtlpLogStreamParser.Parse(json, sink));

        Assert.Equal(Good, sink.Ingested);
        Assert.Equal(1, sink.Batches);
    }

    [Fact]
    public void AJsonBatchThatIngestedNothing_LeavesTheDrainerAlone()
    {
        var sink = new CountingSink();
        byte[] json = Encoding.UTF8.GetBytes(GoodPrefixThenCutInAString(0));

        Assert.ThrowsAny<JsonException>(() => OtlpLogStreamParser.Parse(json, sink));

        Assert.Equal(0, sink.Ingested);
        Assert.Equal(0, sink.Batches);
    }

    /// <summary>
    /// A well-formed OTLP/JSON document up to and including <paramref name="good"/> records,
    /// then one more record cut off inside a string token — the body arrived in full and is
    /// malformed in the middle, which is the case that ingests a prefix.
    /// </summary>
    private static string GoodPrefixThenCutInAString(int good)
    {
        var sb = new StringBuilder(good * 200 + 256);
        sb.Append("""{"resourceLogs":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"Etisalat.API"}}]},"scopeLogs":[{"logRecords":[""");
        for (int i = 0; i < good; i++)
        {
            if (i > 0) sb.Append(',');
            sb.Append("""{"timeUnixNano":"1783953780000000000","severityNumber":9,"body":{"stringValue":"handled"},"attributes":[{"key":"n","value":{"intValue":""");
            sb.Append('"').Append(i).Append('"');
            sb.Append("""}}]}""");
        }
        if (good > 0) sb.Append(',');
        sb.Append("""{"timeUnixNano":"1783953780000000000","body":{"stringValue":"never clos""");   // cut mid-string
        return sb.ToString();
    }
}
