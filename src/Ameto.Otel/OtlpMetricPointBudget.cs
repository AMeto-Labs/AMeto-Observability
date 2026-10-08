using System.Text.Json;

namespace Ameto.Otel;

/// <summary>
/// The data points an OTLP metrics batch carries, counted before a single one is built — the
/// receivers refuse a batch over <c>Ingestion.MaxOtlpMetricPoints</c> with 413 (gRPC:
/// RESOURCE_EXHAUSTED) instead of decoding it (#126 review F2).
///
/// <para><b>Why.</b> A metric data point can be two bytes on the wire; decoded it is a
/// <c>MetricIngestItem</c> and a list slot, ~125 B from protobuf and ~200 B from JSON (measured on
/// 200 000 empty gauge points: 62.5× and 67.3×). A batch inside the 8 MiB body limit — a ~37 KB
/// gzip upload — could therefore decode to ~500 MiB, past the 512 MB stand's whole 384 MiB heap.
/// The decode ran out of memory, which #125 answers 503, and the exporter retried the same batch
/// for minutes, each attempt driving the heap to its limit beside everything else. Counted first,
/// it is refused for what it is — too large — at the cost of a walk that allocates nothing.</para>
///
/// <para>Both counts are upper bounds on what the parsers build: they count every data point of a
/// gauge, sum or histogram (the kinds the parsers decode), including those of a metric the parser
/// would skip for having no name.</para>
/// </summary>
internal static class OtlpMetricPointBudget
{
    /// <summary>What a refusal says, over HTTP and gRPC. Printable ASCII, under 128 bytes.</summary>
    internal const string RefusalMessage = "the batch holds more data points than this server decodes in one request; split it";

    /// <inheritdoc cref="RefusalMessage"/>
    internal static ReadOnlySpan<byte> RefusalMessageUtf8 => "the batch holds more data points than this server decodes in one request; split it"u8;

    /// <summary>Thrown by a decode that found its batch over the limit — the gRPC receiver answers it RESOURCE_EXHAUSTED.</summary>
    internal sealed class TooManyPointsException(int points, int limit)
        : Exception($"an OTLP metrics batch of {points} data points is over the limit of {limit}")
    {
        public int Points { get; } = points;
        public int Limit  { get; } = limit;
    }

    /// <summary>
    /// The data points of an <c>ExportMetricsServiceRequest</c> in protobuf: resource_metrics (1) →
    /// scope_metrics (2) → metrics (2) → gauge (5) / sum (7) / histogram (9) → data_points (1). Each
    /// is skipped by its length, never decoded. A malformed message throws as the parse would.
    /// </summary>
    internal static int CountProto(ReadOnlySpan<byte> payload)
    {
        long points = 0;
        var request = new ProtoReader(payload);
        uint tag;
        while ((tag = request.ReadTag()) != 0)
        {
            if (tag != 10) { request.SkipField(tag); continue; }                    // resource_metrics
            var resource = new ProtoReader(request.ReadLengthDelimited());
            while ((tag = resource.ReadTag()) != 0)
            {
                if (tag != 18) { resource.SkipField(tag); continue; }               // scope_metrics
                var scope = new ProtoReader(resource.ReadLengthDelimited());
                while ((tag = scope.ReadTag()) != 0)
                {
                    if (tag != 18) { scope.SkipField(tag); continue; }              // metrics
                    var metric = new ProtoReader(scope.ReadLengthDelimited());
                    while ((tag = metric.ReadTag()) != 0)
                    {
                        if (tag is not (42 or 58 or 74)) { metric.SkipField(tag); continue; }   // gauge, sum, histogram
                        var data = new ProtoReader(metric.ReadLengthDelimited());
                        while ((tag = data.ReadTag()) != 0)
                        {
                            if (tag == 10) { data.ReadLengthDelimited(); points++; }  // data_points
                            else data.SkipField(tag);
                        }
                    }
                }
            }
        }
        return (int)Math.Min(points, int.MaxValue);
    }

    /// <summary>
    /// The data points of an <c>ExportMetricsServiceRequest</c> in OTLP/JSON: every object directly
    /// inside an array named <c>dataPoints</c>, at any depth — the name the deserializer binds, read
    /// with the same leniency (trailing commas). A malformed document throws as the parse would.
    /// </summary>
    internal static int CountJson(ReadOnlySpan<byte> json)
    {
        var reader = new Utf8JsonReader(json, new JsonReaderOptions { AllowTrailingCommas = true });
        long points = 0;
        int  inside = -1;              // the depth of the dataPoints array being counted, or -1
        bool named  = false;           // the property just read is dataPoints
        while (reader.Read())
        {
            switch (reader.TokenType)
            {
                case JsonTokenType.PropertyName:
                    named = inside < 0 && reader.ValueTextEquals("dataPoints"u8);
                    break;
                case JsonTokenType.StartArray:
                    if (named) inside = reader.CurrentDepth;
                    named = false;
                    break;
                case JsonTokenType.StartObject:
                    if (inside >= 0 && reader.CurrentDepth == inside + 1) points++;
                    named = false;
                    break;
                case JsonTokenType.EndArray:
                    if (reader.CurrentDepth == inside) inside = -1;
                    break;
                default:
                    named = false;
                    break;
            }
        }
        return (int)Math.Min(points, int.MaxValue);
    }
}
