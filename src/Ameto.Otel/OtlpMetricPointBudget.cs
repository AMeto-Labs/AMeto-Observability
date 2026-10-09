using System.Text.Json;
using Ameto.Core;

namespace Ameto.Otel;

/// <summary>
/// What an OTLP metrics batch will decode to, weighed before a single point is built — the receivers
/// refuse a batch that weighs more than <c>Ingestion.MaxOtlpMetricPoints</c> plain data points with 413
/// (gRPC: RESOURCE_EXHAUSTED) instead of decoding it (#126 review F2, NEW-1).
///
/// <para><b>Why.</b> A metric data point can be two bytes on the wire; decoded it is a
/// <c>MetricIngestItem</c> and a list slot, ~125 B from protobuf and ~200 B from JSON (measured on
/// 200 000 empty gauge points: 62.5× and 67.3×). A batch inside the 8 MiB body limit — a ~37 KB
/// gzip upload — could therefore decode to ~500 MiB, past the 512 MB stand's whole 384 MiB heap.
/// The decode ran out of memory, which #125 answers 503, and the exporter retried the same batch
/// for minutes, each attempt driving the heap to its limit beside everything else. Weighed first,
/// it is refused for what it is — too large — at the cost of a walk that allocates nothing.</para>
///
/// <para><b>A histogram point's arrays expand too</b> (#126 review NEW-1). A bucket count is one byte
/// as a packed varint and 24 decoded (a doubling <c>List&lt;long&gt;</c>, then the exact-length copy),
/// a bound eight bytes and 16, an exemplar ~160: one point of 8 300 001 packed buckets, an 8.3 MB body
/// that counted as one point, allocated 200 MB to parse, and two such requests at once would have
/// passed the stand's heap. So a batch weighs
/// <c>points × 128 + buckets × 24 + bounds × 16 + exemplars × 160</c> bytes, against the limit's
/// <c>points × 128</c> — by default the request-body share of the heap (<c>IngestBufferBytes</c>).
/// A per-point bucket cap alone would not do: 128 points of 65 535 buckets fit the same 8.4 MB.</para>
///
/// <para>The weights are upper bounds on what the parsers build: every data point of a gauge, sum or
/// histogram, including a metric's that the parser skips for having no name; a packed bucket array by
/// its length (a varint is at least a byte, and a length that is a multiple of 8 is read as fixed64, as
/// the parser reads it); every exemplar, whether or not it carries the trace link the parser keeps.
/// JSON is weighed with the same table, though it decodes to about twice as much (a bucket count is a
/// string there); the default share of the heap absorbs that.</para>
/// </summary>
internal static class OtlpMetricPointBudget
{
    /// <summary>What a data point weighs: an item and its list slot (measured 125 B from protobuf).</summary>
    internal const int PointBytes = IngestionOptions.DecodedMetricPointBytes;

    /// <summary>What a histogram bucket count weighs: a doubling list's slot, then the exact-length copy.</summary>
    internal const int BucketBytes = 24;

    /// <summary>What a histogram bound weighs: a doubling list's slot, then the exact-length copy.</summary>
    internal const int BoundBytes = 16;

    /// <summary>What an exemplar weighs: the object and its two hex id strings.</summary>
    internal const int ExemplarBytes = 160;

    /// <summary>What a refusal says, over HTTP and gRPC. Printable ASCII, under 128 bytes.</summary>
    internal const string RefusalMessage = "the batch decodes to more data points and buckets than this server takes in one request; split it";

    /// <inheritdoc cref="RefusalMessage"/>
    internal static ReadOnlySpan<byte> RefusalMessageUtf8 => "the batch decodes to more data points and buckets than this server takes in one request; split it"u8;

    /// <summary>What one batch carries, as the walks below count it.</summary>
    internal struct Weight
    {
        public long Points;
        public long Buckets;
        public long Bounds;
        public long Exemplars;

        /// <summary>What the batch decodes to, about — never less (see the class remarks).</summary>
        public readonly long DecodedBytes =>
            Points * PointBytes + Buckets * BucketBytes + Bounds * BoundBytes + Exemplars * ExemplarBytes;
    }

    /// <summary>The decode a limit of <paramref name="maxPoints"/> plain points allows, in bytes.</summary>
    internal static long BudgetBytes(int maxPoints) => (long)maxPoints * PointBytes;

    /// <summary>Whether a batch of this weight may be decoded under a limit of <paramref name="maxPoints"/>.</summary>
    internal static bool Fits(in Weight weight, int maxPoints) => weight.DecodedBytes <= BudgetBytes(maxPoints);

    /// <summary>Thrown by a decode that found its batch over the limit — the gRPC receiver answers it RESOURCE_EXHAUSTED.</summary>
    internal sealed class TooManyPointsException(Weight weight, int limit)
        : Exception($"an OTLP metrics batch of {weight.Points} data points, decoding to about {weight.DecodedBytes} bytes, " +
                    $"is over the limit of {limit} points ({BudgetBytes(limit)} bytes)")
    {
        public Weight Weight { get; } = weight;
        public int    Limit  { get; } = limit;
    }

    /// <summary>
    /// The weight of an <c>ExportMetricsServiceRequest</c> in protobuf: resource_metrics (1) →
    /// scope_metrics (2) → metrics (2) → gauge (5) / sum (7) / histogram (9) → data_points (1), and in a
    /// histogram point its bucket_counts (6), explicit_bounds (7) and exemplars (8). Every field is
    /// skipped by its length, never decoded. A malformed message throws as the parse would.
    /// </summary>
    internal static Weight WeighProto(ReadOnlySpan<byte> payload)
    {
        var weight  = new Weight();
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
                        bool histogram = tag == 74;
                        var data = new ProtoReader(metric.ReadLengthDelimited());
                        while ((tag = data.ReadTag()) != 0)
                        {
                            if (tag != 10) { data.SkipField(tag); continue; }       // data_points
                            var point = data.ReadLengthDelimited();
                            weight.Points++;
                            if (histogram) WeighHistogramPoint(point, ref weight);
                        }
                    }
                }
            }
        }
        return weight;
    }

    /// <summary>A protobuf HistogramDataPoint's arrays, as the parser reads them.</summary>
    private static void WeighHistogramPoint(ReadOnlySpan<byte> point, ref Weight weight)
    {
        var r = new ProtoReader(point);
        uint tag;
        while ((tag = r.ReadTag()) != 0)
        {
            switch (tag)
            {
                case 48 or 49:                                                     // bucket_counts, unpacked
                    r.SkipField(tag);
                    weight.Buckets++;
                    break;
                case 50:                                                           // bucket_counts, packed
                {
                    int length = r.ReadLengthDelimited().Length;
                    weight.Buckets += length % 8 == 0 ? length / 8 : length;       // fixed64, or varints of a byte or more
                    break;
                }
                case 57:                                                           // explicit_bounds, unpacked
                    r.SkipField(tag);
                    weight.Bounds++;
                    break;
                case 58:                                                           // explicit_bounds, packed
                    weight.Bounds += (r.ReadLengthDelimited().Length + 7) / 8;
                    break;
                case 66:                                                           // exemplars
                    r.ReadLengthDelimited();
                    weight.Exemplars++;
                    break;
                default:
                    r.SkipField(tag);
                    break;
            }
        }
    }

    /// <summary>The data points of a protobuf batch (see <see cref="WeighProto"/>).</summary>
    internal static int CountProto(ReadOnlySpan<byte> payload) => (int)Math.Min(WeighProto(payload).Points, int.MaxValue);

    /// <summary>The data points of an OTLP/JSON batch (see <see cref="WeighJson"/>).</summary>
    internal static int CountJson(ReadOnlySpan<byte> json) => (int)Math.Min(WeighJson(json).Points, int.MaxValue);

    /// <summary>
    /// The weight of an <c>ExportMetricsServiceRequest</c> in OTLP/JSON, walked along the shape the
    /// deserializer binds: <c>resourceMetrics</c> → <c>scopeMetrics</c> → <c>metrics</c> →
    /// <c>gauge</c> / <c>sum</c> / <c>histogram</c> → <c>dataPoints</c>, and in a histogram point its
    /// <c>bucketCounts</c>, <c>explicitBounds</c> and <c>exemplars</c> — the property names matched
    /// exactly as it matches them (escapes undone), with the same leniency (trailing commas). Points of
    /// a kind the model does not have — <c>exponentialHistogram</c>, <c>summary</c> — are skipped
    /// unread, as the deserializer skips them (#126 review NEW-6). A property given twice is counted
    /// twice: the deserializer builds both before the second replaces the first. A malformed document
    /// throws as the parse would.
    /// </summary>
    internal static Weight WeighJson(ReadOnlySpan<byte> json)
    {
        var reader = new Utf8JsonReader(json, new JsonReaderOptions { AllowTrailingCommas = true });
        var weight = new Weight();
        if (reader.Read() && reader.TokenType == JsonTokenType.StartObject)
            JsonObject(ref reader, JsonLevel.Request, ref weight);
        return weight;
    }

    /// <summary>The objects of the OTLP/JSON metrics request whose members the walk reads.</summary>
    private enum JsonLevel
    {
        Request, ResourceMetrics, ScopeMetrics, Metric,
        NumberPoints, NumberPoint,             // a gauge's or sum's point set, and one of its points
        HistogramPoints, HistogramPoint,       // a histogram's point set, and one of its points
    }

    /// <summary>The arrays of a histogram point that the walk counts.</summary>
    private enum JsonArray { None, Buckets, Bounds, Exemplars }

    /// <summary>
    /// The members of one object, the reader on its <c>StartObject</c> and left on its <c>EndObject</c>:
    /// the members that lead to data points are followed, every other value is skipped unread.
    /// </summary>
    private static void JsonObject(ref Utf8JsonReader reader, JsonLevel level, ref Weight weight)
    {
        while (reader.Read() && reader.TokenType == JsonTokenType.PropertyName)
        {
            // What the member's value is at this level: an array of objects to walk, one object to
            // walk, or one of a histogram point's arrays to count. Anything else is skipped.
            JsonLevel? array = level switch
            {
                JsonLevel.Request         when reader.ValueTextEquals("resourceMetrics"u8) => JsonLevel.ResourceMetrics,
                JsonLevel.ResourceMetrics when reader.ValueTextEquals("scopeMetrics"u8)    => JsonLevel.ScopeMetrics,
                JsonLevel.ScopeMetrics    when reader.ValueTextEquals("metrics"u8)         => JsonLevel.Metric,
                JsonLevel.NumberPoints    when reader.ValueTextEquals("dataPoints"u8)      => JsonLevel.NumberPoint,
                JsonLevel.HistogramPoints when reader.ValueTextEquals("dataPoints"u8)      => JsonLevel.HistogramPoint,
                _ => null,
            };
            JsonLevel? single = level switch
            {
                JsonLevel.Metric when reader.ValueTextEquals("gauge"u8)
                                   || reader.ValueTextEquals("sum"u8)       => JsonLevel.NumberPoints,
                JsonLevel.Metric when reader.ValueTextEquals("histogram"u8) => JsonLevel.HistogramPoints,
                _ => null,
            };
            JsonArray counted = level != JsonLevel.HistogramPoint         ? JsonArray.None
                              : reader.ValueTextEquals("bucketCounts"u8)   ? JsonArray.Buckets
                              : reader.ValueTextEquals("explicitBounds"u8) ? JsonArray.Bounds
                              : reader.ValueTextEquals("exemplars"u8)      ? JsonArray.Exemplars
                              : JsonArray.None;

            reader.Read();                                                     // the value
            if (array is { } element && reader.TokenType == JsonTokenType.StartArray)
            {
                while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
                {
                    if (reader.TokenType != JsonTokenType.StartObject) { reader.Skip(); continue; }
                    if (element is JsonLevel.NumberPoint or JsonLevel.HistogramPoint) weight.Points++;
                    if (element == JsonLevel.NumberPoint) reader.Skip();          // nothing in a gauge or sum point expands
                    else JsonObject(ref reader, element, ref weight);
                }
            }
            else if (single is { } inner && reader.TokenType == JsonTokenType.StartObject)
            {
                JsonObject(ref reader, inner, ref weight);
            }
            else if (counted != JsonArray.None && reader.TokenType == JsonTokenType.StartArray)
            {
                while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
                {
                    switch (counted)
                    {
                        case JsonArray.Buckets: weight.Buckets++; break;
                        case JsonArray.Bounds:  weight.Bounds++;  break;
                        default: if (reader.TokenType == JsonTokenType.StartObject) weight.Exemplars++; break;
                    }
                    reader.Skip();
                }
            }
            else reader.Skip();
        }
    }
}
