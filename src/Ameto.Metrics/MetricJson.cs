using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.AspNetCore.Http;

namespace Ameto.Metrics;

/// <summary>
/// Source-generated JSON for the metrics endpoints (issue #83 WP7, M#9): their request bodies and
/// the answers that are not series. Every endpoint used to go through <c>Results.Json(&lt;object&gt;)</c>
/// and <c>ReadFromJsonAsync&lt;T&gt;</c> with no type info — reflection metadata built at first use
/// and looked up by type on every call.
///
/// <para><b>Use <see cref="Web"/>, never <c>Default</c>.</b> The bytes must be what ASP.NET Core's
/// <c>JsonOptions</c> produced, and those are the web defaults (camelCase, case-insensitive reads,
/// numbers accepted as strings) PLUS <see cref="JavaScriptEncoder.UnsafeRelaxedJsonEscaping"/>,
/// which ASP.NET Core sets and STJ's defaults do not: <c>&lt;</c>, <c>&amp;</c>, <c>'</c>,
/// <c>+</c> and non-Latin text go out raw. A generated context carries its own options, so
/// <c>Default</c> would escape every one of them — <c>MetricResponseShapeTests</c> found that and
/// pins it.</para>
/// </summary>
[JsonSourceGenerationOptions(JsonSerializerDefaults.Web)]
[JsonSerializable(typeof(MetricQueryDto))]
[JsonSerializable(typeof(MetricExprDto))]
[JsonSerializable(typeof(List<MetricCatalogDto>))]
[JsonSerializable(typeof(MetricSeriesDto))]
[JsonSerializable(typeof(List<MetricSeriesDto>))]
[JsonSerializable(typeof(List<ExemplarDto>))]
[JsonSerializable(typeof(HeatmapDto))]
[JsonSerializable(typeof(List<string>))]
[JsonSerializable(typeof(IReadOnlyList<string>))]
internal partial class MetricJson : JsonSerializerContext
{
    /// <summary>The context over the options the endpoints have always answered with. See the class remarks.</summary>
    public static MetricJson Web { get; } = new(new JsonSerializerOptions(JsonSerializerDefaults.Web)
    {
        Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
    });
}

/// <summary>
/// A double the way the serializer writes one — <see cref="Utf8JsonWriter.WriteNumberValue(double)"/>,
/// the same bytes — except that NaN and ±Infinity, which JSON has no number for and the serializer
/// throws on, are written as <c>null</c> (#92). On the response DTOs' doubles only (exemplar value,
/// heatmap bounds and counts): one non-finite sample must not fail the whole answer. Reads accept a
/// number, or <c>null</c> as NaN; nothing reads these DTOs.
/// </summary>
internal sealed class NonFiniteAsNullConverter : JsonConverter<double>
{
    public override double Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) =>
        reader.TokenType == JsonTokenType.Null ? double.NaN : reader.GetDouble();

    public override void Write(Utf8JsonWriter writer, double value, JsonSerializerOptions options)
    {
        if (double.IsFinite(value)) writer.WriteNumberValue(value);
        else                        writer.WriteNullValue();
    }
}

/// <summary><see cref="NonFiniteAsNullConverter"/> for a <c>double[]</c>, element by element.</summary>
internal sealed class NonFiniteAsNullArrayConverter : JsonConverter<double[]>
{
    public override double[]? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        if (reader.TokenType == JsonTokenType.Null) return null;
        if (reader.TokenType != JsonTokenType.StartArray) throw new JsonException("expected an array of numbers");
        var values = new List<double>();
        while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
            values.Add(reader.TokenType == JsonTokenType.Null ? double.NaN : reader.GetDouble());
        return values.ToArray();
    }

    public override void Write(Utf8JsonWriter writer, double[] value, JsonSerializerOptions options)
    {
        writer.WriteStartArray();
        foreach (double d in value)
        {
            if (double.IsFinite(d)) writer.WriteNumberValue(d);
            else                    writer.WriteNullValue();
        }
        writer.WriteEndArray();
    }
}

/// <summary>
/// The series answers — <c>GET /api/metrics/{name}</c>, <c>POST /api/metrics/query</c>,
/// <c>POST /api/metrics/expr</c> — written straight from <see cref="MetricSeries"/> with a
/// <see cref="Utf8JsonWriter"/> (issue #83 WP7, M#9).
///
/// <para><b>No DTO graph.</b> Each answer used to be materialised as <c>MetricSeriesDto</c>s first:
/// a label dictionary per series and one <c>MetricPointDto</c> object per data point — 120 000
/// short-lived objects for a 2 000-series answer before a byte was written. The rows now go from
/// the series to the wire, and the raw series endpoint writes each series as storage produces it.</para>
///
/// <para><b>The same bytes.</b> Property names, order and casing as the DTOs serialized them
/// (name, kind, unit, labels, points; ts, value, count, sum); labels as an object in the label
/// set's order, keys written as-is (no naming policy touched dictionary keys); the kind as its enum
/// name, or its number when it has none (<c>Enum.ToString</c>); numbers through the same writer
/// calls the serializer makes, so 0.1, 1E+300, -0 and long.MaxValue read the same; the relaxed
/// encoder, as <see cref="MetricJson"/>. <c>MetricResponseShapeTests</c> pins all of it.</para>
///
/// <para><b>A NaN or an infinity is written as <c>null</c></b> (<c>value</c> and <c>sum</c>; <c>ts</c>
/// and <c>count</c> are integers) — per point, so the rest of the series and every other series of
/// the answer are written as ever (#92). JSON has no number for them, and the serializer used to
/// throw: one exporter dividing by zero, an empty histogram's mean, a counter reset some SDKs
/// report as NaN — and the whole answer was a 500, the whole panel empty, or, past the first flush
/// of the streamed answer, a dropped connection. <c>null</c> rather than the string <c>"NaN"</c>:
/// the Angular client reads these fields as numbers, and <c>null</c> is JSON's "no value" — the
/// same answer the exemplar and heatmap DTOs give through <see cref="NonFiniteAsNullConverter"/>.
/// A finite value's bytes are what they were.</para>
///
/// <para><b>A repeated label key is written once — the last of its run — and never fails the
/// answer</b> (#92). It used to throw, as the <c>ToDictionary</c> that built the DTO did: one such
/// series failed a whole panel with a 500, or — past the first flush of the streamed raw answer —
/// dropped the connection. Ingest no longer produces such a set (the OTLP label builder lets the
/// last value of a repeated key win), so what is left is a set stored before that fix, or one a
/// caller built by hand; neither may take the answer down with it. The pairs are sorted by key,
/// then value, so the value written is the one a browser's <c>JSON.parse</c> would have kept had
/// the key gone out twice. A null key (no reader produces one) is skipped for the same reason.</para>
///
/// <para><b>When bytes leave.</b> Output collects in the response pipe and is flushed to the network
/// once more than <see cref="FlushThresholdBytes"/> have accumulated — the serializer's own habit —
/// so an answer that fails before then (storage throwing, say) fails as a clean 500 with nothing
/// sent, exactly as before. A failure after a flush aborts the response mid-body; no series can
/// cause one any more — only the storage behind it.</para>
/// </summary>
internal static class MetricSeriesJson
{
    /// <summary>Bytes gathered before a flush to the network: 90 % of STJ's 16 KiB default buffer, as it flushes.</summary>
    internal const int FlushThresholdBytes = 14_745;

    private const string ContentType = "application/json; charset=utf-8";

    internal static readonly JsonWriterOptions WriterOptions = new()
    {
        Encoder        = JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
        SkipValidation = true,
    };

    private static readonly JsonEncodedText NameProp   = JsonEncodedText.Encode("name");
    private static readonly JsonEncodedText KindProp   = JsonEncodedText.Encode("kind");
    private static readonly JsonEncodedText UnitProp   = JsonEncodedText.Encode("unit");
    private static readonly JsonEncodedText LabelsProp = JsonEncodedText.Encode("labels");
    private static readonly JsonEncodedText PointsProp = JsonEncodedText.Encode("points");
    private static readonly JsonEncodedText TsProp     = JsonEncodedText.Encode("ts");
    private static readonly JsonEncodedText ValueProp  = JsonEncodedText.Encode("value");
    private static readonly JsonEncodedText CountProp  = JsonEncodedText.Encode("count");
    private static readonly JsonEncodedText SumProp    = JsonEncodedText.Encode("sum");

    /// <summary>The series as the array the endpoints answer, written as they are produced.</summary>
    public static IResult Array(IAsyncEnumerable<MetricSeries> series) => new AsyncArrayResult(series);

    /// <summary>The series as the array the endpoints answer.</summary>
    public static IResult Array(IReadOnlyList<MetricSeries> series) => new ListArrayResult(series);

    /// <summary>One series as a JSON object (the expression endpoint's answer).</summary>
    public static IResult Single(MetricSeries series) => new SingleResult(series);

    /// <summary>One series, as the DTO serialized it.</summary>
    public static void Write(Utf8JsonWriter w, MetricSeries s)
    {
        w.WriteStartObject();
        w.WriteString(NameProp, s.Name);
        w.WriteString(KindProp, KindName(s.Kind));
        w.WriteString(UnitProp, s.Unit);

        w.WritePropertyName(LabelsProp);
        w.WriteStartObject();
        WriteLabels(w, s.Labels.Interleaved);
        w.WriteEndObject();

        w.WritePropertyName(PointsProp);
        w.WriteStartArray();
        var points = s.Points;
        for (int i = 0; i < points.Count; i++)
        {
            var p = points[i];
            w.WriteStartObject();
            w.WriteNumber(TsProp, p.TimestampUnixNano);
            WriteNumberOrNull(w, ValueProp, p.Value);
            w.WriteNumber(CountProp, p.Count);
            WriteNumberOrNull(w, SumProp, p.Sum);
            w.WriteEndObject();
        }
        w.WriteEndArray();
        w.WriteEndObject();
    }

    /// <summary>
    /// A finite double as the number the serializer wrote; NaN or ±Infinity — which JSON has no
    /// number for, and which the writer refuses with an exception — as <c>null</c>. See the class
    /// remarks.
    /// </summary>
    private static void WriteNumberOrNull(Utf8JsonWriter w, JsonEncodedText property, double value)
    {
        if (double.IsFinite(value)) w.WriteNumber(property, value);
        else                        w.WriteNull(property);
    }

    /// <summary>
    /// The label pairs as object members, each key ONCE: the pairs are sorted by key, then value, so
    /// a repeated key is a run of adjacent pairs and only the last of the run is written. A null key
    /// is skipped. Neither can come from ingest (see the class remarks); this is the defence that
    /// keeps a set stored before that fix from failing the answer. Valid sets are written pair for
    /// pair, as before.
    /// </summary>
    internal static void WriteLabels(Utf8JsonWriter w, ReadOnlySpan<string> kv)
    {
        for (int i = 0; i < kv.Length; i += 2)
        {
            string key = kv[i];
            if (key is null) continue;
            if (i + 2 < kv.Length && string.Equals(key, kv[i + 2])) continue;   // not the last of its run
            w.WriteString(key, kv[i + 1]);
        }
    }

    /// <summary><c>MetricKind.ToString()</c>, without the allocation for the three named kinds.</summary>
    private static string KindName(MetricKind kind) => kind switch
    {
        MetricKind.Counter   => "Counter",
        MetricKind.Gauge     => "Gauge",
        MetricKind.Histogram => "Histogram",
        _                    => kind.ToString(),
    };

    private static void Begin(HttpContext ctx)
    {
        ctx.Response.StatusCode  = StatusCodes.Status200OK;
        ctx.Response.ContentType = ContentType;
    }

    /// <summary>Hands what the writer holds to the pipe, and the pipe to the network once enough has gathered.</summary>
    private static async ValueTask FlushAsync(Utf8JsonWriter w, HttpResponse response, CancellationToken ct)
    {
        w.Flush();
        await response.BodyWriter.FlushAsync(ct).ConfigureAwait(false);
    }

    private sealed class AsyncArrayResult(IAsyncEnumerable<MetricSeries> series) : IResult
    {
        public async Task ExecuteAsync(HttpContext ctx)
        {
            Begin(ctx);
            var ct = ctx.RequestAborted;
            await using var w = new Utf8JsonWriter(ctx.Response.BodyWriter, WriterOptions);
            long flushedAt = 0;
            w.WriteStartArray();
            await foreach (var s in series.WithCancellation(ct).ConfigureAwait(false))
            {
                Write(w, s);
                if (w.BytesCommitted + w.BytesPending - flushedAt >= FlushThresholdBytes)
                {
                    await FlushAsync(w, ctx.Response, ct).ConfigureAwait(false);
                    flushedAt = w.BytesCommitted;
                }
            }
            w.WriteEndArray();
            w.Flush();
        }
    }

    private sealed class ListArrayResult(IReadOnlyList<MetricSeries> series) : IResult
    {
        public async Task ExecuteAsync(HttpContext ctx)
        {
            Begin(ctx);
            var ct = ctx.RequestAborted;
            await using var w = new Utf8JsonWriter(ctx.Response.BodyWriter, WriterOptions);
            long flushedAt = 0;
            w.WriteStartArray();
            for (int i = 0; i < series.Count; i++)
            {
                Write(w, series[i]);
                if (w.BytesCommitted + w.BytesPending - flushedAt >= FlushThresholdBytes)
                {
                    await FlushAsync(w, ctx.Response, ct).ConfigureAwait(false);
                    flushedAt = w.BytesCommitted;
                }
            }
            w.WriteEndArray();
            w.Flush();
        }
    }

    private sealed class SingleResult(MetricSeries series) : IResult
    {
        public Task ExecuteAsync(HttpContext ctx)
        {
            Begin(ctx);
            using var w = new Utf8JsonWriter(ctx.Response.BodyWriter, WriterOptions);
            Write(w, series);
            w.Flush();
            return Task.CompletedTask;
        }
    }
}
