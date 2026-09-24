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
/// encoder, as <see cref="MetricJson"/>. And the same FAILURE: a non-finite value throws from the
/// writer as it did from the serializer. <c>MetricResponseShapeTests</c> pins all of it.</para>
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
/// so an answer that fails before then fails as a clean 500 with nothing sent, exactly as before.
/// A failure after a flush aborts the response mid-body, which is what a non-finite value past the
/// serializer's flush threshold always did; what is new is only that a series is written before
/// the next one is read.</para>
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
            ThrowIfNotFinite(p.Value);
            ThrowIfNotFinite(p.Sum);
            w.WriteStartObject();
            w.WriteNumber(TsProp,    p.TimestampUnixNano);
            w.WriteNumber(ValueProp, p.Value);
            w.WriteNumber(CountProp, p.Count);
            w.WriteNumber(SumProp,   p.Sum);
            w.WriteEndObject();
        }
        w.WriteEndArray();
        w.WriteEndObject();
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

    /// <summary>
    /// The check the serializer's writer made before it wrote a double. <see cref="WriterOptions"/>
    /// skips the writer's structural validation (the shape here is fixed), which is also where a
    /// non-finite number would have been refused — so it is made here, with the same exception.
    /// </summary>
    private static void ThrowIfNotFinite(double value)
    {
        if (!double.IsFinite(value))
            throw new ArgumentException(
                $".NET number values such as positive and negative infinity cannot be written as valid JSON. " +
                $"To make it work when using 'JsonSerializer', consider specifying 'JsonNumberHandling.AllowNamedFloatingPointLiterals'.");
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
