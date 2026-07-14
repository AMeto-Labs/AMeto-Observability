using Ameto.Tracing;
using Ameto.Tracing.Storage;

namespace Ameto.Integration.Tests;

/// <summary>
/// Round-trips the <c>.tracesum</c> sidecar binary format (sparse volume header +
/// LZ4 per-trace body + service pool) that powers the trace list &amp; stats endpoints.
/// </summary>
public sealed class TraceSummarySidecarTests
{
    private const long Base = 1_700_000_000_000_000_000L; // arbitrary Unix-nanos anchor

    private static SpanRecord Span(
        ulong tid, ulong sid, ulong pid, long startNano, long durNano,
        string name, string svc, SpanStatusCode status, short http = 0,
        IReadOnlyDictionary<string, object?>? attrs = null) => new()
    {
        TraceId           = new TraceId(0, tid),
        SpanId            = new SpanId(sid),
        ParentSpanId      = new SpanId(pid),
        StartTimeUnixNano = startNano,
        DurationNanos     = durNano,
        Name              = name,
        ServiceName       = svc,
        Kind              = SpanKind.Server,
        Status            = status,
        HttpStatusCode    = http,
        Attributes        = attrs,
    };

    private static List<SpanRecord> Sample() =>
    [
        // Trace A: 2 spans (root api + child db), no error, HTTP root.
        Span(1, 0x10, 0, Base,               5_000_000, "GET /orders", "api", SpanStatusCode.Ok, 200,
            new Dictionary<string, object?> { ["http.request.method"] = "GET", ["url.path"] = "/orders" }),
        Span(1, 0x11, 0x10, Base + 1_000_000, 3_000_000, "SELECT", "db", SpanStatusCode.Ok),

        // Trace B: single root span, error, HTTP 500.
        Span(2, 0x20, 0, Base + 1_000_000_000, 8_000_000, "POST /pay", "api", SpanStatusCode.Error, 500,
            new Dictionary<string, object?> { ["http.request.method"] = "POST", ["url.path"] = "/pay" }),

        // Trace C: only a child span (root not captured) → HasRoot=false fallback.
        Span(3, 0x31, 0x30, Base + 2_000_000_000, 2_000_000, "work", "worker", SpanStatusCode.Ok),
    ];

    [Fact]
    public void Write_Then_ReadSummaries_RoundTrips()
    {
        var dir     = Directory.CreateTempSubdirectory("tracesum-test").FullName;
        var trcPath = Path.Combine(dir, "spans-1-2-3.trc");
        try
        {
            TraceSummarySidecar.Write(trcPath, Sample());
            var rows = TraceSummarySidecar.ReadSummaries(trcPath);

            Assert.Equal(3, rows.Count);

            var a = rows.Single(r => r.TraceId.Equals(new TraceId(0, 1)));
            Assert.True(a.HasRoot);
            Assert.False(a.HasError);
            Assert.Equal(2u, a.SpanCount);
            Assert.Equal("api", a.ServiceName);
            Assert.Equal("GET /orders", a.Name);
            Assert.Equal("GET", a.HttpMethod);
            Assert.Equal("/orders", a.HttpPath);
            Assert.Equal((short)200, a.HttpStatusCode);
            Assert.Equal(Base, a.RootStartNano);
            Assert.Contains("api", a.Services);
            Assert.Contains("db", a.Services);
            Assert.Equal(2, a.Services.Length);

            var b = rows.Single(r => r.TraceId.Equals(new TraceId(0, 2)));
            Assert.True(b.HasRoot);
            Assert.True(b.HasError);
            Assert.Equal(SpanStatusCode.Error, b.RootStatus);
            Assert.Equal((short)500, b.HttpStatusCode);
            Assert.Equal(1u, b.SpanCount);

            var c = rows.Single(r => r.TraceId.Equals(new TraceId(0, 3)));
            Assert.False(c.HasRoot);
            Assert.Equal("worker", c.ServiceName);     // falls back to earliest span's service
            Assert.Equal(string.Empty, c.Name);
            Assert.Equal(Base + 2_000_000_000, c.RootStartNano);
        }
        finally { Directory.Delete(dir, recursive: true); }
    }

    [Fact]
    public void Write_Then_ReadVolume_CountsTracesAndErrors()
    {
        var dir     = Directory.CreateTempSubdirectory("tracesum-test").FullName;
        var trcPath = Path.Combine(dir, "spans-1-2-3.trc");
        try
        {
            TraceSummarySidecar.Write(trcPath, Sample());
            var vol = TraceSummarySidecar.ReadVolume(trcPath);

            Assert.NotNull(vol);
            uint traces = 0, errors = 0;
            foreach (var e in vol!.Buckets) { traces += e.TraceCount; errors += e.ErrorCount; }

            Assert.Equal(3u, traces);   // three distinct traces
            Assert.Equal(1u, errors);   // only trace B errored
            Assert.Equal(Base, vol.MinStartNano);
        }
        finally { Directory.Delete(dir, recursive: true); }
    }

    [Fact]
    public void ReadVolume_MissingSidecar_ReturnsNull()
    {
        var dir = Directory.CreateTempSubdirectory("tracesum-test").FullName;
        try
        {
            Assert.Null(TraceSummarySidecar.ReadVolume(Path.Combine(dir, "nope.trc")));
            Assert.False(TraceSummarySidecar.Exists(Path.Combine(dir, "nope.trc")));
        }
        finally { Directory.Delete(dir, recursive: true); }
    }
}
