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

    [Fact]
    public void TryReadSummaries_SkipsOutOfRangeRowsWithoutLosingItsPlaceInTheBody()
    {
        // The window bound is pushed INTO the parse, so an out-of-range row is walked past
        // instead of being built and thrown away by the caller. A row is variable-length —
        // three length-prefixed strings and a service-index list — so "walked past" is arithmetic
        // over the body, and arithmetic that is one byte out does not produce a wrong row: it
        // desynchronises everything after it, the parse throws, and the whole segment is reported
        // unreadable. Which is exactly what happened on the first attempt, from
        // `ms.Position += reader.ReadUInt16() * 4` — the Position getter is evaluated before the
        // read, so the two bytes the read consumed were written back out of the total.
        //
        // The fixture is built to catch that: the rows that must be SKIPPED are the ones with the
        // long names, the long paths and the extra services, so a skip that under-counts lands
        // the reader inside a string rather than politely one field over.
        var dir     = Directory.CreateTempSubdirectory("tracesum-range").FullName;
        var trcPath = Path.Combine(dir, "spans-range.trc");
        try
        {
            var spans = new List<SpanRecord>
            {
                // Out of range (too old), and deliberately the fattest row in the file.
                Span(10, 0xA0, 0, Base, 1_000_000,
                     new string('n', 300), "service-with-a-long-name", SpanStatusCode.Ok, 200,
                     new Dictionary<string, object?>
                     { ["http.request.method"] = "PROPFIND", ["url.path"] = new string('p', 400) }),
                Span(10, 0xA1, 0xA0, Base + 1_000, 1_000, "child", "another-service", SpanStatusCode.Ok),
                Span(10, 0xA2, 0xA0, Base + 2_000, 1_000, "child", "a-third-service",  SpanStatusCode.Ok),

                // In range — the one row the bound must return, and it must come back INTACT.
                Span(11, 0xB0, 0, Base + 5_000_000_000L, 2_000_000,
                     "GET /keep", "keeper", SpanStatusCode.Error, 503,
                     new Dictionary<string, object?> { ["http.request.method"] = "GET", ["url.path"] = "/keep" }),

                // Out of range (too new), fat again.
                Span(12, 0xC0, 0, Base + 9_000_000_000L, 1_000_000,
                     new string('m', 250), "yet-another-service", SpanStatusCode.Ok, 404,
                     new Dictionary<string, object?>
                     { ["http.request.method"] = "OPTIONS", ["url.path"] = new string('q', 500) }),
            };

            TraceSummarySidecar.Write(trcPath, spans);

            // Unbounded first, so a failure below is unambiguously the BOUND and not the format.
            Assert.True(TraceSummarySidecar.TryReadSummaries(trcPath, long.MinValue, long.MaxValue, out var all));
            Assert.Equal(3, all.Count);

            Assert.True(TraceSummarySidecar.TryReadSummaries(
                trcPath, Base + 4_000_000_000L, Base + 6_000_000_000L, out var kept));

            var row = Assert.Single(kept);
            Assert.Equal(new TraceId(0, 11), row.TraceId);
            Assert.Equal(Base + 5_000_000_000L, row.RootStartNano);
            Assert.Equal("GET /keep", row.Name);
            Assert.Equal("/keep", row.HttpPath);
            Assert.Equal("GET", row.HttpMethod);
            Assert.Equal("keeper", row.ServiceName);
            Assert.Equal((short)503, row.HttpStatusCode);
            Assert.True(row.HasError);

            // A bound that excludes everything is still a SUCCESSFUL read — the distinction the
            // walk above it now depends on, because "no rows here" and "I could not look" end a
            // stream very differently.
            Assert.True(TraceSummarySidecar.TryReadSummaries(trcPath, Base - 10, Base - 5, out var none));
            Assert.Empty(none);
        }
        finally { Directory.Delete(dir, recursive: true); }
    }

    [Fact]
    public void TryReadSummaries_OnAFileThatIsMissingOrCorrupt_SaysSoInsteadOfReturningNoRows()
    {
        // The whole point of the bool. `catch { return []; }` made a segment that vanished
        // between the Exists probe and the open — which compaction produces by design — look
        // exactly like a segment that held nothing, and the walk above then reported a window it
        // could not read as one it had read out.
        var dir     = Directory.CreateTempSubdirectory("tracesum-fail").FullName;
        var trcPath = Path.Combine(dir, "spans-fail.trc");
        try
        {
            Assert.False(TraceSummarySidecar.TryReadSummaries(trcPath, long.MinValue, long.MaxValue, out var gone));
            Assert.Empty(gone);

            TraceSummarySidecar.Write(trcPath, Sample());
            var sidecar = Path.ChangeExtension(trcPath, ".tracesum");

            File.WriteAllBytes(sidecar, [0xDE, 0xAD, 0xBE, 0xEF]);     // wrong magic
            Assert.False(TraceSummarySidecar.TryReadSummaries(trcPath, long.MinValue, long.MaxValue, out _));

            File.WriteAllBytes(sidecar, []);                            // torn to nothing
            Assert.False(TraceSummarySidecar.TryReadSummaries(trcPath, long.MinValue, long.MaxValue, out _));
        }
        finally { Directory.Delete(dir, recursive: true); }
    }
}
