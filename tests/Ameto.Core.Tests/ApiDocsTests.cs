namespace Ameto.Core.Tests;

/// <summary>
/// <c>docs/API.md</c> is the whole contract for anyone writing a client against this server, and
/// the ingest failure shapes drifted out from under it. <c>POST /api/events</c> can answer 400
/// with events already durably stored — the handler's own XML doc says so and
/// <c>IngestMalformedBatchReportingTests</c> pins it — while the reference still said only
/// "invalid MessagePack". A client author reading that has no reason to inspect the body before
/// retrying, and there is no de-duplication anywhere on the ingest path: every retry of a
/// partially-accepted batch stores its prefix again.
///
/// <para>A substring guard, deliberately: it cannot check that the prose is GOOD, only that the
/// facts a client must not miss are present at all. That is the drift that actually happened.</para>
/// </summary>
public sealed class ApiDocsTests
{
    private static string RepoRoot()
    {
        var d = new DirectoryInfo(AppContext.BaseDirectory);
        while (d is not null && !File.Exists(Path.Combine(d.FullName, "docs", "API.md")))
            d = d.Parent;

        Assert.NotNull(d);
        return d!.FullName;
    }

    private static string Api() => File.ReadAllText(Path.Combine(RepoRoot(), "docs", "API.md"));

    [Fact]
    public void The_partial_ingest_400_is_documented_with_what_it_leaves_behind()
    {
        string doc = Api();

        Assert.Contains("failedAtElement", doc, StringComparison.Ordinal);
        Assert.Contains("already ingested", doc, StringComparison.Ordinal);
    }

    /// <summary>
    /// The OTLP/HTTP section documented no failure at all, while the gRPC table beside it has
    /// always listed INVALID_ARGUMENT — and the HTTP road is the worse of the two to be silent
    /// about, because its 400 carries no counts and may still have kept a prefix.
    /// </summary>
    [Fact]
    public void The_otlp_http_400_is_documented_beside_its_grpc_counterpart()
    {
        string doc = Api();

        int http = doc.IndexOf("### `POST /v1/logs`", StringComparison.Ordinal);
        int grpc = doc.IndexOf("### OTLP over gRPC", StringComparison.Ordinal);
        Assert.True(http > 0 && grpc > http, "the OTLP sections moved — this guard needs re-aiming");

        string httpSection = doc[http..grpc];
        Assert.Contains("400 Bad Request", httpSection, StringComparison.Ordinal);
        Assert.Contains("may already be ingested", httpSection, StringComparison.Ordinal);
    }

    /// <summary>
    /// <c>processThreads</c> is a documented field whose MEANING changed — Process.Threads.Count
    /// became ThreadPool.ThreadCount, which excludes the drainer, the flushers and the GC, so the
    /// number is smaller for the same load. The Angular client was updated for it and the wire
    /// contract was not, leaving every non-browser consumer (a scrape, an alert threshold, a
    /// runbook) reading the field under its old meaning and seeing an unexplained step change.
    /// </summary>
    [Fact]
    public void The_processThreads_meaning_change_is_called_out()
    {
        string doc = Api();

        Assert.Contains("processThreads", doc, StringComparison.Ordinal);
        Assert.Contains("thread-pool threads", doc, StringComparison.Ordinal);
    }

    /// <summary>
    /// The memory figures this round added exist to be READ by an operator on a small host; a
    /// figure documented nowhere is one nobody will look for.
    /// </summary>
    [Fact]
    public void The_memory_figures_are_in_the_diagnostics_example()
    {
        string doc = Api();

        foreach (string field in new[]
                 {
                     "indexCacheBudgetBytes", "indexCacheIdleEvicted", "indexCacheNativeBytes",
                     "indexCacheNativeEvicted",
                     "indexBuildPooledBytes",
                     "ingestBufferPooledBytes", "ingestArenaResidentBytes", "logsQuarantinedBytes",
                 })
            Assert.Contains(field, doc, StringComparison.Ordinal);
    }
}
