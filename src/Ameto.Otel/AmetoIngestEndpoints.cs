namespace Ameto.Otel;

/// <summary>
/// The paths this server receives telemetry on — the single list both trace decoders use to drop
/// a client span that describes an export to Ameto itself.
///
/// <para>Without that drop an instrumented exporter traces its own exports: one CLIENT span per
/// batch, exported, producing another. It is self-amplifying and it leaves nothing in the log to
/// explain itself. The list used to be duplicated by hand in two files, and this change is here
/// because adding a route spelling to one of them and not the other is exactly what happened.</para>
/// </summary>
internal static class AmetoIngestEndpoints
{
    /// <summary>
    /// Matched as a WHOLE path, never as a substring. `Contains("/v1/traces")` also swallows a
    /// customer's own <c>/api/v1/traces/search</c>, and dropping real client spans to protect
    /// against a feedback loop is the more expensive mistake of the two: the loop is loud, a
    /// missing span is not.
    /// </summary>
    private static readonly string[] Paths =
    [
        "/api/events",
        "/otlp/v1/logs", "/otlp/v1/traces", "/otlp/v1/metrics",
        "/v1/logs",      "/v1/traces",      "/v1/metrics",
        "/opentelemetry.proto.collector.logs.v1.logsservice/export",
        "/opentelemetry.proto.collector.trace.v1.traceservice/export",
        "/opentelemetry.proto.collector.metrics.v1.metricsservice/export",
    ];

    /// <summary>
    /// True when <paramref name="url"/> addresses one of this server's receivers. Takes the path
    /// portion of the URL — scheme, host, query and fragment are irrelevant — and compares it
    /// whole, case-insensitively.
    /// </summary>
    public static bool Matches(ReadOnlySpan<char> url)
    {
        var path = PathOf(url);
        foreach (var candidate in Paths)
            if (path.Equals(candidate, StringComparison.OrdinalIgnoreCase))
                return true;
        return false;
    }

    /// <summary>UTF-8 overload for the streaming parser, which never builds a string.</summary>
    public static bool Matches(ReadOnlySpan<byte> url)
    {
        // Attribute URLs are short; a stack copy keeps the streaming parser allocation-free.
        if (url.Length > 512) return false;
        Span<char> chars = stackalloc char[url.Length];
        for (int i = 0; i < url.Length; i++) chars[i] = (char)url[i];
        return Matches((ReadOnlySpan<char>)chars);
    }

    /// <summary>
    /// The path of an absolute or relative URL, without query or fragment. Deliberately hand-rolled:
    /// this runs per span, and <c>Uri</c> would allocate one to answer a question this simple.
    /// </summary>
    private static ReadOnlySpan<char> PathOf(ReadOnlySpan<char> url)
    {
        int scheme = url.IndexOf("://", StringComparison.Ordinal);
        if (scheme >= 0)
        {
            var afterScheme = url[(scheme + 3)..];
            int slash = afterScheme.IndexOf('/');
            url = slash < 0 ? default : afterScheme[slash..];
        }

        int cut = url.IndexOfAny('?', '#');
        if (cut >= 0) url = url[..cut];

        // A trailing slash names the same endpoint.
        if (url.Length > 1 && url[^1] == '/') url = url[..^1];
        return url;
    }
}
