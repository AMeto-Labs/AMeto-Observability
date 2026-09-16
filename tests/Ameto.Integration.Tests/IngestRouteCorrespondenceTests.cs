using Ameto.Otel;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;

namespace Ameto.Integration.Tests;

/// <summary>
/// The authentication / authorization / rate-limiter stack is skipped for the telemetry
/// receivers, and which requests get that treatment is decided by a HAND-MAINTAINED path list
/// (<c>IngestRoutes</c>) that was pinned to nothing. The routes themselves are mapped elsewhere
/// and independently — EndpointMapper for CLEF, OtlpEndpointMapper for OTLP/HTTP,
/// OtlpGrpcEndpointMapper for gRPC — so the two can drift apart in either direction, and both
/// directions are silent:
///
/// <list type="bullet">
/// <item>a receiver that is renamed, or a new one added, stops matching: the whole JWT stack runs
/// again on every ingest POST, which at the 100k-150k events/s target is the auth pipeline a
/// hundred times a second for no result. Nothing goes red; the regression the bypass exists to
/// prevent simply returns.</item>
/// <item>a listed path that is NOT a receiver reaches an endpoint carrying authorization
/// metadata with no authorization middleware in the pipeline — and ASP.NET Core throws rather
/// than serving it, so every request to it answers 500 instead of 401, in production.</item>
/// </list>
///
/// <para><c>IngestAuthBypassTests</c> cannot see either: all seven of its assertions held before
/// the bypass existed (the receivers enforce their own API keys, and the authorization middleware
/// it removes binds only to endpoints that ask for it), so that class is green against a pipeline
/// with the UseWhen taken out entirely.</para>
///
/// <para>What is NOT asserted, and why: "every unauthenticated POST must be claimed" would be the
/// tightest rule, but deliberately anonymous routes exist (sign-in, token refresh). So an
/// endpoint that is neither claimed nor in the exemption list below fails, which forces the
/// decision to be made by a person rather than by omission.</para>
/// </summary>
public sealed class IngestRouteCorrespondenceTests : IClassFixture<AmetoWebAppFactory>
{
    private readonly AmetoWebAppFactory _factory;
    public IngestRouteCorrespondenceTests(AmetoWebAppFactory factory) => _factory = factory;

    /// <summary>
    /// POSTs that carry no authorization metadata on purpose and are NOT telemetry receivers, so
    /// the bypass must not claim them either:
    ///
    /// <list type="bullet">
    /// <item>sign-in, which has nothing to authenticate yet (it is rate-limited instead);</item>
    /// <item>the two peer-to-peer replication routes, which authenticate inside the handler
    /// against <c>Replication.Secret</c> — fail-closed, rejecting everything while that is unset.
    /// That is the same shape as a receiver checking its own API key, just not on the ingest hot
    /// path this bypass exists for.</item>
    /// </list>
    ///
    /// <para>Named rather than inferred: a rule that let ANY unauthenticated POST pass would be no
    /// guard at all, which is the state this suite exists to end. Note what is deliberately NOT
    /// here — <c>/api/auth/refresh</c> carries authorization metadata and so is not in this set;
    /// listing routes that do not need listing would make the exemption meaningless in its turn.</para>
    /// </summary>
    private static readonly HashSet<string> AnonymousByDesign = new(StringComparer.OrdinalIgnoreCase)
    {
        "/api/auth/login",
        "/api/replication/ping",
        "/api/replication/segments/{nodeId}/{segmentId}",
    };

    private IReadOnlyList<Endpoint> Endpoints()
    {
        _factory.CreateClient().Dispose();          // force the host to build
        return _factory.Services.GetRequiredService<EndpointDataSource>().Endpoints;
    }

    private static bool IsPost(Endpoint e) =>
        e.Metadata.GetMetadata<HttpMethodMetadata>()?.HttpMethods.Contains("POST") == true;

    private static bool RequiresAuthorization(Endpoint e) =>
        e.Metadata.GetMetadata<IAuthorizeData>() is not null;

    private static string PathOf(RouteEndpoint e) => "/" + (e.RoutePattern.RawText ?? "").TrimStart('/');

    /// <summary>The predicate itself, asked the way the pipeline asks it.</summary>
    private static bool Claims(string path, string method = "POST")
    {
        var ctx = new DefaultHttpContext();
        ctx.Request.Method = method;
        ctx.Request.Path   = path;
        return IngestRoutes.IsIngestRequest(ctx);
    }

    [Fact]
    public void Every_claimed_path_that_is_mapped_carries_no_authorization_metadata()
    {
        var offenders = new List<string>();

        foreach (var e in Endpoints().OfType<RouteEndpoint>())
        {
            string path = PathOf(e);
            if (!IsPost(e) || !Claims(path)) continue;
            if (RequiresAuthorization(e)) offenders.Add(path);
        }

        Assert.True(offenders.Count == 0,
            "the auth bypass claims " + string.Join(", ", offenders) + ", whose endpoint(s) carry "
          + "authorization metadata — with the middleware skipped, ASP.NET Core answers those 500, not 401");
    }

    [Fact]
    public void Every_unauthenticated_post_is_either_a_claimed_receiver_or_exempt_by_name()
    {
        var unclaimed = new List<string>();

        foreach (var e in Endpoints().OfType<RouteEndpoint>())
        {
            string path = PathOf(e);
            if (!IsPost(e) || RequiresAuthorization(e)) continue;
            if (Claims(path) || AnonymousByDesign.Contains(path)) continue;
            unclaimed.Add(path);
        }

        Assert.True(unclaimed.Count == 0,
            "POST " + string.Join(", ", unclaimed) + " runs with no authorization metadata and is not "
          + "claimed by IngestRoutes. If it is a telemetry receiver, add it there (and to "
          + "AmetoIngestEndpoints); if it is deliberately anonymous, name it in AnonymousByDesign.");
    }

    /// <summary>The receivers this build must map, whatever else is configured.</summary>
    [Theory]
    [InlineData("/api/events")]
    [InlineData("/v1/logs")]
    [InlineData("/otlp/v1/logs")]
    public void The_core_receivers_are_mapped_and_claimed(string path)
    {
        Assert.Contains(Endpoints().OfType<RouteEndpoint>(),
            e => IsPost(e) && PathOf(e).Equals(path, StringComparison.OrdinalIgnoreCase) && !RequiresAuthorization(e));

        Assert.True(Claims(path), $"{path} is mapped as a receiver but the bypass does not claim it");
    }

    /// <summary>
    /// GET /api/events is the SSE search and shares its path with the CLEF ingest POST, so the
    /// method is part of the match — a bypass that claimed the GET would take the search off
    /// authentication entirely.
    /// </summary>
    [Fact]
    public void The_match_is_by_method_as_well_as_path()
    {
        Assert.True(Claims("/api/events"));
        Assert.False(Claims("/api/events", "GET"));
        Assert.False(Claims("/api/alerts"));
        Assert.False(Claims("/api/events/extra"));      // whole path, never a prefix
    }

    /// <summary>
    /// The gRPC Export methods are claimed by their one-segment prefix rather than by the list,
    /// and case-insensitively — an exporter may send either spelling.
    /// </summary>
    [Fact]
    public void The_grpc_export_paths_are_claimed_by_prefix()
    {
        Assert.True(Claims(IngestRoutes.GrpcPrefix + ".logs.v1.LogsService/Export"));
        Assert.True(Claims(IngestRoutes.GrpcPrefix + ".logs.v1.logsservice/export"));
        Assert.False(Claims("/opentelemetry.proto.collectorX.logs.v1.LogsService/Export", "GET"));
    }

    /// <summary>
    /// The second hand-maintained copy of the same list — the one the trace decoders use to drop
    /// a span describing an export to this server. Adding a spelling to one and not the other is
    /// the drift that already happened once.
    /// </summary>
    [Fact]
    public void Both_hand_maintained_lists_agree_about_the_http_receivers()
    {
        foreach (string path in IngestRoutes.Paths)
            Assert.True(AmetoIngestEndpoints.Matches(path),
                $"IngestRoutes claims {path} and AmetoIngestEndpoints does not know it");
    }
}
