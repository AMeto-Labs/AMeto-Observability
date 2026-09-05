using Ameto.Alerts;
using Ameto.Replication;
using Ameto.Core;
using Ameto.Indexing;
using Ameto.Ingestion;
using Ameto.Metrics;
using Ameto.Otel;
using Ameto.Query;
using Ameto.Server;
using Ameto.Server.Auth;
using Ameto.Server.Updates;
using Ameto.Storage;
using Ameto.Tracing;

var builder = WebApplication.CreateBuilder(new WebApplicationOptions
{
    Args            = args,
    // Use the DLL's directory as content root so wwwroot is found regardless
    // of the working directory the process is started from.
    ContentRootPath = AppContext.BaseDirectory,
});

// ── Windows Service hosting ───────────────────────────────────────────────────
// No-op unless the process is actually launched by the Windows SCM
// (WindowsServiceHelpers.IsWindowsService() returns true). On Linux, in the
// Docker image, and when run from a console this does nothing, so the container
// and systemd paths are unaffected. Without it, a service registered by the
// Windows installer never signals SERVICE_RUNNING and the SCM aborts it with
// "Error 1053: the service did not respond to the start request in a timely
// fashion". It also routes host logs to the Windows Event Log when running as a
// service (no console is attached).
builder.Services.AddWindowsService(static options => options.ServiceName = "Ameto");
// ── Configuration sources ────────────────────────────────────────────────────
// We use a single, app-specific config file instead of appsettings*.json.
builder.Configuration.Sources.Clear();
builder.Configuration
    .AddYamlFile(
        System.IO.Path.Combine(AppContext.BaseDirectory, "config.yml"),
        optional: false,
        reloadOnChange: true)
    .AddEnvironmentVariables()
    .AddCommandLine(args);

// ── Logging (hardcoded; previously in appsettings.json) ───────────────────────────────────
builder.Logging.SetMinimumLevel(Microsoft.Extensions.Logging.LogLevel.Information);
builder.Logging.AddFilter("Microsoft.AspNetCore",       Microsoft.Extensions.Logging.LogLevel.Error);
builder.Logging.AddFilter("Microsoft.Hosting.Lifetime", Microsoft.Extensions.Logging.LogLevel.None);

// ── Host filtering: allow any host (previously "AllowedHosts": "*") ──────────────
builder.Services.Configure<Microsoft.AspNetCore.HostFiltering.HostFilteringOptions>(o =>
{
    o.AllowedHosts        = new[] { "*" };
    o.AllowEmptyHosts     = true;
    o.IncludeFailureMessage = false;
});
// ── Configuration ─────────────────────────────────────────────────────────────
var AmetoSection = builder.Configuration.GetSection("Ameto");

// Auto-bind the entire Ameto section to ServerOptions; class defaults are the fallback.
var serverOptions = AmetoSection.Get<ServerOptions>() ?? new ServerOptions();

// Parsed here rather than at the point of use: a malformed prefix should stop the server with
// one clear line, not half-configure a pipeline that then serves the UI from the wrong place.
var basePath = UrlBasePath.Parse(serverOptions.BasePath);

// ── File log ──────────────────────────────────────────────────────────────────
// Needed because the Windows Event Log provider that AddWindowsService installs
// applies its own Warning minimum: as a service, every Information diagnostic
// (the periodic MEM attribution line, flush/merge progress, the flush budgets
// logged at startup) otherwise had no sink at all. See FileLoggerProvider.
if (serverOptions.Logging.FileEnabled)
{
    var fileLevel = Enum.TryParse<Microsoft.Extensions.Logging.LogLevel>(
        serverOptions.Logging.FileMinimumLevel, ignoreCase: true, out var lvl)
        ? lvl
        : Microsoft.Extensions.Logging.LogLevel.Information;

    builder.Logging.AddProvider(new FileLoggerProvider(
        Path.Combine(serverOptions.DataDirectory, "logs"),
        fileLevel,
        serverOptions.Logging.FileRetainDays));
}



//// Enable reflection-based JSON for minimal-API model binding.
//// Without this, RequestDelegateGenerator fails with "no metadata for type"
//// because the default HttpJsonOptions has an empty TypeInfoResolverChain in .NET 10.
//builder.Services.ConfigureHttpJsonOptions(o =>
//    o.SerializerOptions.TypeInfoResolverChain.Insert(0,
//        new System.Text.Json.Serialization.Metadata.DefaultJsonTypeInfoResolver()));

builder.Services.AddSingleton(serverOptions);
builder.Services.AddSingleton(serverOptions.HotTier);
builder.Services.AddSingleton<Microsoft.Extensions.Options.IOptions<ServerOptions>>(
    _ => Microsoft.Extensions.Options.Options.Create(serverOptions));

// ── Auth services (SQLite) ──────────────────────────────────────────────────────
var authOptions = builder.Configuration.GetSection("Ameto:Auth").Get<Ameto.Server.Auth.AuthOptions>() ?? new Ameto.Server.Auth.AuthOptions();
builder.Services.AddAmetoAuth(serverOptions.DataDirectory, authOptions);

// ── Rate limiting: throttle credential-guessing on the login endpoint ─────────
// Fixed window per client IP: brute-forcing the local admin account is otherwise
// unbounded. Other endpoints are unaffected (ingest is API-key gated).
builder.Services.AddRateLimiter(o =>
{
    o.RejectionStatusCode = StatusCodes.Status429TooManyRequests;
    o.AddPolicy("auth-login", ctx =>
        System.Threading.RateLimiting.RateLimitPartition.GetFixedWindowLimiter(
            partitionKey: ctx.Connection.RemoteIpAddress?.ToString() ?? "unknown",
            factory: _ => new System.Threading.RateLimiting.FixedWindowRateLimiterOptions
            {
                PermitLimit = 10,
                Window      = TimeSpan.FromMinutes(1),
                QueueLimit  = 0,
            }));
});

// ── Core services ─────────────────────────────────────────────────────────────
builder.Services
    .AddAmetoStorage()
    .AddAmetoIndexing()
    .AddAmetoIngestion()
    .AddAmetoQuery();

// Short-TTL cache for GET /api/events/counts header-scan responses.
builder.Services.AddSingleton<LogVolumeCountsCache>();

// Per-search time budget and concurrency limit (see QueryGuard).
builder.Services.AddSingleton<QueryGuard>();

// Live tails wait on this instead of polling; LiveTailWiring hooks it to the write path.
builder.Services.AddSingleton<LiveEventSignal>();
builder.Services.AddHostedService<LiveTailWiring>();

// ── Software-update check (Settings → Updates) ────────────────────────────────
// Singleton holds the latest-release snapshot for the endpoints; the hosted
// service polls GitHub hourly (no-op when Ameto:Updates:Enabled is false).
builder.Services.AddSingleton<UpdateChecker>();
builder.Services.AddHostedService(static sp => sp.GetRequiredService<UpdateChecker>());

// ── Optional signal subsystems (toggle via env for benchmarking / logs-only mode) ──
//   Ameto__Metrics__Enabled / Ameto__Tracing__Enabled / Ameto__Alerts__Enabled
bool enableTracing = builder.Configuration.GetValue("Ameto:Tracing:Enabled", true);
bool enableMetrics = builder.Configuration.GetValue("Ameto:Metrics:Enabled", true);
bool enableAlerts  = builder.Configuration.GetValue("Ameto:Alerts:Enabled",  true);

// The alert evaluator consumes IMetricAggregator + ITraceStatsProvider, so it can
// only run when both subsystems are present. Disable it otherwise to avoid a DI failure.
if (enableAlerts && (!enableMetrics || !enableTracing))
    enableAlerts = false;

// Alert rules store + evaluator
if (enableAlerts)
{
    // Serialise alert channels by runtime type so their (masked) fields reach the client.
    builder.Services.ConfigureHttpJsonOptions(o =>
        o.SerializerOptions.Converters.Add(new Ameto.Server.AlertChannelResponseConverter()));

    // Reversible encryption for channel secrets (bot tokens, SMTP passwords, webhook auth headers).
    builder.Services.AddSingleton<Ameto.Core.ISecretProtector>(sp =>
        Ameto.Core.SecretProtectorFactory.Create(
            serverOptions.DataDirectory,
            builder.Configuration["Ameto:MasterKey"],
            path => sp.GetRequiredService<ILogger<Ameto.Core.AesGcmSecretProtector>>().LogWarning(
                "Secret protector: generated a new master key at {Path}. For production set AMETO__MasterKey and keep it off the data volume.",
                path)));
    builder.Services.AddAmetoAlerts(serverOptions.DataDirectory);
}

// Set when Ameto:Traces:IndexBackfill is not one of the three names; reported once the logger
// exists, because a setting that quietly did the opposite of what was typed is worth saying aloud.
string? unknownBackfillValue = null;

// Distributed tracing
if (enableTracing)
{
    // An unrecognised value falls back to the default rather than failing the start: this is a
    // performance switch, and no setting of it can make an answer wrong — an unindexed segment is
    // read the way every segment was read before the index existed.
    // MATCHED BY NAME RATHER THAN Enum.TryParse, which accepts more than the three documented
    // values: any integer string becomes that numeric value whether or not it is defined, and a
    // comma-separated list parses as flags even though this enum has none. And a real typo —
    // "Offf" — silently became Idle, the mode that does the MOST work, with nothing said about it.
    string configured = serverOptions.Traces.IndexBackfill?.Trim() ?? "";
    var backfillMode = configured.ToLowerInvariant() switch
    {
        "off"   => TraceIndexBackfillMode.Off,
        "idle"  => TraceIndexBackfillMode.Idle,
        "eager" => TraceIndexBackfillMode.Eager,
        _       => TraceIndexBackfillMode.Idle,
    };
    if (!string.Equals(configured, backfillMode.ToString(), StringComparison.OrdinalIgnoreCase))
        unknownBackfillValue = configured;

    builder.Services.AddAmetoTracing(serverOptions.DataDirectory, backfillMode,
                                     serverOptions.Traces.SegmentFormatV4);
}

// Metrics
if (enableMetrics)
    builder.Services.AddAmetoMetrics(serverOptions.DataDirectory);

var repOpts = builder.Configuration.GetSection("Ameto:Replication").Get<ReplicationOptions>() ?? new ReplicationOptions();
builder.Services.AddAmetoReplication(repOpts);

// ── Kestrel ───────────────────────────────────────────────────────────────────
// Listeners are configured explicitly rather than through UseUrls, because UseUrls has no
// per-endpoint control over the HTTP version and the OTLP/gRPC listener needs HTTP/2 while the
// main port must keep HTTP/1.1. Mixing the two APIs is ambiguous — the coded endpoints win and
// the URLs are silently ignored — so both branches moved together.
bool useTls = !string.IsNullOrEmpty(serverOptions.SslCertPath);
HotReloadCertificate? certReloader = null;
if (useTls)
{
    // Hot-reloadable certificate: Kestrel invokes the selector on every TLS handshake, so
    // replacing the .pfx file on disk causes new connections to use the new cert without
    // restarting the process.
    certReloader = new HotReloadCertificate(
        serverOptions.SslCertPath, serverOptions.SslCertPassword,
        LoggerFactory.Create(b => b.AddConsole()).CreateLogger<HotReloadCertificate>());
    builder.Services.AddSingleton(certReloader);
}

builder.WebHost.ConfigureKestrel(k =>
{
    if (useTls)
        k.ConfigureHttpsDefaults(h => h.ServerCertificateSelector = (_, _) => certReloader!.Current);

    // The everything port: UI, /api, SSE, the OTLP/HTTP receivers. Left on the framework
    // default (Http1AndHttp2), which under TLS negotiates h2 by ALPN and in plaintext is
    // HTTP/1.1 — exactly what a browser needs.
    k.ListenAnyIP(serverOptions.HttpPort, l => { if (useTls) l.UseHttps(); });

    // OTLP/gRPC, off unless a port is configured. It is a SEPARATE listener because without
    // TLS there is no ALPN, so a plaintext endpoint that accepts HTTP/2 accepts nothing else —
    // put that on the main port and the UI, /api, the live tail and the health check all stop
    // answering. Under TLS one port could serve both, but keeping the shape identical either
    // way means the documented address does not change when someone turns TLS on.
    if (serverOptions.OtlpGrpcPort > 0)
        k.ListenAnyIP(serverOptions.OtlpGrpcPort, l =>
        {
            l.Protocols = Microsoft.AspNetCore.Server.Kestrel.Core.HttpProtocols.Http2;
            if (useTls) l.UseHttps();
        });
});
var app = builder.Build();


// ── Middleware ─────────────────────────────────────────────────────────────────
// PORT ISOLATION for the OTLP/gRPC listener. A second Kestrel endpoint does not scope routing:
// without this, port 4317 served the entire application — the Angular UI through the SPA
// fallback, /api/auth/login, the whole query surface, replication — to anything that speaks
// HTTP/2 with prior knowledge. That port is conventionally opened to a telemetry network the UI
// port is not. The check runs both ways, so the ingest port carries only ingest and the Export
// methods are unreachable anywhere else.
if (serverOptions.OtlpGrpcPort > 0)
{
    app.Use(async (ctx, next) =>
    {
        bool onGrpcPort = ctx.Connection.LocalPort == serverOptions.OtlpGrpcPort;
        // A plain prefix, NOT StartsWithSegments: a gRPC method path is one long segment —
        // "/opentelemetry.proto.collector.logs.v1.LogsService/Export" — so segment matching
        // never fired, which silently inverted this check and let the Export methods answer on
        // the main port while refusing them on their own.
        bool isGrpcRoute = ctx.Request.Path.HasValue
                        && ctx.Request.Path.Value!.StartsWith("/opentelemetry.proto.collector", StringComparison.Ordinal);
        if (onGrpcPort != isGrpcRoute)
        {
            ctx.Response.StatusCode = StatusCodes.Status404NotFound;
            return;
        }
        await next();
    });
}

// Reverse-proxy support (opt-in): when TLS terminates on nginx/traefik, Kestrel
// sees plain http and OAuth redirect URIs would be built with the wrong scheme.
// The config flag is the trust gate, so accept the headers from any proxy address.
if (serverOptions.TrustForwardedHeaders)
{
    var fwd = new Microsoft.AspNetCore.Builder.ForwardedHeadersOptions
    {
        ForwardedHeaders = Microsoft.AspNetCore.HttpOverrides.ForwardedHeaders.XForwardedProto
                         | Microsoft.AspNetCore.HttpOverrides.ForwardedHeaders.XForwardedHost
                         | Microsoft.AspNetCore.HttpOverrides.ForwardedHeaders.XForwardedFor,
    };
    fwd.KnownIPNetworks.Clear();
    fwd.KnownProxies.Clear();
    // Restrict forwarded-header trust to the configured proxies when given;
    // otherwise trust any source (legacy behaviour) — warned about at startup.
    foreach (var ip in serverOptions.KnownProxies)
        if (System.Net.IPAddress.TryParse(ip, out var addr))
            fwd.KnownProxies.Add(addr);
    app.UseForwardedHeaders(fwd);
}
// ── Deployment prefix ─────────────────────────────────────────────────────────
// Everything below sees the path with the prefix already stripped, so not one route has to
// know about it. Below UseForwardedHeaders so a proxy-corrected scheme and host are already
// in place; above everything else for the reasons in the next paragraph.
//
// The explicit UseRouting() is load-bearing and must stay directly under UsePathBase. Without
// it, WebApplication inserts its own at the very front of the pipeline — ahead of UsePathBase —
// and matching then happens against the UN-stripped path. Nothing errors: "/ameto/api/events"
// simply matches the SPA catch-all below instead, so every prefixed API call returns 200
// text/html, and because the selected endpoint is the fallback it carries no metadata, which
// silently drops .RequireAuthorization() and the login brute-force limiter with it. Measured,
// not assumed. Unconditional, so both deployment shapes run the same pipeline.
if (!basePath.IsRoot) app.UsePathBase(basePath.PathBase);
app.UseRouting();

app.UseRateLimiter();
app.UseAuthentication();
app.UseAuthorization();
// The SPA entry document is the one file whose bytes depend on configuration, so it does not
// come from the static-file middleware — see SpaIndex. This also replaces UseDefaultFiles,
// whose only job here was mapping "/" to it.
var spaIndex = new SpaIndex(app.Environment, basePath,
                            app.Services.GetRequiredService<ILoggerFactory>().CreateLogger<SpaIndex>());
app.Use(async (ctx, next) =>
{
    var path = ctx.Request.Path;
    bool isEntryDocument = !path.HasValue || path == "/" ||
                           path.Equals("/index.html", StringComparison.OrdinalIgnoreCase);
    if (isEntryDocument && (HttpMethods.IsGet(ctx.Request.Method) || HttpMethods.IsHead(ctx.Request.Method))
        && await spaIndex.TryWriteAsync(ctx))
        return;
    await next();
});
app.UseStaticFiles();

// ── Endpoints ─────────────────────────────────────────────────────────────────
app.MapAuthEndpoints();
app.MapSearchHistoryEndpoints();
app.MapAmetoEndpoints();
if (enableAlerts)
    app.MapAlertEndpoints();
app.MapRetentionEndpoints();
app.MapDiagnosticsEndpoints();
app.MapUpdateEndpoints();
app.MapReplicationEndpoints();
app.MapOtlpEndpoints(enableTracing, enableMetrics, basePath.PathBase);
// Only when a port is configured. Kestrel endpoints do not scope routing — every route on this
// WebApplication answers on every listener — so mapping these unconditionally put the Export
// methods on the main port as well, where a plain HTTP/1.1 POST reached them. The middleware
// above is the other half: it keeps the gRPC port to the gRPC methods and the gRPC methods to
// the gRPC port, which is what the two comments here used to merely assert.
if (serverOptions.OtlpGrpcPort > 0)
    app.MapOtlpGrpcEndpoints(enableTracing, enableMetrics);
if (enableMetrics)
    app.MapMetricEndpoints();
if (enableTracing)
    app.MapTraceEndpoints();

// SPA fallback — Angular handles client-side routing.
//
// Both halves of this are copied from MapFallbackToFile, which is what it replaces:
//   * "{*path:nonfile}" — a request for a missing ASSET should 404, not be answered with an HTML
//     page the browser was told to parse as a script.
//   * GET/HEAD only — the helper attached this metadata, a bare MapFallback does not, and without
//     it every unmatched POST answers 200 text/html instead of 405. That reads as success to a
//     sender: an OTLP exporter pointed at the spec paths /v1/logs and friends (this server maps
//     them under /otlp/, so they are unmatched) would report delivery and drop the batch.
app.MapFallback("{*path:nonfile}", async (HttpContext ctx) =>
{
    if (!await spaIndex.TryWriteAsync(ctx)) ctx.Response.StatusCode = StatusCodes.Status404NotFound;
}).WithMetadata(new HttpMethodMetadata(["GET", "HEAD"]));

// ── Startup banner ────────────────────────────────────────────────────────────
app.Lifetime.ApplicationStarted.Register(() =>
{
    var logger    = app.Services.GetRequiredService<ILoggerFactory>().CreateLogger("Ameto");
    var addresses = app.Services.GetRequiredService<Microsoft.AspNetCore.Hosting.Server.IServer>()
                       .Features.Get<Microsoft.AspNetCore.Hosting.Server.Features.IServerAddressesFeature>()
                       ?.Addresses;
    logger.LogInformation("Ameto version: {Version}", UpdateChecker.CurrentVersion);
    logger.LogInformation("Content root: {ContentRoot}", app.Environment.ContentRootPath);
    if (unknownBackfillValue is not null)
        logger.LogWarning(
            "Ameto:Traces:IndexBackfill is {Value}, which is not Off, Idle or Eager — using Idle. "
          + "Idle does MORE work than Off, so a typo here does not mean less", unknownBackfillValue);
    if (!basePath.IsRoot)
        logger.LogInformation(
            "Base path: {BasePath} — the UI and every endpoint also answer under this prefix. " +
            "Paths at the root keep working, so health checks and existing senders are unaffected.",
            basePath.PathBase);
    if (repOpts.Enabled && string.IsNullOrEmpty(repOpts.Secret))
        logger.LogWarning(
            "Replication is enabled but Ameto:Replication:Secret is not set — peer endpoints " +
            "reject all requests (fail-closed). Set the same secret on every node to replicate.");
    if (serverOptions.TrustForwardedHeaders && serverOptions.KnownProxies.Length == 0)
        logger.LogWarning(
            "TrustForwardedHeaders is on with no Ameto:KnownProxies — any client can spoof its " +
            "scheme/host/IP. List your reverse-proxy IP(s) in Ameto:KnownProxies to restrict trust.");
    if (authOptions.Microsoft is { IsMisconfigured: true })
        logger.LogError(
            "Microsoft sign-in is configured but DISABLED: Ameto:Auth:Microsoft:TenantId is unset or " +
            "tenant-agnostic ('common'/'organizations'/'consumers'). Any Entra tenant could then assert " +
            "any email address, and the allowlist is keyed on that address. Set your tenant id, or set " +
            "Ameto:Auth:Microsoft:AllowMultiTenant to true to accept the risk.");
    logger.LogInformation("Listening on: {Urls}",
        addresses is { Count: > 0 } ? string.Join(", ", addresses) : "(none)");
});

app.Run();

// Make the implicit Program class accessible to integration tests
public partial class Program { }
