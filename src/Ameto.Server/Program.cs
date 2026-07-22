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

// ── Core services ─────────────────────────────────────────────────────────────
builder.Services
    .AddAmetoStorage()
    .AddAmetoIndexing()
    .AddAmetoIngestion()
    .AddAmetoQuery();

// Short-TTL cache for GET /api/events/counts header-scan responses.
builder.Services.AddSingleton<LogVolumeCountsCache>();

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

// Distributed tracing
if (enableTracing)
    builder.Services.AddAmetoTracing(serverOptions.DataDirectory);

// Metrics
if (enableMetrics)
    builder.Services.AddAmetoMetrics(serverOptions.DataDirectory);

var repOpts = builder.Configuration.GetSection("Ameto:Replication").Get<ReplicationOptions>() ?? new ReplicationOptions();
builder.Services.AddAmetoReplication(repOpts);

// ── Kestrel ───────────────────────────────────────────────────────────────────
if (!string.IsNullOrEmpty(serverOptions.SslCertPath))
{
  builder.WebHost.UseUrls($"https://*:{serverOptions.HttpPort}");

  // Hot-reloadable certificate: Kestrel invokes the selector on every TLS
  // handshake, so replacing the .pfx file on disk causes new connections to
  // use the new cert without restarting the process.
  var certReloader = new HotReloadCertificate(
      serverOptions.SslCertPath, serverOptions.SslCertPassword,
      LoggerFactory.Create(b => b.AddConsole()).CreateLogger<HotReloadCertificate>());
  builder.Services.AddSingleton(certReloader);

  builder.WebHost.ConfigureKestrel(k =>
      k.ConfigureHttpsDefaults(h =>
          h.ServerCertificateSelector = (_, _) => certReloader.Current));
}
else
{
  builder.WebHost.UseUrls($"http://*:{serverOptions.HttpPort}");
}
var app = builder.Build();

// ── Middleware ─────────────────────────────────────────────────────────────────
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
    fwd.KnownNetworks.Clear();
    fwd.KnownProxies.Clear();
    app.UseForwardedHeaders(fwd);
}
app.UseAuthentication();
app.UseAuthorization();
app.UseDefaultFiles();
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
app.MapOtlpEndpoints(enableTracing, enableMetrics);
if (enableMetrics)
    app.MapMetricEndpoints();
if (enableTracing)
    app.MapTraceEndpoints();

// SPA fallback — Angular handles client-side routing
app.MapFallbackToFile("index.html");

// ── Startup banner ────────────────────────────────────────────────────────────
app.Lifetime.ApplicationStarted.Register(() =>
{
    var logger    = app.Services.GetRequiredService<ILoggerFactory>().CreateLogger("Ameto");
    var addresses = app.Services.GetRequiredService<Microsoft.AspNetCore.Hosting.Server.IServer>()
                       .Features.Get<Microsoft.AspNetCore.Hosting.Server.Features.IServerAddressesFeature>()
                       ?.Addresses;
    logger.LogInformation("Ameto version: {Version}", UpdateChecker.CurrentVersion);
    logger.LogInformation("Content root: {ContentRoot}", app.Environment.ContentRootPath);
    logger.LogInformation("Listening on: {Urls}",
        addresses is { Count: > 0 } ? string.Join(", ", addresses) : "(none)");
});

app.Run();

// Make the implicit Program class accessible to integration tests
public partial class Program { }
