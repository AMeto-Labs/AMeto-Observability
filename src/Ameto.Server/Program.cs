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
using Ameto.Storage;
using Ameto.Tracing;

var builder = WebApplication.CreateBuilder(new WebApplicationOptions
{
    Args            = args,
    // Use the DLL's directory as content root so wwwroot is found regardless
    // of the working directory the process is started from.
    ContentRootPath = AppContext.BaseDirectory,
});
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

// Alert rules store + evaluator
builder.Services.AddAmetoAlerts(serverOptions.DataDirectory);

// Distributed tracing
builder.Services.AddAmetoTracing(serverOptions.DataDirectory);

// Metrics
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
app.UseAuthentication();
app.UseAuthorization();
app.UseDefaultFiles();
app.UseStaticFiles();

// ── Endpoints ─────────────────────────────────────────────────────────────────
app.MapAuthEndpoints();
app.MapAmetoEndpoints();
app.MapAlertEndpoints();
app.MapRetentionEndpoints();
app.MapDiagnosticsEndpoints();
app.MapReplicationEndpoints();
app.MapOtlpEndpoints();
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
    logger.LogInformation("Content root: {ContentRoot}", app.Environment.ContentRootPath);
    logger.LogInformation("Listening on: {Urls}",
        addresses is { Count: > 0 } ? string.Join(", ", addresses) : "(none)");
});

app.Run();

// Make the implicit Program class accessible to integration tests
public partial class Program { }
