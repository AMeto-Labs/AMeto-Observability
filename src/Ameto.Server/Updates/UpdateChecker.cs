using System.Diagnostics;
using System.Net;
using System.Net.Http.Headers;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text.Json;
using Ameto.Core;

namespace Ameto.Server.Updates;

/// <summary>One downloadable file attached to the latest GitHub release.</summary>
public sealed record ReleaseAsset(string Name, string DownloadUrl);

/// <summary>Immutable snapshot of the most recent successful check.</summary>
public sealed record LatestRelease(
    string Tag,
    string Url,
    DateTimeOffset? PublishedAt,
    ReleaseAsset[] Assets);

/// <summary>
/// Polls the GitHub Releases API on a timer (conditional ETag requests — a 304
/// costs nothing against the rate limit) and keeps the latest release in memory
/// for the Settings → Updates endpoints. Cold path: runs once an hour, so plain
/// async code — no pooling gymnastics needed here.
/// </summary>
public sealed class UpdateChecker(ServerOptions options, ILogger<UpdateChecker> logger) : BackgroundService
{
    private static readonly HttpClient Http = CreateClient();

    private readonly UpdatesOptions _opts = options.Updates;

    // Snapshot state — written by the check loop, read by the endpoints.
    private volatile LatestRelease? _latest;
    private string?         _etag;
    private DateTimeOffset? _checkedAt;
    private volatile string? _lastError;
    private int _applying;               // 1 while an installer download/launch is in flight

    /// <summary>Version stamped at build time (-p:Version=…); "1.0.0-dev" for local builds.</summary>
    public static readonly string CurrentVersion =
        typeof(UpdateChecker).Assembly
            .GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion
        is { Length: > 0 } v ? StripBuildMetadata(v) : "dev";

    public LatestRelease?  Latest      => _latest;
    public DateTimeOffset? CheckedAt   => _checkedAt;
    public string?         LastError   => _lastError;
    public bool            ApplyInProgress => Volatile.Read(ref _applying) == 1;

    public bool UpdateAvailable
    {
        get
        {
            var latest = _latest;
            if (latest is null) return false;
            // Dev/unstamped builds never nag — they are not a released version.
            if (!TryParseVersion(CurrentVersion, out var cur)) return false;
            return TryParseVersion(latest.Tag, out var next) && next > cur;
        }
    }

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        if (!_opts.Enabled) return;

        var interval = TimeSpan.FromMinutes(Math.Max(15, _opts.CheckIntervalMinutes));
        // First check shortly after startup so the UI has data without waiting a full hour.
        await Task.Delay(TimeSpan.FromSeconds(15), ct).ConfigureAwait(false);
        while (!ct.IsCancellationRequested)
        {
            await CheckAsync(ct).ConfigureAwait(false);
            await Task.Delay(interval, ct).ConfigureAwait(false);
        }
    }

    /// <summary>One conditional GET of /releases/latest. Never throws.</summary>
    public async Task CheckAsync(CancellationToken ct)
    {
        try
        {
            using var req = new HttpRequestMessage(
                HttpMethod.Get, $"https://api.github.com/repos/{_opts.GitHubRepository}/releases/latest");
            if (_etag is { } etag)
                req.Headers.IfNoneMatch.Add(new EntityTagHeaderValue(etag, isWeak: true));

            using var res = await Http.SendAsync(req, HttpCompletionOption.ResponseHeadersRead, ct).ConfigureAwait(false);

            if (res.StatusCode == HttpStatusCode.NotModified)
            {
                _checkedAt = DateTimeOffset.UtcNow;
                _lastError = null;
                return;
            }
            if (!res.IsSuccessStatusCode)
            {
                // 403/429 = rate limited; anything else is transient. Keep the old
                // snapshot and try again on the next tick.
                _lastError = $"GitHub API: {(int)res.StatusCode} {res.ReasonPhrase}";
                return;
            }

            await using var stream = await res.Content.ReadAsStreamAsync(ct).ConfigureAwait(false);
            using var doc = await JsonDocument.ParseAsync(stream, cancellationToken: ct).ConfigureAwait(false);
            var root = doc.RootElement;

            var tag = root.GetProperty("tag_name").GetString();
            if (string.IsNullOrEmpty(tag)) { _lastError = "GitHub API: release without tag_name"; return; }

            var assets = Array.Empty<ReleaseAsset>();
            if (root.TryGetProperty("assets", out var arr) && arr.ValueKind == JsonValueKind.Array)
            {
                assets = new ReleaseAsset[arr.GetArrayLength()];
                int i = 0;
                foreach (var a in arr.EnumerateArray())
                    assets[i++] = new ReleaseAsset(
                        a.GetProperty("name").GetString() ?? "",
                        a.GetProperty("browser_download_url").GetString() ?? "");
            }

            _latest = new LatestRelease(
                tag,
                root.TryGetProperty("html_url", out var u) ? u.GetString() ?? "" : "",
                root.TryGetProperty("published_at", out var p) && p.ValueKind == JsonValueKind.String
                    ? p.GetDateTimeOffset() : null,
                assets);
            _etag      = res.Headers.ETag?.Tag;
            _checkedAt = DateTimeOffset.UtcNow;
            _lastError = null;

            if (UpdateAvailable)
                logger.LogInformation("Update available: {Current} → {Latest}", CurrentVersion, tag);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested) { }
        catch (Exception ex)
        {
            _lastError = ex.Message;
            logger.LogDebug(ex, "Update check failed");
        }
    }

    /// <summary>
    /// Windows-only self-update: downloads the installer asset for this
    /// architecture, verifies its SHA-256 against the published checksums file,
    /// then launches it silently. The installer stops the service, replaces the
    /// binaries and starts the service again — this process dies mid-way, which
    /// is expected.
    /// </summary>
    public async Task<(bool Ok, string Message)> TryApplyAsync(CancellationToken ct)
    {
        if (!OperatingSystem.IsWindows() || IsContainer())
            return (false, "Self-update is only supported on Windows installs.");
        var latest = _latest;
        if (latest is null)      return (false, "No release information yet — check for updates first.");
        if (!UpdateAvailable)    return (false, "Already on the latest version.");

        if (Interlocked.Exchange(ref _applying, 1) == 1)
            return (false, "An update is already in progress.");
        try
        {
            var arch      = RuntimeInformation.OSArchitecture == Architecture.X86 ? "x86" : "x64";
            var installer = Find(latest.Assets, $"-setup-{arch}.exe");
            var sums      = Find(latest.Assets, "SHA256SUMS-win-installer.txt");
            if (installer is null) return (false, $"Release {latest.Tag} has no {arch} installer asset.");

            var path = Path.Combine(Path.GetTempPath(), installer.Name);
            await DownloadAsync(installer.DownloadUrl, path, ct).ConfigureAwait(false);

            if (sums is not null && !await VerifySha256Async(path, installer.Name, sums.DownloadUrl, ct).ConfigureAwait(false))
            {
                File.Delete(path);
                return (false, "Checksum verification failed — download discarded.");
            }

            logger.LogInformation("Launching installer {Path} for {Tag}", path, latest.Tag);
            Process.Start(new ProcessStartInfo
            {
                FileName        = path,
                Arguments       = "/VERYSILENT /SUPPRESSMSGBOXES /NORESTART",
                UseShellExecute = false,
                CreateNoWindow  = true,
            });
            return (true, $"Installer for {latest.Tag} started — the server will restart shortly.");
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Self-update failed");
            return (false, $"Update failed: {ex.Message}");
        }
        finally
        {
            // On success the flag stays honest too: the service is about to be
            // stopped by the installer, so clearing it here is harmless.
            Volatile.Write(ref _applying, 0);
        }
    }

    public static bool IsContainer() =>
        Environment.GetEnvironmentVariable("DOTNET_RUNNING_IN_CONTAINER") == "true";

    // ── helpers ──────────────────────────────────────────────────────────────

    private static HttpClient CreateClient()
    {
        var http = new HttpClient { Timeout = TimeSpan.FromSeconds(30) };
        // GitHub rejects requests without a User-Agent.
        http.DefaultRequestHeaders.UserAgent.Add(new ProductInfoHeaderValue("Ameto", CurrentVersion));
        http.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/vnd.github+json"));
        return http;
    }

    private static ReleaseAsset? Find(ReleaseAsset[] assets, string suffix)
    {
        foreach (var a in assets)
            if (a.Name.EndsWith(suffix, StringComparison.OrdinalIgnoreCase))
                return a;
        return null;
    }

    private static async Task DownloadAsync(string url, string path, CancellationToken ct)
    {
        // Asset downloads take much longer than API calls — use a per-call timeout.
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(TimeSpan.FromMinutes(10));
        using var res = await Http.GetAsync(url, HttpCompletionOption.ResponseHeadersRead, cts.Token).ConfigureAwait(false);
        res.EnsureSuccessStatusCode();
        await using var file = File.Create(path);
        await res.Content.CopyToAsync(file, cts.Token).ConfigureAwait(false);
    }

    private static async Task<bool> VerifySha256Async(string path, string fileName, string sumsUrl, CancellationToken ct)
    {
        var sums = await Http.GetStringAsync(sumsUrl, ct).ConfigureAwait(false);
        string? expected = null;
        foreach (var line in sums.AsSpan().EnumerateLines())
        {
            // "abc123…  ameto-v1.0.11-setup-x64.exe"
            var trimmed = line.Trim();
            if (trimmed.EndsWith(fileName, StringComparison.OrdinalIgnoreCase))
            {
                expected = trimmed[..64].ToString();
                break;
            }
        }
        if (expected is null) return false;

        await using var file = File.OpenRead(path);
        var hash = await SHA256.HashDataAsync(file, ct).ConfigureAwait(false);
        return string.Equals(Convert.ToHexStringLower(hash), expected, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>"1.0.11+abc123" → "1.0.11" (SourceLink appends commit metadata).</summary>
    private static string StripBuildMetadata(string v)
    {
        int plus = v.IndexOf('+');
        return plus > 0 ? v[..plus] : v;
    }

    /// <summary>
    /// "v1.0.11" → 1.0.11. Pre-release strings ("1.0.0-dev") deliberately fail:
    /// a dev build is not a released version, so it never reports an update.
    /// </summary>
    internal static bool TryParseVersion(string tag, out Version version)
    {
        var span = tag.AsSpan().Trim();
        if (span.StartsWith("v", StringComparison.OrdinalIgnoreCase)) span = span[1..];
        int dash = span.IndexOf('-');
        if (dash == 0) { version = new Version(); return false; }
        if (dash > 0)
        {
            // Pre-release ("1.0.0-dev") is never a comparable released version.
            version = new Version();
            return false;
        }
        return Version.TryParse(span, out version!);
    }
}
