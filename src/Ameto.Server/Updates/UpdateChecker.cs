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

/// <summary>Self-update lifecycle: download (with progress) → explicit install approval.</summary>
public enum UpdatePhase { Idle, Downloading, Ready, Installing, Failed }

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

    // Self-update state machine (download progress → install approval).
    private volatile UpdatePhase _phase = UpdatePhase.Idle;
    private long    _dlBytes;            // Interlocked-updated by the download worker
    private long    _dlTotal;            // 0 until Content-Length is known
    private volatile string? _dlVersion; // tag the downloaded file belongs to
    private volatile string? _dlPath;    // verified installer on disk (phase == Ready)
    private volatile string? _dlError;

    /// <summary>Version stamped at build time (-p:Version=…); "1.0.0-dev" for local builds.</summary>
    public static readonly string CurrentVersion =
        typeof(UpdateChecker).Assembly
            .GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion
        is { Length: > 0 } v ? StripBuildMetadata(v) : "dev";

    public LatestRelease?  Latest      => _latest;
    public DateTimeOffset? CheckedAt   => _checkedAt;
    public string?         LastError   => _lastError;

    public UpdatePhase Phase           => _phase;
    public long        DownloadedBytes => Interlocked.Read(ref _dlBytes);
    public long        DownloadTotalBytes => Interlocked.Read(ref _dlTotal);
    public string?     DownloadedVersion  => _dlVersion;
    public string?     DownloadError      => _dlError;

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
    /// Phase 1 of the Windows self-update: kicks off a background download of the
    /// installer asset for this architecture (progress is exposed via
    /// <see cref="DownloadedBytes"/>/<see cref="DownloadTotalBytes"/>), verifies its
    /// SHA-256 against the published checksums file and parks the file on disk.
    /// NOTHING is installed and the server keeps running — the restart only happens
    /// when the admin explicitly approves via <see cref="TryInstall"/>.
    /// </summary>
    public (bool Ok, string Message) StartDownload()
    {
        if (!OperatingSystem.IsWindows() || IsContainer())
            return (false, "Self-update is only supported on Windows installs.");
        var latest = _latest;
        if (latest is null)      return (false, "No release information yet — check for updates first.");
        if (!UpdateAvailable)    return (false, "Already on the latest version.");

        switch (_phase)
        {
            case UpdatePhase.Downloading: return (true, "Download already in progress.");
            case UpdatePhase.Installing:  return (false, "An install is already in progress.");
            case UpdatePhase.Ready when _dlVersion == latest.Tag && File.Exists(_dlPath):
                return (true, $"{latest.Tag} is already downloaded and verified.");
        }

        var arch      = RuntimeInformation.OSArchitecture == Architecture.X86 ? "x86" : "x64";
        var installer = Find(latest.Assets, $"-setup-{arch}.exe");
        if (installer is null) return (false, $"Release {latest.Tag} has no {arch} installer asset.");
        var sums = Find(latest.Assets, "SHA256SUMS-win-installer.txt");

        _phase     = UpdatePhase.Downloading;
        _dlVersion = latest.Tag;
        _dlError   = null;
        _dlPath    = null;
        Interlocked.Exchange(ref _dlBytes, 0);
        Interlocked.Exchange(ref _dlTotal, 0);

        _ = Task.Run(() => DownloadWorkerAsync(latest.Tag, installer, sums));
        return (true, $"Downloading {latest.Tag}…");
    }

    private async Task DownloadWorkerAsync(string tag, ReleaseAsset installer, ReleaseAsset? sums)
    {
        var path = Path.Combine(Path.GetTempPath(), installer.Name);
        try
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(15));
            using var res = await Http.GetAsync(installer.DownloadUrl,
                HttpCompletionOption.ResponseHeadersRead, cts.Token).ConfigureAwait(false);
            res.EnsureSuccessStatusCode();
            Interlocked.Exchange(ref _dlTotal, res.Content.Headers.ContentLength ?? 0);

            await using (var src = await res.Content.ReadAsStreamAsync(cts.Token).ConfigureAwait(false))
            await using (var dst = File.Create(path))
            {
                var buf = new byte[81920];
                int n;
                while ((n = await src.ReadAsync(buf, cts.Token).ConfigureAwait(false)) > 0)
                {
                    await dst.WriteAsync(buf.AsMemory(0, n), cts.Token).ConfigureAwait(false);
                    Interlocked.Add(ref _dlBytes, n);
                }
            }

            if (sums is not null && !await VerifySha256Async(path, installer.Name, sums.DownloadUrl, cts.Token).ConfigureAwait(false))
            {
                File.Delete(path);
                _dlError = "Checksum verification failed — download discarded.";
                _phase   = UpdatePhase.Failed;
                return;
            }

            _dlPath = path;
            _phase  = UpdatePhase.Ready;
            logger.LogInformation("Update {Tag} downloaded and verified: {Path}", tag, path);
        }
        catch (Exception ex)
        {
            try { File.Delete(path); } catch { /* best effort */ }
            _dlError = ex.Message;
            _phase   = UpdatePhase.Failed;
            logger.LogWarning(ex, "Update download failed");
        }
    }

    /// <summary>
    /// Phase 2 — the admin's explicit approval: launches the already-downloaded,
    /// verified installer silently. The installer stops the service, replaces the
    /// binaries and starts the service again — this process dies mid-way, which
    /// is expected.
    /// </summary>
    public (bool Ok, string Message) TryInstall()
    {
        var latest = _latest;
        if (_phase != UpdatePhase.Ready || _dlPath is null)
            return (false, "No verified installer is ready — download the update first.");
        if (latest is not null && _dlVersion != latest.Tag)
            return (false, $"Downloaded {_dlVersion} but the latest release is now {latest.Tag} — download again.");
        if (!File.Exists(_dlPath))
        {
            _phase = UpdatePhase.Idle;
            return (false, "The downloaded installer is gone — download the update again.");
        }

        try
        {
            _phase = UpdatePhase.Installing;
            logger.LogInformation("Launching installer {Path} for {Tag}", _dlPath, _dlVersion);
            Process.Start(new ProcessStartInfo
            {
                FileName        = _dlPath,
                Arguments       = "/VERYSILENT /SUPPRESSMSGBOXES /NORESTART",
                // Service (LocalSystem): already elevated — CreateProcess starts the
                // installer silently, no prompts. Interactive (portable/dev) run:
                // ShellExecute is the only route, and Windows raises UAC there.
                UseShellExecute = Environment.UserInteractive,
                CreateNoWindow  = true,
            });
            return (true, $"Installing {_dlVersion} — the server will restart shortly.");
        }
        catch (Exception ex)
        {
            _phase   = UpdatePhase.Failed;
            _dlError = ex.Message;
            logger.LogWarning(ex, "Installer launch failed");
            return (false, $"Installer launch failed: {ex.Message}");
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
