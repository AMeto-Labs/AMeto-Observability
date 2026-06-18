using System.Security.Cryptography.X509Certificates;

namespace Ameto.Server;

/// <summary>
/// Loads a PKCS#12 (.pfx) certificate from disk and transparently reloads it
/// when the file changes, so Kestrel can pick up a renewed certificate on the
/// next TLS handshake without restarting the application.
///
/// Existing TLS connections continue to use the previously negotiated cert
/// until they are reconnected — this matches standard nginx / IIS behavior.
/// </summary>
internal sealed class HotReloadCertificate : IDisposable
{
    private readonly string _path;
    private readonly string? _password;
    private readonly ILogger _logger;
    private readonly FileSystemWatcher _watcher;
    private readonly object _gate = new();

    private X509Certificate2 _current;
    private DateTime _loadedAtUtc;
    private long _loadedSize;

    public HotReloadCertificate(string path, string? password, ILogger logger)
    {
        _path = Path.GetFullPath(path);
        _password = password;
        _logger = logger;
        _current = LoadFromDisk();

        var dir = Path.GetDirectoryName(_path) ?? ".";
        var file = Path.GetFileName(_path);
        _watcher = new FileSystemWatcher(dir, file)
        {
            NotifyFilter = NotifyFilters.LastWrite | NotifyFilters.Size |
                           NotifyFilters.CreationTime | NotifyFilters.FileName,
            EnableRaisingEvents = true,
        };
        _watcher.Changed += OnChanged;
        _watcher.Created += OnChanged;
        _watcher.Renamed += OnChanged;
    }

    /// <summary>Current cert. Safe to call from any thread.</summary>
    public X509Certificate2 Current
    {
        get { lock (_gate) return _current; }
    }

    private X509Certificate2 LoadFromDisk()
    {
        var info = new FileInfo(_path);
        var cert = X509CertificateLoader.LoadPkcs12FromFile(_path, _password);
        _loadedAtUtc = info.LastWriteTimeUtc;
        _loadedSize  = info.Length;
        _logger.LogInformation(
            "Loaded TLS certificate '{Path}' (subject={Subject}, thumbprint={Thumb}, notAfter={NotAfter:u}).",
            _path, cert.Subject, cert.Thumbprint, cert.NotAfter);
        return cert;
    }

    private void OnChanged(object sender, FileSystemEventArgs e)
    {
        // FileSystemWatcher can fire multiple events for a single write
        // (and antivirus / atomic-rename tools often emit deletes).
        // Retry briefly while the writer finishes.
        Task.Run(async () =>
        {
            for (var attempt = 0; attempt < 10; attempt++)
            {
                try
                {
                    await Task.Delay(200);
                    if (!File.Exists(_path)) continue;

                    var info = new FileInfo(_path);
                    if (info.LastWriteTimeUtc == _loadedAtUtc && info.Length == _loadedSize)
                        return; // unchanged

                    X509Certificate2 next;
                    lock (_gate)
                    {
                        next = LoadFromDisk();
                        var old = _current;
                        _current = next;
                        old.Dispose();
                    }
                    return;
                }
                catch (Exception ex) when (attempt < 9)
                {
                    _logger.LogWarning(ex,
                        "Certificate reload attempt {Attempt} failed; will retry.", attempt + 1);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex,
                        "Failed to hot-reload TLS certificate '{Path}'. Keeping previous cert.",
                        _path);
                }
            }
        });
    }

    public void Dispose()
    {
        _watcher.Dispose();
        lock (_gate) _current.Dispose();
    }
}
