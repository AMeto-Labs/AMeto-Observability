using System.Collections.Concurrent;
using System.Text;
using Microsoft.Extensions.Logging;

namespace Ameto.Server;

/// <summary>
/// Minimal rolling-file logger provider — no external logging dependency.
///
/// <para>WHY THIS EXISTS: running as a Windows Service there is no console, and
/// <c>AddWindowsService</c> routes host logs to the Windows Event Log, whose provider
/// applies its OWN default minimum of <see cref="LogLevel.Warning"/> regardless of
/// <c>builder.Logging.SetMinimumLevel(Information)</c>. Every Information-level
/// diagnostic — the 30-second <c>MEM ws=… gc_heap=… hot_tier=…</c> attribution line,
/// "Flushed segment", "Merged N small segments", the startup flush budgets — was
/// therefore written nowhere at all on the one deployment target that cannot show a
/// console. Raising the Event Log filter instead would spam a shared, size-capped
/// system log, so the server gets its own file.</para>
///
/// Writes are serialised through a single background drain so request threads never
/// block on disk. Files roll daily and old ones are pruned by count.
/// </summary>
public sealed class FileLoggerProvider : ILoggerProvider
{
    private readonly string                        _dir;
    private readonly LogLevel                      _min;
    private readonly int                           _retainDays;
    private readonly BlockingCollection<string>    _queue = new(boundedCapacity: 8192);
    private readonly Task                          _drain;
    private readonly CancellationTokenSource       _cts   = new();

    private StreamWriter? _writer;
    private DateOnly      _openFor;

    public FileLoggerProvider(string directory, LogLevel minimumLevel, int retainDays = 7)
    {
        _dir        = directory;
        _min        = minimumLevel;
        _retainDays = Math.Max(1, retainDays);
        Directory.CreateDirectory(_dir);
        _drain = Task.Run(DrainAsync);
    }

    public ILogger CreateLogger(string categoryName) => new FileLogger(this, categoryName);

    internal bool IsEnabled(LogLevel level) => level >= _min && level != LogLevel.None;

    /// <summary>Hands a formatted line to the drain. Drops rather than blocks when the
    /// queue is saturated — losing a log line must never stall ingest.</summary>
    internal void Enqueue(string line)
    {
        if (_queue.IsAddingCompleted) return;
        try { _queue.TryAdd(line); }
        catch (InvalidOperationException) { /* completed concurrently */ }
    }

    private async Task DrainAsync()
    {
        try
        {
            foreach (var line in _queue.GetConsumingEnumerable(_cts.Token))
            {
                try
                {
                    var today = DateOnly.FromDateTime(DateTime.Now);
                    if (_writer is null || today != _openFor) Roll(today);
                    await _writer!.WriteLineAsync(line).ConfigureAwait(false);
                }
                catch { /* a broken log file must not take the server down */ }
            }
        }
        catch (OperationCanceledException) { }
        catch (InvalidOperationException)  { /* collection completed */ }
        finally
        {
            try { _writer?.Flush(); _writer?.Dispose(); } catch { }
        }
    }

    private void Roll(DateOnly day)
    {
        try { _writer?.Flush(); _writer?.Dispose(); } catch { }
        var path = Path.Combine(_dir, $"ameto-{day:yyyyMMdd}.log");
        _writer  = new StreamWriter(
            new FileStream(path, FileMode.Append, FileAccess.Write, FileShare.ReadWrite),
            Encoding.UTF8) { AutoFlush = true };
        _openFor = day;
        Prune();
    }

    private void Prune()
    {
        try
        {
            var files = new DirectoryInfo(_dir).GetFiles("ameto-*.log");
            if (files.Length <= _retainDays) return;
            Array.Sort(files, static (a, b) => b.Name.CompareTo(a.Name)); // newest first
            for (int i = _retainDays; i < files.Length; i++)
                try { files[i].Delete(); } catch { /* locked — next roll retries */ }
        }
        catch { /* best-effort */ }
    }

    public void Dispose()
    {
        try { _queue.CompleteAdding(); } catch { }
        try { _drain.Wait(TimeSpan.FromSeconds(5)); } catch { }
        _cts.Cancel();
        _cts.Dispose();
        _queue.Dispose();
    }

    private sealed class FileLogger(FileLoggerProvider owner, string category) : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel level) => owner.IsEnabled(level);

        public void Log<TState>(
            LogLevel level, EventId eventId, TState state, Exception? ex,
            Func<TState, Exception?, string> formatter)
        {
            if (!owner.IsEnabled(level)) return;

            var sb = new StringBuilder(160);
            sb.Append(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff"))
              .Append(" [").Append(Short(level)).Append("] ")
              .Append(category).Append(": ")
              .Append(formatter(state, ex));
            if (ex is not null) sb.Append(Environment.NewLine).Append(ex);

            owner.Enqueue(sb.ToString());
        }

        private static string Short(LogLevel l) => l switch
        {
            LogLevel.Trace       => "TRC",
            LogLevel.Debug       => "DBG",
            LogLevel.Information => "INF",
            LogLevel.Warning     => "WRN",
            LogLevel.Error       => "ERR",
            LogLevel.Critical    => "CRT",
            _                    => "???",
        };
    }
}
