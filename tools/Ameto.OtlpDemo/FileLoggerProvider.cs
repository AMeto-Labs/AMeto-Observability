/// <summary>
/// Minimal file <see cref="ILoggerProvider"/> — appends plain-text log lines
/// to a single file without third-party dependencies.
/// </summary>
internal sealed class FileLoggerProvider : ILoggerProvider
{
    private readonly StreamWriter _writer;
    private readonly Lock         _lock = new();

    public FileLoggerProvider(string path)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(Path.GetFullPath(path))!);
        _writer = new StreamWriter(path, append: true, System.Text.Encoding.UTF8)
        {
            AutoFlush = true,
        };
    }

    public ILogger CreateLogger(string categoryName) =>
        new FileLogger(categoryName, _writer, _lock);

    public void Dispose() => _writer.Dispose();
}

/// <summary>Writes a single formatted line per log event to a shared <see cref="StreamWriter"/>.</summary>
internal sealed class FileLogger(string category, StreamWriter writer, Lock @lock) : ILogger
{
    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

    public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Information;

    public void Log<TState>(
        LogLevel                        logLevel,
        EventId                         eventId,
        TState                          state,
        Exception?                      exception,
        Func<TState, Exception?, string> formatter)
    {
        if (!IsEnabled(logLevel)) return;

        // [2026-05-21T12:34:56.000Z INF] Category - Message
        ReadOnlySpan<char> lvl = logLevel switch
        {
            LogLevel.Trace       => "TRC",
            LogLevel.Debug       => "DBG",
            LogLevel.Information => "INF",
            LogLevel.Warning     => "WRN",
            LogLevel.Error       => "ERR",
            LogLevel.Critical    => "CRT",
            _                    => "???",
        };

        string line = $"[{DateTime.UtcNow:O} {lvl}] {category} - {formatter(state, exception)}";
        if (exception is not null)
            line = string.Concat(line, Environment.NewLine, exception.ToString());

        lock (@lock)
            writer.WriteLine(line);
    }
}
