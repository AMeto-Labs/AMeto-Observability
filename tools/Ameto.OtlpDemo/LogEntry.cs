using System.Text.Json;
using System.Text.Json.Serialization;

/// <summary>Immutable structured log entry produced by <see cref="LogGeneratorService"/>.</summary>
internal sealed record LogEntry(
    DateTimeOffset Timestamp,
    string         Level,
    string         Operation,
    string         User,
    int            StatusCode,
    double         DurationMs,
    string         TraceId,
    string         Message);

/// <summary>
/// Thread-safe store: keeps all <see cref="LogEntry"/> records in memory
/// and appends each one as a JSON line to <c>logs/entries.ndjson</c>.
/// </summary>
internal sealed class LogStore
{
    private readonly Lock           _lock    = new();
    private readonly List<LogEntry> _entries = new(4096);
    private readonly string         _path;

    // Reuse serializer options — avoids per-call option object allocation
    private static readonly JsonSerializerOptions s_json =
        new() { TypeInfoResolver = AppJsonCtx.Default };

    public LogStore()
    {
        Directory.CreateDirectory("logs");
        _path = Path.Combine("logs", "entries.ndjson");
    }

    /// <summary>Appends <paramref name="entry"/> to memory and to the NDJSON file.</summary>
    public void Add(LogEntry entry)
    {
        lock (_lock)
        {
            _entries.Add(entry);

            // AppendText opens, writes one line, and closes — no truncation risk
            using var sw = File.AppendText(_path);
            sw.WriteLine(JsonSerializer.Serialize(entry, s_json));
        }
    }

    public IReadOnlyList<LogEntry> GetAll()
    {
        lock (_lock)
            return _entries.ToArray();
    }

    public int Count
    {
        get { lock (_lock) return _entries.Count; }
    }
}

/// <summary>
/// Payload accepted by <c>POST /logs/ingest</c>.
/// Mirrors key fields from Seq/CLEF so callers can forward real log entries via curl.
/// </summary>
internal sealed record IngestRequest(
    string Level,
    string Message,
    [property: JsonPropertyName("sourceContext")] string? SourceContext,
    Dictionary<string, JsonElement>? Properties);
