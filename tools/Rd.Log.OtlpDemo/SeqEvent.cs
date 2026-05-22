using System.Text.Json;
using System.Text.Json.Serialization;

/// <summary>One event entry as returned by the Seq <c>/api/events</c> REST endpoint.</summary>
internal sealed record SeqEvent(
    DateTimeOffset Timestamp,
    SeqProperty[]  Properties,
    SeqToken[]?    MessageTemplateTokens,
    string?        EventType,
    string         Level,
    string         Id);

/// <summary>A single structured property attached to a Seq event.</summary>
/// <remarks><see cref="Value"/> is kept as <see cref="JsonElement"/> so any JSON type is preserved.</remarks>
internal sealed record SeqProperty(string Name, JsonElement Value);

/// <summary>One token of a Seq message template — either literal text or a property placeholder.</summary>
internal sealed record SeqToken(
    string? Text,
    string? PropertyName,
    string? RawText);

/// <summary>Loads <c>response.json</c> (Seq event array) once at startup and holds it in memory.</summary>
internal sealed class SeqEventStore
{
    private readonly SeqEvent[] _events;

    public SeqEventStore(string path)
    {
        if (!File.Exists(path))
        {
            _events = [];
            return;
        }

        //using var stream = File.OpenRead(path);
        //_events = JsonSerializer.Deserialize(stream, AppJsonCtx.Default.SeqEventArray) ?? [];
    }

    public SeqEvent? GetRandom() =>
        _events.Length > 0 ? _events[Random.Shared.Next(_events.Length)] : null;

    public int Count => _events?.Length ?? 0;
}
