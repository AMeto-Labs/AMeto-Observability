using System.Text.Json.Serialization;

// Source-gen context — eliminates runtime reflection for all serialized types.
[JsonSerializable(typeof(LogEntry))]
[JsonSerializable(typeof(LogEntry[]))]
[JsonSerializable(typeof(List<LogEntry>))]
[JsonSerializable(typeof(IngestRequest))]
[JsonSerializable(typeof(Dictionary<string, System.Text.Json.JsonElement>))]
[JsonSerializable(typeof(SeqEvent))]
[JsonSerializable(typeof(SeqEvent[]))]
[JsonSerializable(typeof(SeqProperty))]
[JsonSerializable(typeof(SeqToken))]
[JsonSerializable(typeof(SeqSignalPayload))]
[JsonSerializable(typeof(SeqSignalFilter))]
[JsonSerializable(typeof(SeqSignalLinks))]
internal sealed partial class AppJsonCtx : JsonSerializerContext { }
