using System.Text.Json;
using Ameto.Core;

namespace Ameto.Otel;

/// <summary>
/// What an OTLP logs batch will decode to, weighed before a record is staged: the receivers refuse a
/// batch heavier than <c>Ingestion.MaxDecodedLogBatchBytes</c> with 413 (gRPC: RESOURCE_EXHAUSTED),
/// nothing of it ingested, and reserve the weight of one they take against what the batches held
/// decoded at once may weigh (<c>IngestionEndpoint.TryBeginBatch</c>).
///
/// <para><b>Why.</b> A log batch is decoded whole before any of it is written — the request is
/// answered only once its records are in the store — and an OTLP record decodes to much more than
/// it was on the wire when it is small: a staged record costs an item (~80 B, and a share of the
/// doubling array that holds it) besides its properties, and its properties begin with a copy of
/// its resource's attributes. An 8 MiB body of empty LogRecords, two bytes each, decoded to ~4 M
/// items, ~540 MB — past a 512 MB container's whole heap; one of small records under kilobytes of
/// resource attributes to many times its size. Nothing bounded it but the body's byte limit.</para>
///
/// <para><b>The weight.</b> Per record, <see cref="IngestionOptions.DecodedLogRecordBytes"/>, plus the
/// record's own bytes on the wire, plus its resource's attribute bytes on the wire. About, and not
/// less in practice: a property is written to msgpack at about its protobuf length; the body, which
/// becomes the template, counts once, where an unpooled template is held as UTF-16 (the default
/// limit's slack absorbs that); OTLP/JSON spells a record out at two or three times its decoded
/// size, so JSON batches weigh heavier than they decode. Every record counts, whatever the parser
/// later makes of it.</para>
///
/// <para>Every field is skipped by its length, never decoded, and nothing is allocated. A message the
/// walk cannot frame throws as the parse would — the receivers answer it 400 with nothing ingested.</para>
/// </summary>
internal static class OtlpLogBatchWeight
{
    /// <summary>What a refusal says, over HTTP and gRPC. Printable ASCII, under 128 bytes.</summary>
    internal const string RefusalMessage = "the batch decodes to more log records and attributes than this server takes in one request; split it";

    /// <inheritdoc cref="RefusalMessage"/>
    internal static ReadOnlySpan<byte> RefusalMessageUtf8 => "the batch decodes to more log records and attributes than this server takes in one request; split it"u8;

    /// <summary>What a 503 for a full decoded budget says: the store is behind, the batch is to come again.</summary>
    internal const string BudgetFullMessage = "the server is holding as many decoded log batches as it allows; retry";

    /// <inheritdoc cref="BudgetFullMessage"/>
    internal static ReadOnlySpan<byte> BudgetFullMessageUtf8 => "the server is holding as many decoded log batches as it allows; retry"u8;

    /// <summary>Thrown by the gRPC decode for a batch over the limit: answered RESOURCE_EXHAUSTED.</summary>
    internal sealed class TooHeavyException(long weight, long limit)
        : Exception($"an OTLP logs batch decoding to about {weight} bytes is over the limit of {limit} bytes")
    {
        public long Weight { get; } = weight;
        public long Limit  { get; } = limit;
    }

    /// <summary>Thrown by the gRPC decode when the decoded budget is spent: answered UNAVAILABLE.</summary>
    internal sealed class BudgetFullException() : Exception(BudgetFullMessage);

    /// <summary>
    /// The weight of an <c>ExportLogsServiceRequest</c> in protobuf: resource_logs (1) → resource (1) →
    /// attributes (1), and resource_logs (1) → scope_logs (2) → log_records (2), in whatever order the
    /// fields come. A resource given twice counts both, as the parser keeps both.
    /// </summary>
    internal static long WeighProto(ReadOnlySpan<byte> payload)
    {
        long weight  = 0;
        var  request = new ProtoReader(payload);
        uint tag;
        while ((tag = request.ReadTag()) != 0)
        {
            if (tag != 10) { request.SkipField(tag); continue; }                  // resource_logs
            long resourceBytes = 0, records = 0, recordBytes = 0;
            var  block = new ProtoReader(request.ReadLengthDelimited());
            while ((tag = block.ReadTag()) != 0)
            {
                if (tag == 10)                                                     // resource
                {
                    var resource = new ProtoReader(block.ReadLengthDelimited());
                    while ((tag = resource.ReadTag()) != 0)
                    {
                        if (tag == 10) resourceBytes += resource.ReadLengthDelimited().Length;   // attributes
                        else resource.SkipField(tag);
                    }
                }
                else if (tag == 18)                                                // scope_logs
                {
                    var scope = new ProtoReader(block.ReadLengthDelimited());
                    while ((tag = scope.ReadTag()) != 0)
                    {
                        if (tag != 18) { scope.SkipField(tag); continue; }         // log_records
                        recordBytes += scope.ReadLengthDelimited().Length;
                        records++;
                    }
                }
                else block.SkipField(tag);
            }
            weight += records * (IngestionOptions.DecodedLogRecordBytes + resourceBytes) + recordBytes;
        }
        return weight;
    }

    /// <summary>
    /// The weight of an <c>ExportLogsServiceRequest</c> in OTLP/JSON, along the names the parser reads:
    /// <c>resourceLogs</c> → <c>resource</c> → <c>attributes</c>, and <c>resourceLogs</c> →
    /// <c>scopeLogs</c> → <c>logRecords</c>. A value's bytes are measured by skipping it. A malformed
    /// document throws as the parse would.
    /// </summary>
    internal static long WeighJson(ReadOnlySpan<byte> json)
    {
        var  reader = new Utf8JsonReader(json, isFinalBlock: true, state: default);
        long weight = 0;
        if (!reader.Read() || reader.TokenType != JsonTokenType.StartObject) return 0;
        while (reader.Read() && reader.TokenType == JsonTokenType.PropertyName)
        {
            bool blocks = reader.ValueTextEquals("resourceLogs"u8);
            reader.Read();
            if (blocks && reader.TokenType == JsonTokenType.StartArray)
            {
                while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
                {
                    if (reader.TokenType == JsonTokenType.StartObject) weight += WeighJsonResourceLogs(ref reader);
                    else reader.Skip();
                }
            }
            else reader.Skip();
        }
        return weight;
    }

    /// <summary>One <c>resourceLogs</c> element, the reader on its <c>StartObject</c> and left on its <c>EndObject</c>.</summary>
    private static long WeighJsonResourceLogs(ref Utf8JsonReader reader)
    {
        long resourceBytes = 0, records = 0, recordBytes = 0;
        while (reader.Read() && reader.TokenType == JsonTokenType.PropertyName)
        {
            bool resource = reader.ValueTextEquals("resource"u8);
            bool scopes   = !resource && reader.ValueTextEquals("scopeLogs"u8);
            reader.Read();
            if (resource && reader.TokenType == JsonTokenType.StartObject)
            {
                while (reader.Read() && reader.TokenType == JsonTokenType.PropertyName)
                {
                    bool attributes = reader.ValueTextEquals("attributes"u8);
                    reader.Read();
                    if (attributes) resourceBytes += SkipMeasured(ref reader);
                    else reader.Skip();
                }
            }
            else if (scopes && reader.TokenType == JsonTokenType.StartArray)
            {
                while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
                {
                    if (reader.TokenType != JsonTokenType.StartObject) { reader.Skip(); continue; }
                    while (reader.Read() && reader.TokenType == JsonTokenType.PropertyName)
                    {
                        bool logRecords = reader.ValueTextEquals("logRecords"u8);
                        reader.Read();
                        if (logRecords && reader.TokenType == JsonTokenType.StartArray)
                        {
                            while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
                            {
                                recordBytes += SkipMeasured(ref reader);
                                records++;
                            }
                        }
                        else reader.Skip();
                    }
                }
            }
            else reader.Skip();
        }
        return records * (IngestionOptions.DecodedLogRecordBytes + resourceBytes) + recordBytes;
    }

    /// <summary>The bytes of the value the reader is on, which it skips — left on its last token.</summary>
    private static long SkipMeasured(ref Utf8JsonReader reader)
    {
        long start = reader.TokenStartIndex;
        reader.Skip();
        return reader.BytesConsumed - start;
    }
}
