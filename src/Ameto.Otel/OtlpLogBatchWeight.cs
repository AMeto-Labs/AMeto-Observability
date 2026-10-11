using System.Text.Json;
using Ameto.Core;

namespace Ameto.Otel;

/// <summary>
/// What an OTLP logs batch weighs before a record is staged: the receivers refuse at once a batch
/// whose weight already passes <c>Ingestion.MaxDecodedLogBatchBytes</c> — 413 (gRPC: RESOURCE_EXHAUSTED),
/// nothing of it ingested — and reserve the weight of one they take as its first reservation against
/// what the batches held decoded at once may hold (<c>IngestionEndpoint.TryBeginBatch</c>).
///
/// <para><b>Not the bound.</b> The decode counts what it actually takes and stops the moment the
/// batch passes the limit (<c>LogIngestBatch</c>). This weight is an estimate, and a body built to
/// beat it beat it: a resource attribute of bytes that are not UTF-8 is stored as U+FFFD, three
/// bytes for each, in every record under it — 25 KB on the wire decoded to ~100 MB staged, weighed
/// at 33 MB. It stays as the cheap early refusal of a body that is plainly too heavy.</para>
///
/// <para><b>The weight.</b> Protobuf: per record, <see cref="IngestionOptions.DecodedLogRecordBytes"/>,
/// plus the record's own bytes on the wire, plus its resource's attribute bytes on the wire.
/// OTLP/JSON: per record, <see cref="IngestionOptions.DecodedLogRecordBytes"/> alone. JSON spells a
/// record out at two or three times what it decodes to, and weighed by its text a JSON batch was
/// refused at a fraction of the content a protobuf one carried; what it holds once decoded is
/// counted like any other.</para>
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
    /// The weight of an <c>ExportLogsServiceRequest</c> in OTLP/JSON: its records, along the names
    /// the parser reads (<c>resourceLogs</c> → <c>scopeLogs</c> → <c>logRecords</c>), at
    /// <see cref="IngestionOptions.DecodedLogRecordBytes"/> each — what a record costs at the least,
    /// not what its text spells out (see the class doc). A malformed document throws as the parse
    /// would.
    /// </summary>
    internal static long WeighJson(ReadOnlySpan<byte> json)
    {
        var  reader  = new Utf8JsonReader(json, isFinalBlock: true, state: default);
        long records = 0;
        if (!reader.Read() || reader.TokenType != JsonTokenType.StartObject) return 0;
        while (reader.Read() && reader.TokenType == JsonTokenType.PropertyName)
        {
            bool blocks = reader.ValueTextEquals("resourceLogs"u8);
            reader.Read();
            if (blocks && reader.TokenType == JsonTokenType.StartArray)
            {
                while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
                {
                    if (reader.TokenType == JsonTokenType.StartObject) records += CountJsonRecords(ref reader);
                    else reader.Skip();
                }
            }
            else reader.Skip();
        }
        return records * IngestionOptions.DecodedLogRecordBytes;
    }

    /// <summary>The records of one <c>resourceLogs</c> element, the reader on its <c>StartObject</c> and left on its <c>EndObject</c>.</summary>
    private static long CountJsonRecords(ref Utf8JsonReader reader)
    {
        long records = 0;
        while (reader.Read() && reader.TokenType == JsonTokenType.PropertyName)
        {
            bool scopes = reader.ValueTextEquals("scopeLogs"u8);
            reader.Read();
            if (scopes && reader.TokenType == JsonTokenType.StartArray)
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
                                reader.Skip();
                                records++;
                            }
                        }
                        else reader.Skip();
                    }
                }
            }
            else reader.Skip();
        }
        return records;
    }
}
