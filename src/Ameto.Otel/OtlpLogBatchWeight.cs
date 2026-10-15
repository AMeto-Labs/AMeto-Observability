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
/// OTLP/JSON: per record, <see cref="IngestionOptions.DecodedLogRecordBytes"/> plus its resource's
/// attributes as they decode (their strings by their text, a number 9 B) — not the record's own text.
/// JSON spells a record out at two or three times what it decodes to, and weighed by its text a JSON
/// batch was refused at a fraction of the content a protobuf one carried; what it holds once decoded
/// is counted like any other. The resource is copied into every record, and that, as in protobuf, is
/// what a small body multiplies.</para>
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
    /// not what its text spells out (see the class doc) — plus, for each, its resource's attributes as
    /// the decode holds them (<see cref="JsonLeafBytes"/>), which it copies into every record, as the
    /// protobuf weight counts them (round-5 review L-12). Weighed at 128 B a record alone, a batch of
    /// 10 000 empty records under a 70 KB resource attribute was taken, and each record refused as
    /// oversized — 10 000 Error markers written for a 100 KB body — where protobuf answered 413. A
    /// malformed document throws as the parse would.
    /// </summary>
    internal static long WeighJson(ReadOnlySpan<byte> json)
    {
        var  reader  = new Utf8JsonReader(json, isFinalBlock: true, state: default);
        long weight  = 0;
        // An OTLP/JSON request is ONE object and nothing after it. Anything else is refused here, 400
        // with nothing ingested, as a document the reader cannot read is: the walk used to stop at a
        // first token that was not an object, or at the object's end, and '[', '[1,2' and '{}garbage'
        // were answered 200 (docs/API.md says a malformed JSON logs body ingests nothing).
        if (!reader.Read() || reader.TokenType != JsonTokenType.StartObject)
            throw new InvalidDataException("an OTLP/JSON request is a JSON object");
        while (reader.Read() && reader.TokenType == JsonTokenType.PropertyName)
        {
            bool blocks = reader.ValueTextEquals("resourceLogs"u8);
            reader.Read();
            if (blocks && reader.TokenType == JsonTokenType.StartArray)
            {
                while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
                {
                    if (reader.TokenType == JsonTokenType.StartObject) weight += WeighJsonBlock(ref reader);
                    else reader.Skip();
                }
            }
            else reader.Skip();
        }
        // On the object's end — and then the end of the body: a value after it throws in Read.
        if (reader.TokenType != JsonTokenType.EndObject || reader.Read())
            throw new InvalidDataException("an OTLP/JSON request is one JSON object");
        return weight;
    }

    /// <summary>
    /// One <c>resourceLogs</c> element's weight — its records, each with its resource's attributes — the
    /// reader on its <c>StartObject</c> and left on its <c>EndObject</c>. A resource given after its
    /// records, or twice, counts as the parser keeps it: in whatever order, both.
    /// </summary>
    private static long WeighJsonBlock(ref Utf8JsonReader reader)
    {
        long records = 0, resourceBytes = 0;
        while (reader.Read() && reader.TokenType == JsonTokenType.PropertyName)
        {
            if (reader.ValueTextEquals("resource"u8))
            {
                reader.Read();
                if (reader.TokenType != JsonTokenType.StartObject) { reader.Skip(); continue; }
                while (reader.Read() && reader.TokenType == JsonTokenType.PropertyName)
                {
                    bool attributes = reader.ValueTextEquals("attributes"u8);
                    reader.Read();
                    if (attributes) resourceBytes += JsonLeafBytes(ref reader);
                    else reader.Skip();
                }
                continue;
            }
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
        return records * (IngestionOptions.DecodedLogRecordBytes + resourceBytes);
    }

    /// <summary>
    /// What a JSON value's data comes to, about as the decode writes it into a record's map: each string
    /// — a key or a value — by the bytes of its text (escapes only shrink it), a number 9 B, a boolean or
    /// a null 1 B; the OTLP names around them (<c>key</c>, <c>value</c>, <c>stringValue</c>) are structure,
    /// not data. The reader on the value's first token, and left on its last.
    /// </summary>
    private static long JsonLeafBytes(ref Utf8JsonReader reader)
    {
        if (reader.TokenType is not (JsonTokenType.StartObject or JsonTokenType.StartArray)) return LeafBytes(ref reader);
        long bytes = 0;
        int  depth = reader.CurrentDepth;
        while (reader.Read() && reader.CurrentDepth > depth)
            if (reader.TokenType != JsonTokenType.PropertyName) bytes += LeafBytes(ref reader);
        return bytes;
    }

    private static long LeafBytes(ref Utf8JsonReader reader) => reader.TokenType switch
    {
        JsonTokenType.String => reader.ValueSpan.Length,
        JsonTokenType.Number => 9,
        JsonTokenType.True or JsonTokenType.False or JsonTokenType.Null => 1,
        _ => 0,
    };
}
