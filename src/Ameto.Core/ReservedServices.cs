namespace Ameto.Core;

/// <summary>
/// Service names the server writes under itself. What is filed under one of them is the
/// server's own account of something — the record of AI calls is the first — so nothing that
/// arrives over ingestion may carry it: an ingest key could otherwise write a record of a call
/// that was never made, or bury the real ones under invented ones.
///
/// <para>An event that arrives claiming a reserved name is kept, and filed under
/// <see cref="AiAuditIngested"/> instead: the sender loses nothing it sent, and the reserved name
/// stays the server's. The comparison ignores ASCII case, because that is how <c>@service = '…'</c>
/// compares.</para>
/// </summary>
public static class ReservedServices
{
    /// <summary>The service the record of AI calls is written under.</summary>
    public const string AiAudit = "ameto-mcp";

    /// <summary>What an ingested event that claimed <see cref="AiAudit"/> is filed under.</summary>
    public const string AiAuditIngested = "ingested:ameto-mcp";

    /// <summary>
    /// The name to file an ingested event under: <paramref name="serviceUtf8"/> itself, unless
    /// it is a reserved one. Allocation-free — one length test for every name that is not nine
    /// bytes long, which is nearly all of them.
    /// </summary>
    public static ReadOnlySpan<byte> ForIngestion(ReadOnlySpan<byte> serviceUtf8) =>
        serviceUtf8.Length == AiAudit.Length && System.Text.Ascii.EqualsIgnoreCase(serviceUtf8, "ameto-mcp"u8)
            ? "ingested:ameto-mcp"u8
            : serviceUtf8;
}
