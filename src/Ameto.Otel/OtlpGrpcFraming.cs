using System.Buffers;
using System.Buffers.Binary;
using Ameto.Core;

namespace Ameto.Otel;

/// <summary>Why a frame could not be turned into a message.</summary>
internal enum UnframeResult
{
    Ok,
    /// <summary>Too short, or a length that overruns the body.</summary>
    Malformed,
    /// <summary>Compressed with something other than gzip.</summary>
    UnsupportedEncoding,
    /// <summary>The message inflates past the batch limit.</summary>
    TooLarge,
    /// <summary>The server ran out of memory inflating it: retryable, not the client's fault.</summary>
    Unavailable,
}

/// <summary>
/// The little that separates a gRPC unary call from an ordinary protobuf POST.
///
/// <para>An OTLP <c>Export</c> is a plain unary method: one request message in, one response
/// message out. The body is the SAME protobuf this server already decodes by hand, wrapped in
/// gRPC's length-prefixed framing — one byte saying whether the message is compressed, four
/// big-endian bytes of length, then the message. That is the whole difference, which is why this
/// file exists instead of a dependency on Grpc.AspNetCore: pulling in the generated marshalling
/// would put an allocating object graph on the hottest path in the system, and the
/// zero-allocation decoders it would replace are already written and already tested.</para>
/// </summary>
internal static class OtlpGrpcFraming
{
    /// <summary>Compression flag + 4-byte big-endian length.</summary>
    public const int HeaderBytes = 5;

    /// <summary>
    /// Unwraps the single message of a unary request.
    /// </summary>
    /// <param name="maxInflatedBytes">
    /// Ceiling on the DECOMPRESSED message. Without it a batch limit means nothing: deflate
    /// reaches about 1032:1, so a frame small enough to pass the wire check inflates to
    /// gigabytes, and the buffer doubling on the way there commits several times that. One valid
    /// ingest key — the credential on every application that ships logs — would be enough to
    /// take the process down, and repeatably, because the failure was swallowed and left no
    /// trace to alert on.
    /// </param>
    /// <param name="rented">
    /// Non-null only when the message had to be decompressed into a pooled buffer, which the
    /// caller then returns to <see cref="IngestBufferPool"/>. When null,
    /// <paramref name="message"/> points into the caller's own buffer and nothing extra was
    /// allocated.
    /// </param>
    /// <remarks>
    /// Takes the body as <see cref="ReadOnlyMemory{T}"/>, not a span, for one reason:
    /// <c>GZipStream</c> needs a <c>Stream</c>, and a memory can be handed to a
    /// <c>MemoryStream</c> over the caller's own array. From a span the only route was
    /// <c>ToArray()</c> — a copy of the whole compressed payload, per request.
    /// </remarks>
    public static UnframeResult TryUnframe(
        ReadOnlyMemory<byte> body, string? encoding, int maxInflatedBytes,
        out ReadOnlySpan<byte> message, out byte[]? rented, out int rentedLength)
    {
        message      = default;
        rented       = null;
        rentedLength = 0;

        var span = body.Span;
        if (span.Length < HeaderBytes) return UnframeResult.Malformed;

        bool compressed = span[0] != 0;
        uint declared   = BinaryPrimitives.ReadUInt32BigEndian(span[1..]);
        // A length that overruns the body is a truncated or hostile frame, and unsigned overflow
        // would turn it into a slice of someone else's memory.
        if (declared > (uint)(span.Length - HeaderBytes)) return UnframeResult.Malformed;

        if (!compressed)
        {
            // The flag decides, not the header: `grpc-encoding` only names HOW a compressed
            // message was compressed, so an identity frame is fine whatever it says.
            message = span.Slice(HeaderBytes, (int)declared);
            return UnframeResult.Ok;
        }

        // gzip is the only compression OTLP exporters send in practice; anything else is refused
        // so the caller can answer UNIMPLEMENTED with grpc-accept-encoding, which is what makes a
        // client retry uncompressed instead of failing the batch.
        if (!string.Equals(encoding, "gzip", StringComparison.OrdinalIgnoreCase))
            return UnframeResult.UnsupportedEncoding;

        // The inflate is OtlpGzip's, shared with the HTTP receivers' Content-Encoding: gzip road:
        // one copy of the rule that the ceiling is decided on bytes already written.
        var inflated = OtlpGzip.Inflate(body.Slice(HeaderBytes, (int)declared), maxInflatedBytes,
                                        out rented, out rentedLength);
        if (inflated != InflateResult.Ok)
            return inflated switch
            {
                InflateResult.TooLarge    => UnframeResult.TooLarge,
                InflateResult.Unavailable => UnframeResult.Unavailable,
                _                         => UnframeResult.Malformed,
            };

        message = rented.AsSpan(0, rentedLength);
        return UnframeResult.Ok;
    }

    /// <summary>
    /// Whether <see cref="TryUnframe"/> would INFLATE this body: its compression flag is set and
    /// it names gzip. What decides whether the receiver waits for an <see cref="OtlpInflateGate"/>
    /// slot — an identity frame never does, and one in a coding this server refuses must get its
    /// UNIMPLEMENTED at once, not UNAVAILABLE after a wait for a slot it would never use.
    /// </summary>
    public static bool WillInflate(ReadOnlySpan<byte> body, string? encoding)
        => body.Length >= HeaderBytes && body[0] != 0
        && string.Equals(encoding, "gzip", StringComparison.OrdinalIgnoreCase);

    /// <summary>Wraps a response message in an uncompressed frame.</summary>
    public static byte[] Frame(ReadOnlySpan<byte> message)
    {
        var framed = new byte[HeaderBytes + message.Length];
        framed[0]  = 0;                                                   // identity
        BinaryPrimitives.WriteUInt32BigEndian(framed.AsSpan(1), (uint)message.Length);
        message.CopyTo(framed.AsSpan(HeaderBytes));
        return framed;
    }

    /// <summary>
    /// The bytes of an <c>Export…ServiceResponse</c>. Empty means "everything accepted"; when
    /// records were rejected the response carries a <c>partial_success</c> submessage, because a
    /// client that is dropping data deserves to be told rather than to read a success.
    ///
    /// <para>Hand-encoded, matching the .proto:
    /// <c>ExportLogsServiceResponse{ partial_success = 1 }</c> and
    /// <c>ExportLogsPartialSuccess{ rejected = 1 (varint), error_message = 2 (string) }</c> —
    /// the field numbers are the same for logs, traces and metrics.</para>
    /// </summary>
    public static byte[] ExportResponse(long rejected, string? message = null)
    {
        if (rejected <= 0) return [];

        Span<byte> inner = stackalloc byte[32];
        int n = 0;
        inner[n++] = 0x08;                                                // field 1, varint
        n += WriteVarint(inner[n..], (ulong)rejected);

        byte[]? msgBytes = string.IsNullOrEmpty(message) ? null : System.Text.Encoding.UTF8.GetBytes(message);
        int innerLen = n + (msgBytes is null ? 0 : 1 + VarintSize((ulong)msgBytes.Length) + msgBytes.Length);

        var outer = new byte[1 + VarintSize((ulong)innerLen) + innerLen];
        int o = 0;
        outer[o++] = 0x0A;                                                // field 1, length-delimited
        o += WriteVarint(outer.AsSpan(o), (ulong)innerLen);
        inner[..n].CopyTo(outer.AsSpan(o));
        o += n;
        if (msgBytes is not null)
        {
            outer[o++] = 0x12;                                            // field 2, length-delimited
            o += WriteVarint(outer.AsSpan(o), (ulong)msgBytes.Length);
            msgBytes.CopyTo(outer.AsSpan(o));
        }
        return outer;
    }

    private static int WriteVarint(Span<byte> dest, ulong value)
    {
        int i = 0;
        while (value >= 0x80) { dest[i++] = (byte)(value | 0x80); value >>= 7; }
        dest[i++] = (byte)value;
        return i;
    }

    private static int VarintSize(ulong value)
    {
        int n = 1;
        while (value >= 0x80) { value >>= 7; n++; }
        return n;
    }
}
