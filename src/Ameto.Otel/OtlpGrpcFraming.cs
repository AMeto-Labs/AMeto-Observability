using System.Buffers;
using System.Buffers.Binary;
using System.IO.Compression;
using System.Runtime.InteropServices;
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

        return Inflate(body.Slice(HeaderBytes, (int)declared), maxInflatedBytes,
                       out message, out rented, out rentedLength);
    }

    /// <summary>
    /// Inflates into a pooled buffer, refusing the moment the output would pass the limit —
    /// which is the point: the decision is made from bytes ALREADY written, so a bomb is stopped
    /// after one buffer's worth rather than after it has been materialised and measured.
    /// </summary>
    private static UnframeResult Inflate(
        ReadOnlyMemory<byte> payload, int maxInflatedBytes,
        out ReadOnlySpan<byte> message, out byte[]? rented, out int rentedLength)
    {
        message      = default;
        rentedLength = 0;

        // Sized from the message, not from the ceiling. Renting maxInflatedBytes meant every
        // compressed request — which for a collector's gRPC exporter is every request — took an
        // 8 MiB array to hold what is usually a few hundred KB, and made the pool keep 8 MiB
        // arrays around for it. The gzip trailer says how big the output will be; failing that,
        // 4:1 is the guess. Either way it grows by doubling if the guess was low, and the limit
        // check below is unchanged.
        int hint     = InflatedSizeHint(payload.Span);
        long want    = Math.Max(hint > 0 ? hint : (long)payload.Length * 4, MinInflateBuffer);
        int capacity = (int)Math.Min(want, Math.Max(maxInflatedBytes, 1));
        rented       = IngestBufferPool.Rent(capacity);

        try
        {
            // No copy of the compressed bytes: the payload is a window onto the caller's request
            // buffer, and MemoryStream can wrap that array where it lies.
            using var input = AsStream(payload);
            using var gzip  = new GZipStream(input, CompressionMode.Decompress);

            int total = 0;
            while (true)
            {
                // Against the LIMIT, never against the buffer. ArrayPool rounds a rent up to the
                // next power of two, so Rent(10_000_000) hands back 16 MiB — measuring against
                // that would silently raise every non-power-of-two limit to nearly double what
                // the operator configured, and only the 8 MiB default would mean what it says.
                if (total == maxInflatedBytes)
                {
                    // Full, and the stream still has bytes: over the limit. One more byte rather
                    // than growing, so nothing past the cap is ever committed.
                    if (gzip.ReadByte() >= 0)
                    {
                        IngestBufferPool.Return(rented);
                        rented = null;
                        return UnframeResult.TooLarge;
                    }
                    break;
                }

                // Room left in THIS buffer, which is what runs out first now. Doubling, never
                // past the limit, so the "one more byte" test above remains the only thing that
                // decides whether a message is too large.
                int room = Math.Min(rented.Length, maxInflatedBytes) - total;
                if (room == 0)
                {
                    int target   = (int)Math.Min((long)rented.Length * 2, maxInflatedBytes);
                    byte[] grown = IngestBufferPool.Rent(target);
                    rented.AsSpan(0, total).CopyTo(grown);
                    IngestBufferPool.Return(rented);
                    rented = grown;
                    room   = Math.Min(rented.Length, maxInflatedBytes) - total;
                }

                int read = gzip.Read(rented, total, room);
                if (read == 0) break;
                total += read;
            }

            rentedLength = total;
            message      = rented.AsSpan(0, total);
            return UnframeResult.Ok;
        }
        catch
        {
            if (rented is not null) { IngestBufferPool.Return(rented); rented = null; }
            return UnframeResult.Malformed;
        }
    }

    /// <summary>Smallest inflate buffer worth renting — below this the growth loop costs more than the slack.</summary>
    private const int MinInflateBuffer = 64 * 1024;

    /// <summary>
    /// The uncompressed length a gzip member declares in its last four bytes (ISIZE, little
    /// endian, modulo 2^32). Exporters send exactly one member, so this is normally the exact
    /// answer and the buffer is rented once at the right size instead of doubling into it —
    /// which on a 20:1 batch is three rents and three copies of everything written so far.
    ///
    /// <para>A hint and nothing more: a truncated frame, a multi-member stream or an outright
    /// lie costs one resize, never correctness, because the loop below measures what it
    /// actually wrote against the limit and never trusts this number for anything else.</para>
    /// </summary>
    private static int InflatedSizeHint(ReadOnlySpan<byte> payload)
    {
        if (payload.Length < 4) return 0;
        uint isize = BinaryPrimitives.ReadUInt32LittleEndian(payload[^4..]);
        return isize <= int.MaxValue ? (int)isize : 0;
    }

    /// <summary>
    /// A stream over the payload without copying it, when the memory is array-backed — which it
    /// always is here, since it is a slice of a pooled request buffer. The fallback exists so a
    /// caller passing some other memory still works rather than throwing.
    /// </summary>
    private static Stream AsStream(ReadOnlyMemory<byte> payload)
        => MemoryMarshal.TryGetArray(payload, out ArraySegment<byte> seg) && seg.Array is not null
               ? new MemoryStream(seg.Array, seg.Offset, seg.Count, writable: false)
               : new MemoryStream(payload.ToArray(), writable: false);

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
