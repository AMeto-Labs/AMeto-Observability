using System.Buffers.Binary;
using System.IO.Compression;
using System.Runtime.InteropServices;
using Ameto.Core;

namespace Ameto.Otel;

/// <summary>Why a gzip body could not be turned into the message it wraps.</summary>
internal enum InflateResult
{
    Ok,
    /// <summary>Not gzip, or its trailer does not match what it inflated to.</summary>
    Malformed,
    /// <summary>It inflates past the batch limit.</summary>
    TooLarge,
}

/// <summary>
/// The one gzip inflate both OTLP receivers use: the gRPC one for a frame whose compression flag
/// is set, the HTTP one for a body sent with <c>Content-Encoding: gzip</c> — which is what the
/// collector's <c>otlphttp</c> exporter does by default.
///
/// <para>One copy rather than two, because what matters here is the ceiling, and the ceiling is
/// easy to get wrong: the review of the gRPC receiver (#57) blocked on an inflate that had none.
/// Deflate reaches about 1032:1, so a body small enough to pass the wire check inflates to
/// gigabytes, and the buffer doubling on the way there commits several times that. One valid
/// ingest key — the credential on every application that ships logs — was enough to take the
/// process down. A second, hand-copied inflate on the HTTP path would have been a second place
/// for that to come back.</para>
/// </summary>
internal static class OtlpGzip
{
    /// <summary>Smallest inflate buffer worth renting — below this the growth loop costs more than the slack.</summary>
    internal const int MinInflateBuffer = 64 * 1024;

    /// <summary>
    /// Inflates <paramref name="payload"/> into a buffer from <see cref="IngestBufferPool"/>,
    /// refusing the moment the output would pass <paramref name="maxInflatedBytes"/> — decided
    /// from bytes ALREADY written, so a bomb is stopped after at most one limit's worth of output
    /// rather than after it has been materialised and measured.
    ///
    /// <para>On <see cref="InflateResult.Ok"/> the CALLER owns <paramref name="rented"/> and returns
    /// it to <see cref="IngestBufferPool"/> in a finally; the message is its first
    /// <paramref name="length"/> bytes. On anything else <paramref name="rented"/> is null and
    /// nothing is left rented. Never throws: a stream zlib cannot read is
    /// <see cref="InflateResult.Malformed"/>, which the receivers answer as a client error.</para>
    /// </summary>
    /// <remarks>
    /// Takes the payload as <see cref="ReadOnlyMemory{T}"/>, not a span, for one reason:
    /// <c>GZipStream</c> needs a <c>Stream</c>, and a memory can be handed to a
    /// <c>MemoryStream</c> over the caller's own array. From a span the only route was
    /// <c>ToArray()</c> — a copy of the whole compressed payload, per request.
    /// </remarks>
    public static InflateResult Inflate(
        ReadOnlyMemory<byte> payload, int maxInflatedBytes, out byte[]? rented, out int length)
    {
        length = 0;

        // Sized from the message, not from the ceiling. Renting maxInflatedBytes meant every
        // compressed request — which for a collector is every request — took an 8 MiB array to
        // hold what is usually a few hundred KB, and made the pool keep 8 MiB arrays around for
        // it. The gzip trailer says how big the output will be; failing that, 4:1 is the guess.
        // Either way it grows by doubling if the guess was low, and the limit check below is
        // unchanged. A trailer that LIES high buys one rent of at most the limit — the clamp is
        // what keeps a four-byte claim from sizing anything past it.
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
                // Against the LIMIT, never against the buffer. The pool rounds a rent up to the
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
                        return InflateResult.TooLarge;
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

            length = total;
            return InflateResult.Ok;
        }
        catch
        {
            if (rented is not null) { IngestBufferPool.Return(rented); rented = null; }
            return InflateResult.Malformed;
        }
    }

    /// <summary>
    /// The uncompressed length a gzip member declares in its last four bytes (ISIZE, little
    /// endian, modulo 2^32). Exporters send exactly one member, so this is normally the exact
    /// answer and the buffer is rented once at the right size instead of doubling into it —
    /// which on a 20:1 batch is three rents and three copies of everything written so far.
    ///
    /// <para>A hint and nothing more: a truncated body, a multi-member stream or an outright lie
    /// costs one resize, never correctness, because the loop above measures what it actually
    /// wrote against the limit and never trusts this number for anything else.</para>
    /// </summary>
    internal static int InflatedSizeHint(ReadOnlySpan<byte> payload)
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
}
