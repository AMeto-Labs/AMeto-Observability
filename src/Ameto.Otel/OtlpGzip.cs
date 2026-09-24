using System.Buffers.Binary;
using System.IO.Compression;
using System.Runtime.InteropServices;
using Ameto.Core;

namespace Ameto.Otel;

/// <summary>Why a gzip body could not be turned into the message it wraps.</summary>
internal enum InflateResult
{
    Ok,
    /// <summary>Not gzip, cut off before its trailer, or a trailer that does not match what it inflated to.</summary>
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
    /// Deflate's ceiling: a 258-byte match coded in two bits is the most one input byte can
    /// buy, 4 x 258 = 1032 output bytes per input byte. No stream inflates further, so no
    /// honest ISIZE can exceed its payload's length times this.
    /// </summary>
    internal const int MaxDeflateRatio = 1032;

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
        // unchanged.
        //
        // The trailer is the CLIENT's claim, so it is believed only as far as the payload could
        // make it true: deflate cannot beat MaxDeflateRatio, so a body of N bytes inflates to at
        // most N x 1032 whatever its last four bytes say. Clamped to the limit alone, a 30-byte
        // body claiming 2 GiB rented a whole limit-sized buffer — 8 MiB, a fresh large-object
        // array whenever the pool was drained — for a request that could never fill 31 KB of it.
        int hint     = InflatedSizeHint(payload.Span);
        long ceiling = (long)payload.Length * MaxDeflateRatio;
        long want    = Math.Max(hint > 0 ? Math.Min(hint, ceiling) : (long)payload.Length * 4, MinInflateBuffer);
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
                    // Full — which is where an EXACT-FIT message ends, not only one that needs
                    // more: a trailer naming a bucket size, and every rent above
                    // IngestBufferPool.MaxPooledBytes (those are exact-length), fill the buffer
                    // to the byte. Growing first meant doubling and copying just to learn the
                    // stream was over — at a 16 MiB limit a 9 MiB message took 25 MiB of large-
                    // object arrays. One byte asks the same question for nothing.
                    int next = gzip.ReadByte();
                    if (next < 0) break;

                    int target   = (int)Math.Min((long)rented.Length * 2, maxInflatedBytes);
                    byte[] grown = IngestBufferPool.Rent(target);
                    rented.AsSpan(0, total).CopyTo(grown);
                    IngestBufferPool.Return(rented);
                    rented = grown;
                    rented[total++] = (byte)next;                    // total < limit here, so it fits
                    continue;                                        // back past the limit check
                }

                int read = gzip.Read(rented, total, room);
                if (read == 0) break;
                total += read;
            }

            if (!payload.IsEmpty && !TrailerFits(payload.Span, total))
            {
                IngestBufferPool.Return(rented);
                rented = null;
                return InflateResult.Malformed;
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

    /// <summary>A gzip member's fixed header (10 bytes) and trailer (CRC-32 and ISIZE, 8 bytes): nothing shorter is one.</summary>
    internal const int HeaderAndTrailerBytes = 18;

    /// <summary>
    /// Whether the stream could have ENDED where it did, rather than been cut off.
    ///
    /// <para><c>GZipStream</c> is strict about everything but one thing, and that one thing
    /// matters here: input that simply stops. zlib checks a member's CRC-32 and ISIZE only when
    /// it reaches the trailer, and a stream that runs out before then is reported as an ordinary
    /// end — so a body cut short mid-member inflated to a clean-looking PREFIX, and the parser
    /// ingested whatever records that prefix held. (The runtime's
    /// <c>System.IO.Compression.UseStrictValidation</c> switch would catch it, but it is
    /// process-wide and latched on the first <c>DeflateStream</c> anyone touches, so it cannot
    /// be relied on from here.)</para>
    ///
    /// <para>The trailer settles it instead. A complete stream ends with its last member's ISIZE,
    /// which can never exceed everything inflated: it EQUALS it for the single member every
    /// exporter sends (and zlib has already verified it did), and is smaller for a multi-member
    /// stream. A cut-off stream ends in deflate data or half a trailer, and those four bytes
    /// usually read as a length far past the output. USUALLY, not always: compressed data looks
    /// random, and then a cut slips past about (total + 1) / 2^32 of the time — fewer than one in
    /// 500 at the 8 MiB default — but a STORED block carries the message raw, and a cut that ends
    /// on four zero bytes (an empty field, the low half of a <c>1.0</c> double) reads as a size
    /// of 0 and passes. Then the parser sees a message cut short and refuses it as it would any
    /// malformed body. Zero padding after a member, which some tools add, reads as 0 and passes
    /// too — on purpose.</para>
    /// </summary>
    internal static bool TrailerFits(ReadOnlySpan<byte> payload, int inflated)
        => payload.Length >= HeaderAndTrailerBytes
        && BinaryPrimitives.ReadUInt32LittleEndian(payload[^4..]) <= (uint)inflated;

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

/// <summary>
/// How many gzip bodies may be inflated — and held inflated — at once, across BOTH OTLP receivers.
///
/// <para>Each inflate is bounded; nothing bounded how many ran together. A bomb needs ~8 KB of
/// deflate to fill the 8 MiB default and holds 8–12 MiB until it is refused, so ~30 concurrent
/// requests with any ingest key — some 360 KB uploaded — reached the 512 MB stand's 384 MB heap
/// limit. Nothing on the way stopped them: the ingest routes skip the rate limiter, the pool caps
/// only what it PARKS and allocates past that when empty, and Kestrel's connection and stream
/// limits are the defaults. An uncompressed body costs its sender every byte it makes the
/// server hold; only inflation multiplies, so only inflation is gated — identity bodies never
/// see this.</para>
///
/// <para><b>What a slot covers is the inflated BUFFER, not the inflate call.</b> The buffer is
/// what occupies the memory, and it lives until the parser behind it is done; releasing at the
/// end of the inflate would leave N highly compressible — valid — batches parsing at once with a
/// limit-sized buffer each. So a receiver takes a slot before it inflates and gives it back with
/// the inflated buffer.</para>
///
/// <para><b>How many:</b> <c>min(ProcessorCount, MemoryBudgets.IngestBufferBytes /
/// MaxOtlpBatchBytes)</c>, at least one. The budget term is the memory model's share for request
/// bodies, spent a limit-sized buffer at a time — 2 on the stand (0.05 x 384 MiB ≈ 20.1 MB over
/// 8 MiB), 16 where the 128 MiB cap applies; the core term because inflating and parsing are CPU
/// work, and more of them than cores only queue while holding their buffers.</para>
///
/// <para><b>Full</b> is a brief wait (<see cref="DefaultPatience"/>), then a refusal the receiver
/// answers as HTTP 503 with <c>Retry-After</c> or gRPC <c>UNAVAILABLE</c> — the two answers OTLP
/// exporters retry. The wait is asynchronous: a queued request holds its compressed body and its
/// connection, not a thread. Uncontended, entering is <see cref="SemaphoreSlim"/>'s fast path — a
/// cached completed task, no allocation.</para>
/// </summary>
internal sealed class OtlpInflateGate
{
    /// <summary>How long a compressed batch waits for a slot before it is told to retry.</summary>
    internal static readonly TimeSpan DefaultPatience = TimeSpan.FromSeconds(1);

    private readonly SemaphoreSlim _slots;
    private readonly TimeSpan      _patience;

    public OtlpInflateGate(int capacity, TimeSpan patience)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(capacity, 1);
        Capacity  = capacity;
        _patience = patience;
        _slots    = new SemaphoreSlim(capacity, capacity);
    }

    /// <summary>Slots in all.</summary>
    public int Capacity { get; }

    /// <summary>Slots free right now.</summary>
    public int Available => _slots.CurrentCount;

    /// <summary>The sizing rule, separated so it can be checked against the figures it is argued from.</summary>
    public static int CapacityFor(long ingestBufferBytes, int maxOtlpBatchBytes, int processorCount)
        => (int)Math.Clamp(Math.Min(processorCount, ingestBufferBytes / Math.Max(1L, maxOtlpBatchBytes)), 1L, int.MaxValue);

    /// <summary>
    /// The gate for this process. <see cref="IngestBufferPool.MaxPooledTotalBytes"/> IS
    /// <c>MemoryBudgets.Current().IngestBufferBytes</c>, read once at startup — the same figure,
    /// without a second pass over the GC's configuration.
    /// </summary>
    public static OtlpInflateGate For(int maxOtlpBatchBytes)
        => new(CapacityFor(IngestBufferPool.MaxPooledTotalBytes, maxOtlpBatchBytes, Environment.ProcessorCount),
               DefaultPatience);

    /// <summary>
    /// Takes a slot, waiting at most the gate's patience. False: none came free — the caller
    /// answers "retry" and must NOT call <see cref="Exit"/>. Throws only if
    /// <paramref name="ct"/> (the request's abort) fires.
    /// </summary>
    public Task<bool> TryEnterAsync(CancellationToken ct) => _slots.WaitAsync(_patience, ct);

    /// <summary>Gives back a slot <see cref="TryEnterAsync"/> granted — once, when the inflated buffer goes back.</summary>
    public void Exit() => _slots.Release();
}
