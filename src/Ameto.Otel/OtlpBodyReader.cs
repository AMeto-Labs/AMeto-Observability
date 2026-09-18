using Microsoft.AspNetCore.Http;
using Ameto.Core;

namespace Ameto.Otel;

/// <summary>
/// The request-body read both OTLP receivers do: rent from <see cref="IngestBufferPool"/>, fill,
/// and refuse anything past <c>Ingestion.MaxOtlpBatchBytes</c>.
///
/// <para>One copy of it rather than two, which is how it comes to be here: the HTTP receiver and
/// the gRPC one carried the same twenty lines each, and so carried the same defect. Both grew a
/// full buffer by doubling with no reference to the ceiling, so a body one byte over the default
/// 8 MiB limit rented 16 MiB — past <see cref="IngestBufferPool.MaxPooledBytes"/>, therefore a
/// fresh large-object array rather than a pooled one — copied a megabyte-scale prefix into it,
/// and dropped it again with the refusal. On the 512 MB console stand that was a 16 MiB LOH
/// allocation per abusive or misconfigured request, on the one component whose job is to survive
/// them.</para>
///
/// <para>What the receivers genuinely differ in is how they say no — an HTTP 413 against a gRPC
/// status in trailers — and that stays with each of them. How they read does not.</para>
///
/// <para>Growth here follows <c>OtlpGrpcFraming.Inflate</c>, the third reader of the same
/// ceiling, which already had this right: never grow past the limit, and when the buffer reaches
/// it, one byte — not one doubling — settles whether the body has more to give.</para>
/// </summary>
internal static class OtlpBodyReader
{
    /// <summary>
    /// Starting capacity when the request declares no length. HTTP/2 rarely declares one, so the
    /// usual gRPC path is grow-by-doubling from here rather than the exact-size rent a declared
    /// body gets.
    /// </summary>
    internal const int UndeclaredCapacityBytes = 65_536;

    /// <summary>Floor on the first rent, so a tiny declared length still gets a usable buffer.</summary>
    private const int MinCapacityBytes = 256;

    /// <summary>
    /// Reads the whole request body into a buffer from <see cref="IngestBufferPool"/>.
    ///
    /// <para>Returns <c>(null, 0)</c> — with nothing left rented — when the body is over
    /// <paramref name="maxBytes"/>, whether it said so in Content-Length or proved it by
    /// arriving. The caller turns that into the refusal its protocol speaks.</para>
    ///
    /// <para>On success the CALLER owns the buffer and must give it back with
    /// <see cref="IngestBufferPool.Return"/>, in a finally. A read that throws — a reset stream,
    /// a client deadline, a collector timing out mid-upload, all everyday events on an ingest
    /// port — returns the buffer here before the exception leaves: dropping it is not a leak, it
    /// is a permanent withdrawal from the pool every receiver shares.</para>
    /// </summary>
    internal static async ValueTask<(byte[]? Buffer, int Length)> ReadAsync(HttpContext ctx, int maxBytes)
    {
        long? declared = ctx.Request.ContentLength;
        if (declared > maxBytes) return (null, 0);

        // A declared length is an exact-size rent and no growth at all; only an undeclared body
        // walks up through the buckets.
        int initial = declared.HasValue ? (int)declared.Value : UndeclaredCapacityBytes;
        byte[] buf  = IngestBufferPool.Rent(Math.Max(initial, MinCapacityBytes));
        int total   = 0;
        try
        {
            while (true)
            {
                if (total == buf.Length)
                {
                    // Full AND at the ceiling. There is nothing left to HOLD — a body that fits
                    // is already whole — only something left to KNOW: whether one more byte
                    // exists. That costs one byte, not one doubling. Growing here is what made
                    // an over-limit body cost 16 MiB: above IngestBufferPool.MaxPooledBytes a
                    // rent is a fresh large-object allocation, and this one's entire life would
                    // be to be copied into and dropped with the 413. The single-byte array is a
                    // gen0 object, on a request that is either at its limit or already refused.
                    //
                    // `>=` rather than `==`: the limit check below refuses at maxBytes + 1, so
                    // `total` cannot pass the ceiling here — but a buffer that started larger
                    // than the ceiling (a pooled rent rounds UP to a power of two) must refuse
                    // rather than ask for a bucket smaller than the bytes it is already holding.
                    if (total >= maxBytes)
                    {
                        if (await ctx.Request.Body.ReadAsync(new byte[1], ctx.RequestAborted) == 0)
                            break;                              // exactly at the ceiling, and complete

                        IngestBufferPool.Return(buf);
                        return (null, 0);
                    }

                    byte[] larger = IngestBufferPool.Rent(GrowTarget(buf.Length, maxBytes));
                    buf.AsSpan(0, total).CopyTo(larger);
                    IngestBufferPool.Return(buf);
                    buf = larger;
                }

                int read = await ctx.Request.Body.ReadAsync(buf.AsMemory(total), ctx.RequestAborted);
                if (read == 0) break;
                total += read;

                if (total > maxBytes)
                {
                    IngestBufferPool.Return(buf);
                    return (null, 0);
                }
            }

            return (buf, total);
        }
        catch
        {
            IngestBufferPool.Return(buf);
            throw;
        }
    }

    /// <summary>
    /// What a full buffer grows to: double it, but never past one byte beyond the ceiling the
    /// body is about to be measured against — a buffer larger than that can only ever hold bytes
    /// that are going to be refused.
    ///
    /// <para>The clamp is what a ceiling that is NOT a power of two needs: at a 10 MB limit the
    /// 8 MiB buffer doubled to 16 MiB, where 10,000,001 is both enough and 6.7 MiB cheaper. At
    /// the power-of-two default the pool's own bucket rounding would undo it, which is why the
    /// caller settles the body-at-the-ceiling case with a one-byte read instead.</para>
    ///
    /// <para>Never returns less than <paramref name="current"/> + 1: the read that follows a
    /// grow must have somewhere to go, or a full buffer would look like the end of the body.</para>
    /// </summary>
    internal static int GrowTarget(int current, int maxBytes)
    {
        long doubled = Math.Min((long)current * 2, (long)maxBytes + 1);
        return (int)Math.Min(Math.Max(doubled, (long)current + 1), Array.MaxLength);
    }
}
