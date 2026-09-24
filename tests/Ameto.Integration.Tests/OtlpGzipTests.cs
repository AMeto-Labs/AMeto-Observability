using System.Buffers.Binary;
using System.IO.Compression;
using System.Numerics;
using Ameto.Core;
using Ameto.Otel;
using Xunit.Abstractions;

namespace Ameto.Integration.Tests;

/// <summary>
/// <see cref="OtlpGzip.Inflate"/>, the one gzip inflate both OTLP receivers call — gRPC for a
/// compressed frame, HTTP for <c>Content-Encoding: gzip</c> — driven directly, without a host.
///
/// <para>Two things are pinned here that a host cannot pin precisely. That a stream CUT OFF
/// before its trailer is refused: <c>GZipStream</c> reports running out of input as an ordinary
/// end, so a truncated body used to inflate to a clean-looking prefix and have its records
/// ingested. And what a bomb costs on the thread that inflates it, measured with
/// <see cref="GC.GetAllocatedBytesForCurrentThread"/> — the inflate is synchronous, so every
/// byte it allocates is on this thread and nothing else is.</para>
/// </summary>
public sealed class OtlpGzipTests
{
    /// <summary>A power of two, so the pool's bucket for a limit-sized rent IS the limit.</summary>
    private const int Limit = 1024 * 1024;

    /// <summary>
    /// What an inflate may allocate beyond its output buffers: the <c>GZipStream</c>, its
    /// <c>DeflateStream</c> and inflater state, and the <c>MemoryStream</c> over the payload —
    /// a few hundred bytes steady-state, with room here for a first use on this thread.
    /// </summary>
    private const long Slack = 64 * 1024;

    private readonly ITestOutputHelper _out;
    public OtlpGzipTests(ITestOutputHelper output) => _out = output;

    private static InflateResult Inflate(byte[] payload, out byte[] message, int limit = Limit)
    {
        var result = OtlpGzip.Inflate(payload, limit, out byte[]? rented, out int length);
        if (result != InflateResult.Ok) Assert.Null(rented);            // nothing left rented on a refusal
        message = rented is null ? [] : rented.AsSpan(0, length).ToArray();
        if (rented is not null) IngestBufferPool.Return(rented);
        return result;
    }

    // ── A stream that stops before it ends ────────────────────────────────────

    /// <summary>
    /// THE ONE THE TRAILER CHECK EXISTS FOR. Every one of these inflated WITHOUT ERROR before it:
    /// zlib checks CRC-32 and ISIZE only when it reaches them, and a stream that runs out first is
    /// an ordinary end to <c>GZipStream</c> — so a body cut anywhere came back as the prefix it
    /// held (half of this one: 127 856 of 300 000 bytes), and the parser behind it ingested
    /// every record in that prefix before failing, or, cut on a record boundary, answered 200.
    /// </summary>
    [Theory]
    [InlineData("the last byte of ISIZE")]
    [InlineData("all of ISIZE")]
    [InlineData("the whole trailer")]
    [InlineData("the trailer and a byte of deflate data")]
    [InlineData("half the stream")]
    [InlineData("everything after the header")]
    public void A_stream_cut_off_before_its_trailer_is_malformed_not_a_shorter_message(string cut)
    {
        byte[] whole = Gzip(Pattern(300_000));
        byte[] body  = cut switch
        {
            "the last byte of ISIZE"                 => whole[..^1],
            "all of ISIZE"                           => whole[..^4],
            "the whole trailer"                      => whole[..^8],
            "the trailer and a byte of deflate data" => whole[..^9],
            "half the stream"                        => whole[..(whole.Length / 2)],
            "everything after the header"            => whole[..10],
            _ => throw new ArgumentOutOfRangeException(nameof(cut)),
        };

        Assert.Equal(InflateResult.Malformed, Inflate(body, out _));
    }

    [Fact]
    public void A_whole_stream_inflates_to_exactly_what_was_compressed()
    {
        // The control for the theory above: the same stream, uncut.
        byte[] data = Pattern(300_000);
        Assert.Equal(InflateResult.Ok, Inflate(Gzip(data), out var message));
        Assert.Equal(data, message);
    }

    [Fact]
    public void Two_members_still_inflate_whole_though_the_trailer_names_only_the_last()
    {
        // The case the check must NOT refuse: a multi-member stream's last four bytes are its
        // LAST member's size, smaller than the total. Nothing an exporter sends, but legal gzip,
        // and the gRPC receiver has accepted it since the inflate was written.
        byte[] first = Pattern(300_000, seed: 1), second = Pattern(200_000, seed: 2);
        Assert.Equal(InflateResult.Ok, Inflate([.. Gzip(first), .. Gzip(second)], out var message));
        Assert.Equal([.. first, .. second], message);
    }

    [Fact]
    public void Zero_padding_after_the_stream_is_still_the_stream()
    {
        // Some writers pad to a block; four zero bytes read as an ISIZE of 0, which fits anything.
        byte[] data = Pattern(10_000);
        Assert.Equal(InflateResult.Ok, Inflate([.. Gzip(data), 0, 0, 0, 0, 0, 0, 0, 0], out var message));
        Assert.Equal(data, message);
    }

    [Fact]
    public void Bytes_after_the_stream_that_cannot_be_its_end_are_refused()
    {
        // GZipStream skips what follows a member when it is not another member. As the last four
        // bytes of the body, 0xFFFFFFFF claims a 4 GiB member that nothing here inflated.
        Assert.Equal(InflateResult.Malformed, Inflate([.. Gzip(Pattern(10_000)), 0xFF, 0xFF, 0xFF, 0xFF], out _));
    }

    [Fact]
    public void An_empty_payload_is_an_empty_message()
    {
        // What the gRPC receiver answered a compressed empty frame before the trailer check, and
        // still does: nothing is not a truncated something.
        Assert.Equal(InflateResult.Ok, Inflate([], out var message));
        Assert.Empty(message);
    }

    // ── The trailer as a size hint ────────────────────────────────────────────

    [Fact]
    public void A_trailer_that_lies_high_sizes_nothing_past_the_limit_and_is_refused()
    {
        // ISIZE is where the first rent's size comes from, so a four-byte claim of 2 GiB must buy
        // at most one limit-sized buffer — and zlib, which DOES check ISIZE when it reaches it,
        // then refuses the stream.
        byte[] body = Gzip(Pattern(10_000));
        BinaryPrimitives.WriteUInt32LittleEndian(body.AsSpan(body.Length - 4), int.MaxValue);

        using var ledger = IngestBufferPoolLedger.Open();
        Assert.Equal(InflateResult.Malformed, Inflate(body, out _));

        Assert.True(ledger.LargestRent <= Limit,
            $"a trailer claiming {int.MaxValue:N0} bytes had the inflate rent {ledger.LargestRent:N0} — past the {Limit:N0} limit");
        ledger.AssertEveryBufferCameBackOnce(minRents: 1);
    }

    [Fact]
    public void A_tiny_body_claiming_gigabytes_rents_only_what_it_could_inflate_to()
    {
        // The amplification the limit clamp alone allowed: ~30 bytes on the wire, a 2 GiB claim
        // in the trailer, and a whole limit-sized buffer rented for it — 8 MiB at the default, a
        // fresh large-object array whenever the pool is drained, per request, for anyone holding
        // an ingest key. Deflate cannot beat 1032:1, so that is all the claim is believed for.
        byte[] body = Gzip("tiny"u8);
        BinaryPrimitives.WriteUInt32LittleEndian(body.AsSpan(body.Length - 4), int.MaxValue);

        using var ledger = IngestBufferPoolLedger.Open();
        Assert.Equal(InflateResult.Malformed, Inflate(body, out _));

        long believable = Math.Max((long)body.Length * OtlpGzip.MaxDeflateRatio, OtlpGzip.MinInflateBuffer);
        long bucket     = (long)BitOperations.RoundUpToPowerOf2((ulong)believable);
        _out.WriteLine($"{body.Length} B body claiming {int.MaxValue:N0} B: largest rent {ledger.LargestRent:N0} B "
                     + $"(believable {believable:N0} B, limit {Limit:N0} B)");
        Assert.True(ledger.LargestRent <= bucket,
            $"a {body.Length}-byte body rented {ledger.LargestRent:N0} B; it cannot inflate past {believable:N0} B");
        Assert.True(ledger.LargestRent < Limit, "the trailer's claim still sized a whole limit-sized buffer");
        ledger.AssertEveryBufferCameBackOnce(minRents: 1);
    }

    /// <summary>
    /// A message that fills its first buffer to the byte is WHOLE, and costs that one buffer.
    /// It is the common shape, not a corner: the trailer is exact, so a message whose size is a
    /// pool bucket fits exactly, and above <see cref="IngestBufferPool.MaxPooledBytes"/> every
    /// rent is exact-length. The loop used to grow and copy before it learned the stream was
    /// over — a second, doubled buffer each time, 16 MiB of large-object array for the 9 MiB
    /// row below.
    /// </summary>
    [Theory]
    [InlineData(256 * 1024,      Limit)]                 // the 256 KiB bucket, exactly
    [InlineData(9 * 1024 * 1024, 16 * 1024 * 1024)]      // an exact unpooled rent under a raised limit
    public void A_message_that_fits_its_buffer_exactly_is_inflated_in_that_one_buffer(int length, int limit)
    {
        byte[] data = Pattern(length);
        byte[] body = Gzip(data);

        using var ledger = IngestBufferPoolLedger.Open();
        Assert.Equal(InflateResult.Ok, Inflate(body, out var message, limit));

        Assert.Equal(data, message);
        Assert.True(ledger.Rents == 1,
            $"a {length:N0}-byte message that fits its first buffer took {ledger.Rents} rents, the largest {ledger.LargestRent:N0} B");
        ledger.AssertEveryBufferCameBackOnce(minRents: 1);
    }

    // ── The bomb ──────────────────────────────────────────────────────────────

    /// <summary>
    /// THE BOUND #57 BLOCKED ON, AS BYTES ON THE THREAD. 256 MiB of zeros compresses about
    /// 1032:1, to a body a quarter of the limit — small enough to pass any wire check. What
    /// inflating it may cost is the limit and not a byte of the 256 MiB it describes:
    /// <list type="bullet">
    ///   <item>its trailer honest (every exporter's), the first rent is clamped to the limit and
    ///   is the only one: at most <c>Limit</c> allocated;</item>
    ///   <item>its trailer lying low, so the buffer starts at 64 KiB and doubles into the limit:
    ///   64 KiB + 128 KiB + … + 1 MiB, under <c>2 x Limit</c> allocated in all, and never more
    ///   than one and a half limits held at once (the old buffer and the new one, at the copy).</item>
    /// </list>
    /// The pool is emptied first so every rent is a REAL allocation — measured against a warm
    /// pool this would read near zero and prove nothing. Without the ceiling the same call
    /// allocates past 256 MiB, which is what the thresholds are there to catch.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void A_bomb_costs_at_most_the_limit_on_the_thread_that_inflates_it(bool trailerLiesLow)
    {
        byte[] bomb = GzipBomb.Payload;
        if (trailerLiesLow)
        {
            bomb = (byte[])bomb.Clone();
            BinaryPrimitives.WriteUInt32LittleEndian(bomb.AsSpan(bomb.Length - 4), 1);
        }
        Assert.True(bomb.Length < Limit / 2, $"the bomb must pass a wire check; it is {bomb.Length:N0} bytes");

        Inflate(Gzip(Pattern(1_000)), out _);                           // JIT and first-use state are not what is measured
        IngestBufferPool.Trim();                                        // a cold pool: every rent below allocates

        using var ledger = IngestBufferPoolLedger.Open();
        long before = GC.GetAllocatedBytesForCurrentThread();
        var result  = OtlpGzip.Inflate(bomb, Limit, out byte[]? rented, out _);
        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        _out.WriteLine($"bomb {bomb.Length:N0} B → {GzipBomb.InflatedBytes:N0} B described; trailer "
                     + $"{(trailerLiesLow ? "lying low" : "honest")}: {allocated:N0} B allocated, largest rent "
                     + $"{ledger.LargestRent:N0} B, peak held {ledger.PeakOutstandingBytes:N0} B (limit {Limit:N0} B)");

        Assert.Equal(InflateResult.TooLarge, result);
        Assert.Null(rented);
        Assert.True(ledger.LargestRent <= Limit, $"a rent of {ledger.LargestRent:N0} B passed the {Limit:N0} B limit");

        long allocationBound = (trailerLiesLow ? 2L * Limit : Limit) + Slack;
        long heldBound       = trailerLiesLow ? Limit + Limit / 2 : Limit;
        Assert.True(allocated <= allocationBound,
            $"inflating the bomb allocated {allocated:N0} B on this thread; the bound is {allocationBound:N0} B");
        Assert.True(ledger.PeakOutstandingBytes <= heldBound,
            $"the inflate held {ledger.PeakOutstandingBytes:N0} B at once; the bound is {heldBound:N0} B");
        ledger.AssertEveryBufferCameBackOnce(minRents: 1);
    }

    [Fact]
    public void A_message_exactly_at_the_limit_is_accepted_whole()
    {
        // The boundary from the other side, now with the trailer check in the way: at exactly
        // the limit the loop stops on "one more byte?" rather than on end of stream, and the
        // trailer it then checks must still fit.
        byte[] data = Pattern(Limit);
        Assert.Equal(InflateResult.Ok, Inflate(Gzip(data), out var message));
        Assert.Equal(data, message);
    }

    // ── Payloads ──────────────────────────────────────────────────────────────

    internal static byte[] Gzip(ReadOnlySpan<byte> data)
    {
        using var ms = new MemoryStream();
        using (var z = new GZipStream(ms, CompressionMode.Compress, leaveOpen: true)) z.Write(data);
        return ms.ToArray();
    }

    /// <summary>Compressible but verifiable — a wrong offset anywhere shows up as a wrong byte.</summary>
    private static byte[] Pattern(int length, int seed = 0)
    {
        var data = new byte[length];
        for (int i = 0; i < length; i++) data[i] = (byte)((i / 7 + seed) % 251);
        return data;
    }
}

/// <summary>
/// 256 MiB of zeros, gzipped once per test run: about 250 KB that describe a quarter of a
/// gigabyte — deflate's ~1032:1 ceiling, which is what makes a compressed-body limit on the WIRE
/// meaningless on its own. Shared by the inflate tests and the HTTP receiver's.
/// </summary>
internal static class GzipBomb
{
    public const long InflatedBytes = 256L * 1024 * 1024;

    private static readonly Lazy<byte[]> _payload = new(static () =>
    {
        var zeros = new byte[1024 * 1024];
        using var ms = new MemoryStream();
        using (var z = new GZipStream(ms, CompressionLevel.Optimal, leaveOpen: true))
            for (long written = 0; written < InflatedBytes; written += zeros.Length) z.Write(zeros);
        return ms.ToArray();
    });

    public static byte[] Payload => _payload.Value;
}
