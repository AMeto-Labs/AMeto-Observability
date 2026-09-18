using Ameto.Core;
using Ameto.Otel;
using Microsoft.AspNetCore.Http;

namespace Ameto.Integration.Tests;

/// <summary>
/// What a body at or over <c>Ingestion.MaxOtlpBatchBytes</c> costs the OTLP receivers before it
/// is refused — which is the interesting number, because the request it describes is either
/// abusive or misconfigured and the receiver has to survive a stream of them.
///
/// <para>The defect these pin: both receivers grew a full buffer by doubling, with no reference
/// to the ceiling. At the DEFAULT 8 MiB limit the last doubling asked for 16 MiB, which is one
/// bucket past <see cref="IngestBufferPool.MaxPooledBytes"/> — so it was not a pooled array at
/// all but a fresh 16 MiB allocation on the large object heap, filled with bytes that were about
/// to be thrown away with the 413. On the 512 MB console stand, once per such request.</para>
///
/// <para>These drive <see cref="OtlpBodyReader"/> directly rather than through a host. That is
/// the honest way to cover BOTH receivers: it is literally the code each of their
/// <c>ReadBodyAsync</c> methods runs, and the gRPC one cannot be tested through TestServer at
/// all — see <see cref="OtlpGrpcFramingTests"/>, which explains why a green test there can
/// assert transport behaviour the real server does not have.</para>
///
/// <para>The counting is <see cref="IngestBufferPoolLedger"/>: every rent this flow makes from
/// the pool, and what it gave back.</para>
/// </summary>
public sealed class OtlpOversizedBodyTests
{
    /// <summary>
    /// The ceiling under test. <c>Ingestion.MaxOtlpBatchBytes</c> defaults to exactly this, and
    /// so does the largest array the pool serves — the two being the same number is what put the
    /// old doubling one bucket into unpooled territory, so a smaller stand-in would prove
    /// nothing: below the pool's cap a rent rounds up to a power of two and hides the difference.
    /// </summary>
    private const int Max = IngestBufferPool.MaxPooledBytes;   // 8 MiB

    [Fact]
    public async Task A_body_over_the_ceiling_is_refused_without_renting_past_it()
    {
        var ctx = new DefaultHttpContext();
        ctx.Request.Body = new MemoryStream(new byte[Max + 1]);   // no Content-Length: the doubling road

        using var ledger = IngestBufferPoolLedger.Open();
        var (buffer, length) = await OtlpBodyReader.ReadAsync(ctx, Max);

        Assert.Null(buffer);
        Assert.Equal(0, length);
        Assert.True(ledger.LargestRent <= Max,
            $"a body {Max + 1:N0} bytes long was refused at {Max:N0}, but the read asked the pool for "
          + $"{ledger.LargestRent:N0} bytes — above IngestBufferPool.MaxPooledBytes ({IngestBufferPool.MaxPooledBytes:N0}) "
          + "that is an unpooled large-object array, allocated only to be dropped with the refusal");
        ledger.AssertEveryBufferCameBackOnce(minRents: 2);
    }

    [Fact]
    public async Task A_body_exactly_at_the_ceiling_is_read_whole_without_renting_past_it()
    {
        // The other side of the same decision, and the reason the buffer is not simply clamped
        // and left: a body that ENDS on the ceiling is legal and must come back whole. What
        // settles which of the two this is costs one byte, not one more buffer.
        var ctx = new DefaultHttpContext();
        ctx.Request.Body = new MemoryStream(new byte[Max]);

        using var ledger = IngestBufferPoolLedger.Open();
        var (buffer, length) = await OtlpBodyReader.ReadAsync(ctx, Max);

        Assert.NotNull(buffer);
        Assert.Equal(Max, length);
        IngestBufferPool.Return(buffer);                        // on success the caller owns it
        Assert.True(ledger.LargestRent <= Max,
            $"a body of exactly {Max:N0} bytes fits the ceiling, but the read asked the pool for "
          + $"{ledger.LargestRent:N0} bytes to discover it");
        ledger.AssertEveryBufferCameBackOnce(minRents: 2);
    }

    /// <summary>
    /// The growth rule itself. Doubling is untouched below the ceiling; a ceiling that is not a
    /// power of two clamps to one byte past it (nothing beyond that can be anything but refused);
    /// and the target is always larger than the buffer it replaces, or the read that follows
    /// would get an empty span and mistake a full buffer for the end of the body.
    /// </summary>
    [Theory]
    [InlineData(65_536,           Max,                65_536 * 2)]        // room to spare: plain doubling
    [InlineData(4 * 1024 * 1024,  10_000_000,         8 * 1024 * 1024)]   // doubling still fits: untouched
    [InlineData(Max,              10_000_000,         10_000_001)]        // would overshoot 10 MB: clamped, not 16 MiB
    [InlineData(Max,              Max,                Max + 1)]           // at the ceiling: one byte past it
    [InlineData(1024,             100,                1025)]              // never below current + 1
    public void The_growth_target_never_passes_the_ceiling_that_is_about_to_refuse_it(
        int current, int maxBytes, int expected)
    {
        int target = OtlpBodyReader.GrowTarget(current, maxBytes);

        Assert.Equal(expected, target);
        Assert.True(target > current, "a grow must leave the next read somewhere to go");
        Assert.True(target <= (long)maxBytes + 1 || target == current + 1,
            "nothing past one byte beyond the ceiling can hold anything but refused bytes");
    }
}
