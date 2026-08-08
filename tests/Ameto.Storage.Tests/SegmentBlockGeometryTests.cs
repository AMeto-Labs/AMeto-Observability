using Ameto.Core;
using Xunit;

namespace Ameto.Storage.Tests;

/// <summary>
/// What one row costs the block format before it holds anything.
///
/// <para><see cref="SegmentWriter.FixedRowCostBytes"/> is not a tuning number. It floors the
/// divisor that turns an index group's payload budget into an event forecast, and that forecast
/// is what a group's bloom filter is sized from — so it decides, on its own, the largest filter
/// the writer can ever ask for. The bound documented on <c>SegmentBloomFilter.MaxFilterBytes</c>
/// is 64 MB / this number, and the reason it can be stated at all is that no row can come in
/// under it.</para>
///
/// <para>Which is exactly what went wrong when the number was a guessed 32: no row could reach
/// THAT either, so the floor never bound anything and the worst case documented against it —
/// 2 097 152 events and 160 MiB of filter for one group — described a writer that does not
/// exist, overstating the real ceiling by 1.8x. A constant nothing measures is a constant that
/// can be quietly wrong in either direction, so this measures it.</para>
/// </summary>
public sealed class SegmentBlockGeometryTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-geometry-" + Guid.NewGuid().ToString("N"));
    private readonly long   _base = new DateTimeOffset(2026, 8, 3, 9, 0, 0, TimeSpan.Zero).UtcTicks;

    public SegmentBlockGeometryTests() => Directory.CreateDirectory(_dir);
    public void Dispose() { try { Directory.Delete(_dir, true); } catch { } }

    /// <summary>
    /// The thinnest row the format can express — a one-character template, no service name, no
    /// properties, no exception, no trace — still costs the fixed columns (@t 8, @l 1, @i 8,
    /// trace 16, span 8) and one uint32 offset in each of the four string columns. The floor has
    /// to be at or under that, or the event forecast is too large and the filter over-sized; and
    /// it has to be CLOSE to it, or the floor is decorative and the ceiling derived from it is
    /// a number about nothing.
    /// </summary>
    [Fact]
    public void NoRowCostsLessThanTheForecastsFloor()
    {
        var (bytes, events) = WriteThinnestPossible(20_000);
        double perEvent = bytes / (double)events;

        Assert.True(perEvent >= SegmentWriter.FixedRowCostBytes,
            $"the format spends {perEvent:F1} B/row, under the {SegmentWriter.FixedRowCostBytes} B floor the " +
            "event forecast is divided by — the forecast, and every filter sized from it, is now too large");

        // Tight, not merely safe. A floor well under what the format actually spends is what let
        // the old 32 sit there unreachable; the block header and the four string columns' extra
        // (n+1)-th offset are the only things between the constant and this measurement.
        Assert.True(perEvent < SegmentWriter.FixedRowCostBytes + 8,
            $"the format spends {perEvent:F1} B/row against a {SegmentWriter.FixedRowCostBytes} B floor — " +
            "the floor has drifted far enough below the format that nothing it bounds means anything");
    }

    /// <summary>
    /// The consequence, stated as the number the filter ceiling's own documentation is built on:
    /// the most events one 64 MB group can be forecast to hold. On the merge path the source's
    /// remaining-event hint is the sum of every source and does not bind, so this quotient is the
    /// forecast, and at the fallback 64 terms/event it is what decides whether the ceiling on one
    /// filter is a backstop or a routine constraint.
    /// </summary>
    [Fact]
    public void TheLargestForecastAGroupCanMakeIsTheBudgetOverThatFloor()
    {
        long maxEvents = SegmentWriter.DefaultGroupPayloadBudgetBytes / SegmentWriter.FixedRowCostBytes;
        long maxTerms  = maxEvents * 64;                 // SegmentIndexBuilder.EstimatedBloomTermsPerEvent
        double maxMiB  = maxTerms * 10 / 8.0 / 1048576;  // ~10 bits a term

        Assert.Equal(1_177_348, maxEvents);
        Assert.InRange(maxMiB, 89.0, 90.5);
    }

    /// <summary>Writes the thinnest rows the format can express and returns the segment's total
    /// uncompressed bytes and event count.</summary>
    private (long UncompressedBytes, long Events) WriteThinnestPossible(int events)
    {
        var pool = new StringInternPool();
        using var hot = new HotTierSegment(events + 1, (long)events * 256 + (8L << 20));
        int tmplIdx = pool.Intern("x");

        for (int i = 0; i < events; i++)
        {
            var h = new LogEventHeader
            {
                Id                       = new EventId(0u, (uint)i).RawValue,
                TimestampUtcTicks        = _base + i * TimeSpan.TicksPerMillisecond,
                Level                    = LogLevel.Information,
                MessageTemplatePoolIndex = tmplIdx,
                ServiceNamePoolIndex     = -1,
            };
            if (!hot.TryWrite(h, default, null, null)) break;
        }
        hot.Freeze();

        string path = Path.Combine(_dir, "thin.seg");
        using var writer = new SegmentWriter(path);
        writer.WriteEvents(hot, pool, SegmentWriter.ComputeSortOrder(hot));
        var info = writer.Finalise(new NodeId(0), new SegmentId(1UL));
        return (info.UncompressedBytes, info.EventCount);
    }
}
