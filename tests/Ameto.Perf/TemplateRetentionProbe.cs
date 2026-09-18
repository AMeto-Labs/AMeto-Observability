using Ameto.Core;
using Ameto.Storage;
using Xunit;
using Xunit.Abstractions;

namespace Ameto.Perf;

/// <summary>
/// Retained-memory probe for the hot tier's per-event template slot.
///
/// <para>Every event written into a <see cref="HotTierSegment"/> parks a
/// <see cref="string"/> reference in the chunk's template array for the LIFE of the tier
/// (seconds to minutes). If the ingest path hands over the fresh string it just decoded
/// off the wire, the tier pins one duplicate per event; if it hands over the intern pool's
/// canonical instance, it pins one per distinct template. The difference is resident
/// memory that is released only as gen2 garbage at flush.</para>
///
/// <para>This measures the managed heap actually held while the tier is alive —
/// <c>GC.GetTotalMemory(forceFullCollection: true)</c> before and after — for 100k events
/// over a realistic (low-cardinality) template set.</para>
/// </summary>
public sealed class TemplateRetentionProbe
{
    private readonly ITestOutputHelper _out;
    public TemplateRetentionProbe(ITestOutputHelper o) => _out = o;

    private const int EventCount     = 100_000;
    private const int DistinctTmpls  = 32;

    private static readonly string[] Templates = BuildTemplates();

    private static string[] BuildTemplates()
    {
        var t = new string[DistinctTmpls];
        for (int i = 0; i < DistinctTmpls; i++)
            t[i] = $"Order {{OrderId}} for customer {{CustomerId}} moved to state {{State}} by worker {i} in region {{Region}}";
        return t;
    }

    [Fact]
    public void CanonicalTemplateRetainsFarLessThanPerEventDuplicates()
    {
        // Warm the pool + JIT on a throwaway run so the numbers below are steady state.
        Measure(canonical: true,  out _);
        Measure(canonical: false, out _);

        long dupBytes  = Measure(canonical: false, out int wroteDup);
        long canonBytes = Measure(canonical: true,  out int wroteCanon);

        Assert.Equal(EventCount, wroteDup);
        Assert.Equal(EventCount, wroteCanon);

        string report =
            $"Hot tier template retention — {EventCount:N0} events, {DistinctTmpls} distinct templates:\n" +
            $"  fresh string per event (before): {dupBytes:N0} B retained  ({(double)dupBytes / EventCount:F1} B/event)\n" +
            $"  canonical pool instance (after): {canonBytes:N0} B retained  ({(double)canonBytes / EventCount:F1} B/event)\n" +
            $"  saved                          : {dupBytes - canonBytes:N0} B" +
            $"  ({(dupBytes == 0 ? 0 : 100.0 * (dupBytes - canonBytes) / dupBytes):F1}%)";
        _out.WriteLine(report);

        // The duplicate run must retain at least the string bodies it allocated; the
        // canonical run retains only the reference array. Generous factor so the gate is
        // about the regression, not about GC timing noise.
        Assert.True(canonBytes * 3 < dupBytes,
            $"expected canonical retention to be far below duplicate retention, got {canonBytes:N0} vs {dupBytes:N0}");
    }

    /// <summary>
    /// Writes <see cref="EventCount"/> events into a real tier and returns the managed
    /// bytes still held with the tier alive. <paramref name="canonical"/> selects between
    /// the pool's shared instance and a fresh per-event string (what the wire decode hands
    /// the ingest path).
    /// </summary>
    private static long Measure(bool canonical, out int written)
    {
        var pool = new StringInternPool();
        var idx  = new int[DistinctTmpls];
        for (int i = 0; i < DistinctTmpls; i++)
            idx[i] = pool.Intern(Templates[i]);

        long before = GC.GetTotalMemory(forceFullCollection: true);

        using var tier = new HotTierSegment(EventCount + 1, (long)EventCount * 64 + (8L << 20));
        ReadOnlySpan<byte> payload = [0x80];     // empty msgpack map (fixmap 0)

        int n = 0;
        for (int i = 0; i < EventCount; i++)
        {
            int t = i % DistinctTmpls;

            // A fresh instance every time — exactly what reader.ReadString() produces on
            // the CLEF path and cis.ReadString() on the proto path.
            string perEvent = canonical ? pool.Get(idx[t]) : new string(Templates[t].AsSpan());

            var h = new LogEventHeader
            {
                Id                       = (ulong)i,
                TimestampUtcTicks        = DateTime.UtcNow.Ticks,
                MessageTemplatePoolIndex = idx[t],
                ServiceNamePoolIndex     = -1,
                Level                    = Ameto.Core.LogLevel.Information,
            };
            if (tier.TryWrite(h, payload, perEvent)) n++;
        }

        long after = GC.GetTotalMemory(forceFullCollection: true);
        written = n;

        GC.KeepAlive(tier);
        GC.KeepAlive(pool);
        return after - before;
    }
}
