using Ameto.Core;

namespace Ameto.Storage.Tests;

/// <summary>
/// <see cref="HotTierAdmission"/> is a copy of <see cref="HotTierSegment"/>'s admission rules, and a
/// spilled block is only as safe as that copy is exact: a spill admits what this says a tier would
/// take, and the despill replays the block into one tier, so a single answer that differs is an event
/// a despill would refuse — lost. These drive a real tier and the copy side by side, over sizes that
/// reach every rule: the event limit, the payload limit checked at a chunk's allocation, and the
/// per-chunk fit that refuses an event and then takes a smaller one.
/// </summary>
public sealed class HotTierAdmissionTests
{
    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    public void It_answers_every_event_exactly_as_a_tier_does(int seed)
    {
        // Three chunks' worth of payload, so the payload limit is met at a chunk boundary, and an
        // event cap that the count can reach too.
        long maxPayload = 2 * HotTierSegment.ChunkPayloadBytes + 123;
        int  maxEvents  = HotTierSegment.EventCapacityFor(maxPayload);
        using var tier  = new HotTierSegment(maxEvents, maxPayload);
        var admission   = new HotTierAdmission(maxEvents, maxPayload);

        var rng     = new Random(seed);
        var payload = new byte[2 * 1024 * 1024];
        int refusedThenTaken = 0;
        bool lastRefused = false;
        for (int i = 0; i < 120_000; i++)
        {
            // Small events first, enough to fill a chunk's slots and cross into the next; then a mix
            // with events large enough to leave a chunk's tail too short for the one after.
            int len = i < 20_000 ? rng.Next(0, 300) : rng.Next(100) switch
            {
                < 2 => rng.Next(256 * 1024, payload.Length),
                < 5 => rng.Next(8 * 1024, 64 * 1024),
                _   => rng.Next(0, 900),
            };
            bool byTier      = tier.TryWrite(new LogEventHeader { TimestampUtcTicks = i }, payload.AsSpan(0, len));
            bool byAdmission = admission.TryAdmit(len);
            Assert.True(byTier == byAdmission, $"event {i} of {len} B: tier {byTier}, admission {byAdmission}");

            if (byTier && lastRefused) refusedThenTaken++;
            lastRefused = !byTier;
        }

        Assert.Equal(tier.Count, admission.Count);
        Assert.Equal(tier.IsFull, admission.PayloadBytes >= maxPayload || admission.Count >= maxEvents);
        Assert.True(refusedThenTaken > 0, "setup: the run never reached a refusal followed by an admission");
        Assert.True(tier.Count > HotTierSegment.ChunkEventCapacity, "setup: the run never crossed into a second chunk");
    }

    /// <summary>
    /// What a despill and a WAL replay rely on to size their tier from the block rather than from
    /// today's configuration: a sequence a tier of some limits takes whole, a tier of larger limits
    /// takes whole too.
    /// </summary>
    [Fact]
    public void A_sequence_a_tier_takes_whole_a_larger_tier_takes_whole()
    {
        long smallPayload = HotTierSegment.ChunkPayloadBytes;   // one chunk
        var  small        = new HotTierAdmission(HotTierSegment.EventCapacityFor(smallPayload), smallPayload);

        var rng     = new Random(7);
        var lengths = new List<int>();
        while (true)
        {
            int len = rng.Next(100) < 3 ? rng.Next(16 * 1024, 128 * 1024) : rng.Next(0, 600);
            if (!small.TryAdmit(len)) break;   // a spill closes its block at the first refusal
            lengths.Add(len);
        }
        Assert.True(lengths.Count > 1_000, "setup: the small tier took too little to mean anything");

        long total = lengths.Sum(static l => (long)l);
        using var larger = new HotTierSegment(Math.Max(lengths.Count, 3 * HotTierSegment.ChunkEventCapacity),
                                              Math.Max(4 * HotTierSegment.ChunkPayloadBytes, total + 1));
        var payload = new byte[128 * 1024];
        for (int i = 0; i < lengths.Count; i++)
            Assert.True(larger.TryWrite(new LogEventHeader { TimestampUtcTicks = i }, payload.AsSpan(0, lengths[i])),
                        $"event {i} of {lengths[i]} B was refused by the larger tier");
    }
}
