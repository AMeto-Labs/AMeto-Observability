using System.Buffers;
using Ameto.Core;
using Ameto.Indexing;
using Ameto.Storage;
using MessagePack;

namespace Ameto.Indexing.Tests;

/// <summary>
/// The builder adds a term to the bloom filter the first time it sees it and not again. The
/// filter is a set, so the section must be byte-identical to adding on every event (the
/// reference build still does that), and the count the writer sizes the next group from must
/// still be the number of terms PRESENTED — repeats included — or every forecast after the
/// first group would silently shrink.
///
/// <para>Byte-identical for the inputs tried here, that is. One input is known not to be, and
/// no query can tell: the KEY of a client property named like a bucket the header files is
/// never added once the header has made the bucket (see the note on first sight in
/// <see cref="SegmentIndexBuilder"/>).</para>
/// </summary>
public sealed class BloomFirstSightTests
{
    private static HotTierSegment Tier(int events, StringInternPool pool, int svcIdx, int tmplIdx, string tmpl)
    {
        var hot = new HotTierSegment(events + 1, (long)events * 512 + 1024 * 1024);
        long baseTicks = DateTimeOffset.UtcNow.UtcTicks;
        var buf = new ArrayBufferWriter<byte>(256);
        for (int i = 0; i < events; i++)
        {
            buf.Clear();
            var w = new MessagePackWriter(buf);
            w.WriteMapHeader(4);
            w.Write("region");  w.Write("ae-dxb");                 // repeats every event
            w.Write("attempt"); w.Write((long)(i % 3));             // three distinct values
            w.Write("ok");      w.Write(i % 2 == 0);                // both bools
            w.Write("user");    w.Write("u-" + i);                  // unique per event
            w.Flush();
            hot.TryWrite(new LogEventHeader
            {
                Id = new EventId(0u, (uint)i).RawValue, TimestampUtcTicks = baseTicks + i,
                Level = LogLevel.Warning, MessageTemplatePoolIndex = tmplIdx, ServiceNamePoolIndex = svcIdx,
                TraceIdHi = 0x11UL, TraceIdLo = (ulong)(i % 5) + 1, SpanId = 0x22UL,
            }, buf.WrittenSpan, tmpl);
        }
        return hot;
    }

    [Fact]
    public void FirstSightBloom_IsByteIdenticalToAddOnEveryEvent_AndCountsPresentedTerms()
    {
        const int events = 1_000;
        var pool   = new StringInternPool();
        int svcIdx = pool.Intern("Wallet.API");
        int tmplIdx = pool.Intern("settlement {Id} done");
        using var hot = Tier(events, pool, svcIdx, tmplIdx, pool.Get(tmplIdx));

        using var streaming = new SegmentIndexBuilder(events);
        streaming.Build(hot, pool);
        using var reference = new SegmentIndexBuilder(events);
        reference.BuildReference(hot, pool);

        Assert.Equal(reference.SerialisedBloomFilter, streaming.SerialisedBloomFilter);

        // Per event: level, template, trace, span, service, and 4 keys + 4 values = 13 presented.
        Assert.Equal(13L * events, streaming.BloomTermsAdded);

        using var bloom = SegmentBloomFilter.Deserialise(streaming.SerialisedBloomFilter);
        foreach (var term in new[] { "warning", "settlement {id} done", "wallet.api", "ae-dxb", "region", "attempt", "2", "ok", "true", "false", "user", "u-999", "0000000000000011" + "0000000000000003" })
            Assert.True(bloom.MightContain(term), term);
    }

    // ── The one term whose first filer gives the bloom a different text ───────

    /// <summary>
    /// "New to the inverted index" stands for "new to the bloom" only where whoever filed the
    /// term first gave the bloom the same text. The exception marker does not: it files
    /// <c>"true"</c> in the bucket <c>@x.exists</c> and gives the bloom the bucket's NAME. A
    /// client property of that literal name holding the string <c>"true"</c> is the same term,
    /// so after the marker it read as a repeat and its text never reached the bloom — which a
    /// filter on the value then asked, and was told the group held no such thing.
    ///
    /// <para>The reference build adds every term on every event and so never had the gap; the
    /// streaming build has to come out byte-identical to it in whichever order the two meet —
    /// rows apart, or the marker and the property on one row, where the marker is always first.
    /// The property is tried as a scalar and as an array, which the walk files alike.</para>
    /// </summary>
    [Theory]
    [InlineData("exception,client")]
    [InlineData("client,exception")]
    [InlineData("both")]
    [InlineData("plain,exception,plain,client,client,exception,both")]
    [InlineData("client,client,exception,client")]
    public void AClientValueInTheMarkersTerm_ReachesTheBloom_WhicheverWasFiledFirst(string order)
    {
        foreach (bool asArray in (bool[])[false, true])
        {
            var pool = new StringInternPool();
            using var hot = MarkerTier(order.Split(','), pool, asArray);

            using var streaming = new SegmentIndexBuilder(hot.Count);
            streaming.Build(hot, pool);
            using var reference = new SegmentIndexBuilder(hot.Count);
            reference.BuildReference(hot, pool);

            Assert.Equal(reference.SerialisedBloomFilter,   streaming.SerialisedBloomFilter);
            Assert.Equal(reference.SerialisedInvertedIndex, streaming.SerialisedInvertedIndex);
            Assert.Equal(reference.BloomTermsAdded,         streaming.BloomTermsAdded);

            // Said outright as well: the value's text, and the bucket's name beside it.
            using var bloom = SegmentBloomFilter.Deserialise(streaming.SerialisedBloomFilter);
            Assert.True(bloom.MightContain("true"), $"`{order}`: the client's value is not in the bloom");
            Assert.True(bloom.MightContain("@x.exists"));
        }
    }

    /// <summary>
    /// The fix adds the one term that was missing and nothing else. In a group where no client
    /// files the marker's value the bloom is given the bucket's name and never <c>"true"</c> —
    /// which is what the reference build does, adding the name for every exception and the value
    /// for none, so its bytes are the statement. (Giving the bloom the value in the marker's own
    /// add would have closed the gap too, and let every group that holds an exception pass the
    /// gate for <c>true</c>.)
    /// </summary>
    [Fact]
    public void WithoutAClientValue_TheMarkerStillGivesTheBloomItsNameAlone()
    {
        var pool = new StringInternPool();
        using var hot = MarkerTier(["exception", "plain", "exception"], pool, asArray: false);

        using var streaming = new SegmentIndexBuilder(hot.Count);
        streaming.Build(hot, pool);
        using var reference = new SegmentIndexBuilder(hot.Count);
        reference.BuildReference(hot, pool);

        Assert.Equal(reference.SerialisedBloomFilter, streaming.SerialisedBloomFilter);
        Assert.Equal(reference.BloomTermsAdded,       streaming.BloomTermsAdded);

        using var bloom = SegmentBloomFilter.Deserialise(streaming.SerialisedBloomFilter);
        Assert.True(bloom.MightContain("@x.exists"));
    }

    /// <summary>
    /// Rows by kind: <c>exception</c> carries one, <c>client</c> the property
    /// <c>@x.exists = "true"</c> (or <c>["true"]</c>), <c>both</c> both, <c>plain</c> neither.
    /// </summary>
    private static HotTierSegment MarkerTier(string[] rows, StringInternPool pool, bool asArray)
    {
        const string Template = "settle {Row}";
        byte[] exception = new ExceptionInfo { Type = "System.InvalidOperationException", Message = "gateway timeout" }.ToBytes();

        var hot = new HotTierSegment(rows.Length + 1, 1024 * 1024);
        var buf = new ArrayBufferWriter<byte>(64);
        for (int i = 0; i < rows.Length; i++)
        {
            bool client = rows[i] is "client" or "both";
            buf.Clear();
            var w = new MessagePackWriter(buf);
            w.WriteMapHeader(client ? 2 : 1);
            w.Write("Row"); w.Write("r-" + i);
            if (client)
            {
                w.Write("@x.exists");
                if (asArray) w.WriteArrayHeader(1);
                w.Write("true");
            }
            w.Flush();

            Assert.True(hot.TryWrite(new LogEventHeader
            {
                Id                       = new EventId(0u, (uint)i).RawValue,
                TimestampUtcTicks        = new DateTimeOffset(2026, 11, 16, 9, 0, 0, TimeSpan.Zero).UtcTicks + i,
                Level                    = LogLevel.Warning,
                MessageTemplatePoolIndex = pool.Intern(Template),
                ServiceNamePoolIndex     = pool.Intern("Billing.API"),
            }, buf.WrittenSpan, Template, rows[i] is "exception" or "both" ? exception : default));
        }
        return hot;
    }
}
