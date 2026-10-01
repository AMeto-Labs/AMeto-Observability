using System.Buffers;
using Ameto.Core;
using Ameto.Indexing;
using Ameto.Query.Filtering;
using Ameto.Storage;
using MessagePack;

namespace Ameto.Query.Tests;

/// <summary>
/// The service's inverted bucket was renamed from <c>service.name</c> to <c>@service</c>, and no
/// segment is ever rewritten to the new name. Every filter spelling of the service now hints
/// <c>@service</c>; a group built before the rename has only <c>service.name</c>, and asking it
/// for <c>@service</c> alone would find no bucket — at best a full scan, and in a group that also
/// holds an <c>@service</c> USER property, a bucket without the value: "proven empty", the group
/// dropped, its rows gone.
///
/// <para>Each test runs the executor's own prefilter (<see cref="QueryExecutor.PassesBloomGate"/>,
/// then <see cref="QueryExecutor.TryNarrowWithIndex(CompiledFilter, ISegmentIndex, out uint[])"/>)
/// over a real view, so what is asserted is the decision production makes. The old layout is
/// written by hand — the builder that wrote it no longer exists — and the new one by the
/// builder itself.</para>
/// </summary>
public sealed class ServiceBucketRenameTests
{
    private const string Auth   = "Auth.API";
    private const string Wallet = "Wallet.API";

    /// <summary>Every spelling the filter language accepts for the service, each pointing at one row.</summary>
    public static TheoryData<string> Spellings => new()
    {
        $"@service = '{Auth}'",
        $"service.name = '{Auth}'",
        $"['service.name'] = '{Auth}'",
        $"ServiceName = '{Auth}'",
        $"@service in ['{Auth}']",
        $"@l = 'Information' and @service = '{Auth}'",
    };

    // ── A group built before the rename ──────────────────────────────────────

    [Theory]
    [MemberData(nameof(Spellings))]
    public void LegacyGroup_IsFoundThroughItsServiceNameBucket(string expression)
    {
        var (inv, bloom) = Group(
            ("service.name", 0, Wallet), ("service.name", 1, Auth), ("service.name", 2, Wallet),
            ("@l", 0, "Information"), ("@l", 1, "Information"), ("@l", 2, "Information"));

        Assert.True(Prefilter(CompiledFilter.Compile(expression), cache: null, "legacy.seg", inv, bloom, out var candidates),
            "a group built before the rename was dropped for its own service");
        Assert.Equal([1u], candidates!);
    }

    [Fact]
    public void LegacyGroup_AbsentServiceIsStillProof()
    {
        // The legacy bucket is a COMPLETE bucket, not "no information": a service it does not
        // hold must still prune the group, or the fallback would cost every old segment a scan.
        var (inv, bloom) = Group(("service.name", 0, Wallet), ("Probe", 0, "Billing.API"));

        using var view = SegmentIndexView.OverSections(null, "legacy.seg", 0, inv, default, bloom);
        var proven = view.LookupIntersect([(ClefFields.ServiceName, (object?)"Billing.API")]);
        Assert.NotNull(proven);                                      // known bucket…
        Assert.Empty(proven!);                                       // …without the value: proof
        Assert.False(view.MightContain(ClefFields.ServiceName, "Billing.API"));

        // A group with no service bucket under either name has no opinion at all.
        var (bare, bareBloom) = Group(("Probe", 0, "Billing.API"));
        using var bareView = SegmentIndexView.OverSections(null, "bare.seg", 0, bare, default, bareBloom);
        Assert.Null(bareView.LookupIntersect([(ClefFields.ServiceName, (object?)"Billing.API")]));
    }

    // ── A group the current builder writes ───────────────────────────────────

    [Theory]
    [MemberData(nameof(Spellings))]
    public void NewGroup_IsFoundThroughItsAtServiceBucket(string expression)
    {
        using var hot = new HotTierSegment(16, 64 * 1024);
        var pool = new StringInternPool();
        Write(hot, pool, 1, Wallet);
        Write(hot, pool, 2, Auth);
        Write(hot, pool, 3, Wallet);
        hot.Freeze();

        var builder = new SegmentIndexBuilder(hot.Count);
        builder.Build(hot, pool);
        byte[] inv = builder.SerialisedInvertedIndex, bloom = builder.SerialisedBloomFilter;

        // The builder files under the new name and ONLY the new name: asked by the old name
        // itself (which has no fallback of its own), the group has no such bucket.
        using var reader = SegmentIndexReader.Load(inv, default, bloom);
        Assert.Equal([1u], reader.LookupIntersect([(ClefFields.ServiceName, (object?)Auth)])!);
        Assert.Null(reader.LookupIntersect([(ClefFields.LegacyServiceName, (object?)Auth)]));
        Assert.Null(SegmentInvertedIndex.Deserialise(inv).LookupIntersect([(ClefFields.LegacyServiceName, (object?)Auth)]));

        Assert.True(Prefilter(CompiledFilter.Compile(expression), cache: null, "new.seg", inv, bloom, out var candidates));
        Assert.Equal([1u], candidates!);
    }

    // ── A group holding both names ───────────────────────────────────────────

    [Fact]
    public void GroupHoldingBothNames_UnionsThem_AndBothReadersAgree()
    {
        // An old group whose client sent a literal `@service` USER property on one row: the group
        // has an `@service` bucket that does not hold the header's value. Trying the old name only
        // when the new one is missing would read that bucket's silence as proof and drop row 1.
        var (inv, bloom) = Group(("@service", 0, "Rogue"), ("service.name", 1, Auth), ("service.name", 0, Wallet));

        Assert.True(Prefilter(CompiledFilter.Compile($"@service = '{Auth}'"), cache: null, "both.seg", inv, bloom, out var candidates),
            "a bucket under the new name that lacks the value must not hide the old one");
        Assert.Equal([1u], candidates!);

        // The decoded index and the lazy reader answer by one rule.
        var       decoded = SegmentInvertedIndex.Deserialise(inv);
        using var lazy    = SegmentIndexReader.Load(inv, default, bloom);
        foreach (var value in new[] { Auth, Wallet, "Rogue", "Nobody" })
        {
            IReadOnlyList<(string, object?)> q = [(ClefFields.ServiceName, value)];
            Assert.Equal(decoded.LookupIntersect(q), lazy.LookupIntersect(q));
            Assert.Equal(decoded.MightContain(ClefFields.ServiceName, value), lazy.MightContain(ClefFields.ServiceName, value));
        }
        Assert.Equal([0u], lazy.LookupIntersect([(ClefFields.ServiceName, (object?)"Rogue")])!);
    }

    // ── The cache's memo keeps the two buckets apart ─────────────────────────

    [Fact]
    public void Memo_RemembersEachBucketUnderItsOwnName()
    {
        // The SAME value under both names, on different rows. A memo that keyed a bucket by the
        // value alone — or by the hint key instead of the bucket's own name — would hand the
        // second bucket the first one's postings, and the repeated query would lose a row.
        var (inv, bloom) = Group(("@service", 0, Auth), ("service.name", 2, Auth), ("service.name", 1, Wallet));
        using var cache = new SegmentIndexCache(1 << 20);
        var filter = CompiledFilter.Compile($"@service = '{Auth}'");

        Assert.True(Prefilter(filter, cache, "both.seg", inv, bloom, out var first, out bool firstRead));
        Assert.True(firstRead);                                          // a cold memo reads the section
        Assert.Equal([0u, 2u], first!);

        Assert.True(Prefilter(filter, cache, "both.seg", inv, bloom, out var again, out bool againRead));
        Assert.False(againRead);                                         // answered from the memo alone
        Assert.Equal([0u, 2u], again!);

        // And a legacy-only group in the same cache answers from its own memo, not this one's.
        var (old, oldBloom) = Group(("service.name", 3, Auth), ("service.name", 0, Wallet));
        Assert.True(Prefilter(filter, cache, "legacy.seg", old, oldBloom, out var legacy, out _));
        Assert.Equal([3u], legacy!);
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    /// <summary>One group's inverted section and bloom, written bucket by bucket.</summary>
    private static (byte[] Inverted, byte[] Bloom) Group(params (string Bucket, uint Offset, string Value)[] postings)
    {
        var inv   = new SegmentInvertedIndex();
        var bloom = SegmentBloomFilter.Create(64);
        try
        {
            foreach (var (bucket, offset, value) in postings.OrderBy(static p => p.Offset))
            {
                inv.Add(offset, bucket, value);
                bloom.Add(value);
            }
            return (inv.Serialise(), bloom.Serialise());
        }
        finally { bloom.Dispose(); }
    }

    private static bool Prefilter(CompiledFilter filter, SegmentIndexCache? cache, string path,
                                  byte[] inverted, byte[] bloom, out uint[]? candidates)
        => Prefilter(filter, cache, path, inverted, bloom, out candidates, out _);

    /// <summary>The executor's two prefilter phases over one group view; false = the group is dropped.</summary>
    private static bool Prefilter(CompiledFilter filter, SegmentIndexCache? cache, string path,
                                  byte[] inverted, byte[] bloom, out uint[]? candidates, out bool readSections)
    {
        candidates = null;
        using var index = SegmentIndexView.OverSections(cache, path, 0, inverted, default, bloom);
        bool kept = (!filter.TryGetIndexHint(out _, out _) || QueryExecutor.PassesBloomGate(filter, index))
                 && QueryExecutor.TryNarrowWithIndex(filter, index, out candidates);
        readSections = index.ReadSections;
        return kept;
    }

    private static void Write(HotTierSegment hot, StringInternPool pool, ulong id, string service)
    {
        const string template = "Handled {Request}";
        var header = new LogEventHeader
        {
            Id                       = id,
            TimestampUtcTicks        = new DateTimeOffset(2026, 10, 1, 9, 0, (int)id, TimeSpan.Zero).UtcTicks,
            MessageTemplatePoolIndex = pool.Intern(template),
            ServiceNamePoolIndex     = pool.Intern(service),
            Level                    = LogLevel.Information,
        };
        var buf = new ArrayBufferWriter<byte>(32);
        var w   = new MessagePackWriter(buf);
        w.WriteMapHeader(1);
        w.Write("Request");
        w.Write($"r-{id}");
        w.Flush();
        Assert.True(hot.TryWrite(header, buf.WrittenSpan, template, null));
    }
}
