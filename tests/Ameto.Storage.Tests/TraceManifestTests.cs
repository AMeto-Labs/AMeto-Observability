using Microsoft.Extensions.Logging.Abstractions;
using Ameto.Tracing.Storage;
using Xunit.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// THE CATALOG'S TWO PROMISES: an id means one segment for the life of the install, and a claim of
/// coverage is never made over something the catalog cannot back.
///
/// <para>The second is the one worth testing hardest. Everything the trace-id index will later do
/// rests on "a negative answer counts only inside <c>Covered</c>", so the tests below spend most of
/// their effort on the ways that set could become a lie — damage, truncation, a crash between two
/// writes, a segment removed while its coverage stayed behind.</para>
/// </summary>
public sealed class TraceManifestTests : IDisposable
{
    private readonly string            _dir;
    private readonly ITestOutputHelper _out;

    public TraceManifestTests(ITestOutputHelper output)
    {
        _out = output;
        _dir = Path.Combine(Path.GetTempPath(), "ameto-manifest-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_dir);
    }

    public void Dispose() { try { Directory.Delete(_dir, true); } catch { } }

    private TraceManifest Open() => TraceManifest.Load(_dir, NullLogger.Instance);
    private string        Path_  => System.IO.Path.Combine(_dir, TraceManifest.FileName);

    private static TraceSegmentEntry Seg(ulong id, string path = "", long min = 1000, long max = 2000)
        => new(id, path.Length == 0 ? $"seg-{id}.trc" : path, min, max, 100);

    /// <summary>A run written beside one segment — the kind that dies with it.</summary>
    private static TraceIndexRun Run(string path, ulong coversSegment)
        => new(1, path, 0x1000, 0x9000, 42, coversSegment);

    /// <summary>A run produced by index compaction — covers many, dies with none of them.</summary>
    private static TraceIndexRun MergedRun(string path)
        => new(2, path, 0x1000, 0x9000, 999, CoversSegment: null);

    // ── Identity ───────────────────────────────────────────────────────────────

    [Fact]
    public void Ids_are_unique_and_survive_a_restart()
    {
        var m  = Open();
        var a  = m.AllocateSegmentId();
        var b  = m.AllocateSegmentId();
        Assert.NotEqual(a, b);

        m.AddSegment(Seg(a));
        m.AddSegment(Seg(b));

        // A NEW PROCESS MUST NOT REISSUE THEM. An index entry naming segment 1 has to mean one
        // file forever; handing 1 out again after a restart would silently point it at another.
        var reopened = Open();
        var c = reopened.AllocateSegmentId();
        _out.WriteLine($"before restart: {a}, {b}; after: {c}");
        Assert.True(c > b, $"id {c} was reissued at or below {b}");
        Assert.Equal(2, reopened.Segments.Count);
    }

    [Fact]
    public void The_allocator_floor_is_derived_from_the_segments_not_believed_from_the_field()
    {
        // The field can be torn low without failing the CRC — the CRC is rewritten by whatever
        // wrote the damage. What cannot be faked is the ids already in the catalog, so the floor
        // is taken from them.
        var m = Open();
        for (int i = 0; i < 5; i++) m.AddSegment(Seg(m.AllocateSegmentId()));

        byte[] raw = File.ReadAllBytes(Path_);
        // nextSegmentId sits at offset 4 (magic) + 2 (version) + 8 (generation) = 14.
        System.Buffers.Binary.BinaryPrimitives.WriteUInt64LittleEndian(raw.AsSpan(14), 1UL);
        uint crc = Ameto.Core.Crc32c.Append(0, raw.AsSpan(0, raw.Length - 4));
        System.Buffers.Binary.BinaryPrimitives.WriteUInt32LittleEndian(raw.AsSpan(raw.Length - 4), crc);
        File.WriteAllBytes(Path_, raw);

        var reopened = Open();
        ulong next = reopened.AllocateSegmentId();
        _out.WriteLine($"nextSegmentId forced to 1; allocator returned {next}");
        Assert.True(next > 5, $"the allocator reissued {next} over ids already in the catalog");
    }

    // ── Coverage ───────────────────────────────────────────────────────────────

    [Fact]
    public void A_segment_is_covered_only_when_its_run_is_written_in_the_same_generation()
    {
        var m  = Open();
        var id = m.AllocateSegmentId();

        m.AddSegment(Seg(id));                       // no run yet — the backfill has not run
        Assert.False(m.IsCovered(id));
        Assert.Empty(m.Runs);

        m.MarkCovered(id, Run("seg-1.tix", id));
        Assert.True(m.IsCovered(id));
        Assert.Single(m.Runs);

        // And it survives the trip through the file, which is the only form that matters.
        Assert.True(Open().IsCovered(id));
    }

    [Fact]
    public void Removing_a_segment_takes_its_coverage_and_its_run_with_it()
    {
        var m  = Open();
        var id = m.AllocateSegmentId();
        m.AddSegment(Seg(id), Run("seg-1.tix", id));
        Assert.True(m.IsCovered(id));

        m.RemoveSegments([id]);

        // Coverage left behind would be the index vouching for data that is gone — the exact
        // shape of the silent loss this whole design is built to avoid.
        Assert.False(m.IsCovered(id));
        Assert.Empty(m.Runs);
        Assert.Empty(m.Segments);
        Assert.False(Open().IsCovered(id));
    }

    [Fact]
    public void A_compaction_moves_coverage_from_the_sources_to_the_merged_segment()
    {
        var m = Open();
        var a = m.AllocateSegmentId();
        var b = m.AllocateSegmentId();
        m.AddSegment(Seg(a), Run("a.tix", a));
        m.AddSegment(Seg(b), Run("b.tix", b));

        var merged = m.AllocateSegmentId();
        m.ReplaceSegments([a, b], Seg(merged), Run("merged.tix", merged));

        Assert.False(m.IsCovered(a));
        Assert.False(m.IsCovered(b));
        Assert.True(m.IsCovered(merged));
        Assert.Equal(["merged.tix"], m.Runs.Select(r => r.FilePath));
        Assert.Equal([merged], m.Segments.Keys);
    }

    [Fact]
    public void A_merged_run_outlives_the_segments_it_covers()
    {
        // THE NO-TOMBSTONE PROPERTY, and the distinction the first version of this class got
        // wrong. A run written beside one segment dies with it — nothing else is in it. A run
        // produced by index compaction spans many, so removing one segment must NOT take it:
        // the departing segment's entries become garbage, filtered on read by "is this id still
        // in the catalog?" and dropped physically at the next index compaction. Dropping the run
        // instead would silently un-index every other segment in it.
        var m = Open();
        var a = m.AllocateSegmentId();
        var b = m.AllocateSegmentId();
        m.AddSegment(Seg(a));
        m.AddSegment(Seg(b));
        m.ReplaceRuns([], [MergedRun("L2-0001.tix")]);
        m.MarkCovered(a, MergedRun("L2-0001.tix"));

        m.RemoveSegments([a]);

        Assert.False(m.IsCovered(a));                       // the claim goes
        Assert.NotEmpty(m.Runs);                            // the run stays
        Assert.Contains(m.Runs, r => r.FilePath == "L2-0001.tix");
        Assert.Equal(m.Runs.Count, Open().Runs.Count);      // and through the file

        // The per-segment kind still behaves the other way, in the same catalog.
        m.MarkCovered(b, Run("b.tix", b));
        int before = m.Runs.Count;
        m.RemoveSegments([b]);
        _out.WriteLine($"runs {before} → {m.Runs.Count}; merged run kept, per-segment run dropped");
        Assert.Equal(before - 1, m.Runs.Count);
        Assert.DoesNotContain(m.Runs, r => r.FilePath == "b.tix");
    }

    [Fact]
    public void A_covered_id_naming_no_segment_is_dropped_on_load()
    {
        // Reachable only through damage the CRC did not catch or a bug on the write side. Either
        // way the read path must not be handed a claim the catalog cannot back.
        var m  = Open();
        var id = m.AllocateSegmentId();
        m.AddSegment(Seg(id), Run("seg.tix", id));

        // Rewrite the covered id to one that is in no segment, and re-checksum so the file is
        // "valid" — this test is about the semantic guard, not the CRC.
        byte[] raw = File.ReadAllBytes(Path_);
        int at = raw.Length - 4 - 8;                 // the single covered id sits just before the CRC
        System.Buffers.Binary.BinaryPrimitives.WriteUInt64LittleEndian(raw.AsSpan(at), 9999UL);
        uint crc = Ameto.Core.Crc32c.Append(0, raw.AsSpan(0, raw.Length - 4));
        System.Buffers.Binary.BinaryPrimitives.WriteUInt32LittleEndian(raw.AsSpan(raw.Length - 4), crc);
        File.WriteAllBytes(Path_, raw);

        var reopened = Open();
        _out.WriteLine($"covered after load: {reopened.CoveredCount}");
        Assert.Equal(0, reopened.CoveredCount);
        Assert.False(reopened.IsCovered(9999UL));
    }

    [Fact]
    public void Coverage_can_be_dropped_without_losing_identity()
    {
        // The operator's switch, and the config flag's: stop trusting the index, keep the ids.
        var m  = Open();
        var id = m.AllocateSegmentId();
        m.AddSegment(Seg(id), Run("seg.tix", id));

        m.ClearCoverage();

        Assert.Equal(0, m.CoveredCount);
        Assert.Empty(m.Runs);
        Assert.Single(m.Segments);
        Assert.Equal(1, Open().Segments.Count);
    }

    // ── Degradation: every damaged form loads as empty, never throws ───────────

    public static TheoryData<string, Func<byte[], byte[]>> Damage => new()
    {
        { "truncated to a third",  raw => raw[..(raw.Length / 3)] },
        { "truncated to 4 bytes",  raw => raw[..4] },
        { "empty file",            _   => [] },
        { "wrong magic",           raw => { var c = (byte[])raw.Clone(); c[0] ^= 0xFF; return c; } },
        { "wrong version",         raw => { var c = (byte[])raw.Clone(); c[4] = 0x7F; return c; } },
        { "flipped byte mid-body", raw => { var c = (byte[])raw.Clone(); c[c.Length / 2] ^= 0x01; return c; } },
        { "zeroed checksum",       raw => { var c = (byte[])raw.Clone(); c[^1] = c[^2] = c[^3] = c[^4] = 0; return c; } },
        { "all zeroes",            raw => new byte[raw.Length] },
    };

    [Theory]
    [MemberData(nameof(Damage))]
    public void Any_damaged_manifest_loads_as_empty_and_never_throws(string what, Func<byte[], byte[]> damage)
    {
        var m = Open();
        for (int i = 0; i < 3; i++) m.AddSegment(Seg(m.AllocateSegmentId()), Run($"r{i}.tix", (ulong)(i + 1)));
        Assert.Equal(3, m.CoveredCount);

        File.WriteAllBytes(Path_, damage(File.ReadAllBytes(Path_)));

        // NEVER THROWS is half the promise and EMPTY is the other half. An empty catalog means no
        // ids and no coverage, which means every read falls back to the scan it does today: slow,
        // and right. A throw here would cost the engine its startup over a file nothing needs.
        var reopened = Open();
        _out.WriteLine($"{what,-24} → segments={reopened.Segments.Count} covered={reopened.CoveredCount}");
        Assert.Empty(reopened.Segments);
        Assert.Equal(0, reopened.CoveredCount);
        Assert.Empty(reopened.Runs);

        // And it is usable afterwards rather than wedged: the engine goes on allocating.
        Assert.True(reopened.AllocateSegmentId() > 0);
    }

    [Fact]
    public void A_missing_manifest_is_the_ordinary_first_start_not_a_fault()
    {
        var m = Open();
        Assert.Empty(m.Segments);
        Assert.Equal(0UL, m.Generation);
        Assert.False(File.Exists(Path_));   // nothing is written until something is recorded
    }

    // ── Atomicity ──────────────────────────────────────────────────────────────

    [Fact]
    public void A_reader_never_sees_a_half_written_generation()
    {
        // The rename is what buys this, so the test asserts the property the rename provides:
        // at no point does a file exist that parses to something between two generations. Every
        // intermediate state of the write lives under the temp name.
        var m = Open();
        for (int i = 0; i < 20; i++)
        {
            m.AddSegment(Seg(m.AllocateSegmentId()));
            var seen = Open();
            Assert.Equal(m.Segments.Count, seen.Segments.Count);
            Assert.Equal(m.Generation, seen.Generation);
        }
        Assert.False(File.Exists(System.IO.Path.Combine(_dir, "traces.manifest.tmp")),
            "a temp manifest was left behind");
    }

    [Fact]
    public void Generation_advances_on_every_write_so_a_stale_file_is_recognisable()
    {
        var m = Open();
        ulong g0 = m.Generation;
        m.AllocateSegmentId();
        ulong g1 = m.Generation;
        m.AddSegment(Seg(1));
        ulong g2 = m.Generation;
        _out.WriteLine($"generations: {g0} → {g1} → {g2}");
        Assert.True(g1 > g0 && g2 > g1);
    }

    [Fact]
    public void Paths_round_trip_including_the_ones_windows_produces()
    {
        var m  = Open();
        var id = m.AllocateSegmentId();
        string path = @"C:\ameto-storage\traces\spans-1785585600000000000-1785585600039000000-40-a1b2c3d4.trc";
        m.AddSegment(Seg(id, path, min: -5, max: long.MaxValue));

        var back = Open().Segments[id];
        Assert.Equal(path, back.FilePath);
        Assert.Equal(-5, back.MinStartNano);
        Assert.Equal(long.MaxValue, back.MaxStartNano);
        Assert.Equal(id, Open().IdForPath(path));
    }
}
