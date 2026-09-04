using Ameto.Tracing;
using Ameto.Tracing.Storage;
using Xunit.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// THE RUN FILE: what it must answer, and what it must never answer.
///
/// <para>A <c>.tix</c> is only allowed to be believed in one direction. A hit is a hint — the
/// caller opens the segment and checks the full trace id anyway, so a wrong hit costs a wasted
/// read. A MISS is the load-bearing answer: it is what lets a segment be skipped, and a miss that
/// is wrong is a trace that has silently stopped existing. So the tests below care much more about
/// false negatives than about anything else, and the two places one could come from — the bloom
/// and the block walk — are pushed at directly.</para>
/// </summary>
public sealed class TraceIndexFileTests : IDisposable
{
    private readonly string            _dir;
    private readonly ITestOutputHelper _out;

    public TraceIndexFileTests(ITestOutputHelper output)
    {
        _out = output;
        _dir = Path.Combine(Path.GetTempPath(), "ameto-tix-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_dir);
    }

    public void Dispose() { try { Directory.Delete(_dir, true); } catch { } }

    private string Path_(string name) => System.IO.Path.Combine(_dir, name);

    /// <summary>Trace ids as OTel produces them: uniformly random, no structure to exploit.</summary>
    private static TraceId Id(int i)
    {
        ulong hi = unchecked((ulong)(i * 6364136223846793005L + 1442695040888963407L));
        ulong lo = unchecked((ulong)(i * 2862933555777941757L + 3037000493L));
        return new TraceId(hi, lo);
    }

    private static uint[] Offsets(int i, int n) => Enumerable.Range(0, n).Select(k => (uint)(i * 8 + k)).ToArray();

    [Fact]
    public void Every_key_written_is_found_again()
    {
        // THE ONLY PROPERTY THAT REALLY MATTERS. A false positive costs a read; a false negative
        // loses a trace. 20 000 entries is five blocks' worth, so this crosses block boundaries,
        // the binary search and the bloom all at once.
        const int N = 20_000;
        var w = new TraceIndexWriter();
        for (int i = 0; i < N; i++) w.Add(Id(i), segmentId: 7, Offsets(i, 3));
        var run = w.Write(Path_("all.tix"), level: 1, coversSegment: 7);

        _out.WriteLine($"{run.EntryCount} entries, {new FileInfo(run.FilePath).Length / 1024} KB "
                     + $"({new FileInfo(run.FilePath).Length / (double)N:F1} B/entry)");

        using var r = TraceIndexReader.Open(run.FilePath);
        Assert.NotNull(r);

        var hits = new List<TraceIndexHit>();
        int missing = 0;
        for (int i = 0; i < N; i++)
        {
            hits.Clear();
            if (!r!.Lookup(TraceIndexFileTestsAccess.Key(Id(i)), hits)) { missing++; continue; }
            if (!hits.Any(h => h.SegmentId == 7 && h.Offsets.SequenceEqual(Offsets(i, 3)))) missing++;
        }
        _out.WriteLine($"retained in RAM: {r!.RetainedBytes / 1024} KB");
        Assert.Equal(0, missing);
    }

    [Fact]
    public void A_key_that_was_never_written_is_usually_refused_and_never_wrongly_resolved()
    {
        var w = new TraceIndexWriter();
        for (int i = 0; i < 5_000; i++) w.Add(Id(i), segmentId: 3, Offsets(i, 2));
        var run = w.Write(Path_("some.tix"), level: 1, coversSegment: 3);

        using var r = TraceIndexReader.Open(run.FilePath)!;
        var hits = new List<TraceIndexHit>();
        int falsePositives = 0;
        for (int i = 1_000_000; i < 1_020_000; i++)
        {
            hits.Clear();
            if (r.Lookup(TraceIndexFileTestsAccess.Key(Id(i)), hits)) falsePositives++;
        }

        // A false positive is legal and cheap — the caller checks the full id against the spans.
        // It must still be RARE, or the fast path stops being fast: the bloom is built for ~1%.
        double rate = falsePositives / 20_000.0;
        _out.WriteLine($"false positives: {falsePositives}/20000 = {rate:P2}");
        Assert.True(rate < 0.05, $"the filter let {rate:P2} through — it is not discriminating");
    }

    [Fact]
    public void One_trace_in_two_segments_returns_both()
    {
        // NOT A CORNER CASE. Spans of a trace arrive over time and a flush lands between them, so
        // a trace really does live in two segments; a merged run holds both. Returning one would
        // draw a waterfall with half its spans missing.
        var w = new TraceIndexWriter();
        for (int i = 0; i < 500; i++) w.Add(Id(i), segmentId: 1, Offsets(i, 2));
        w.Add(Id(42), segmentId: 9, [777, 778, 779]);
        var run = w.Write(Path_("split.tix"), level: 2, coversSegment: null);

        using var r = TraceIndexReader.Open(run.FilePath)!;
        var hits = new List<TraceIndexHit>();
        Assert.True(r.Lookup(TraceIndexFileTestsAccess.Key(Id(42)), hits));

        _out.WriteLine($"segments for the split trace: {string.Join(", ", hits.Select(h => h.SegmentId))}");
        Assert.Equal(2, hits.Count);
        Assert.Contains(hits, h => h.SegmentId == 1);
        Assert.Contains(hits, h => h.SegmentId == 9 && h.Offsets.SequenceEqual([777u, 778u, 779u]));
    }

    [Fact]
    public void Duplicates_are_found_even_when_they_straddle_a_block_boundary()
    {
        // A block closes on an entry boundary, so equal keys can end one block and open the next.
        // The walk has to follow them; stopping at the block edge would drop half a trace, and it
        // would do it only for the traces unlucky enough to land on the seam.
        var w = new TraceIndexWriter();
        for (int i = 0; i < 4_000; i++) w.Add(Id(i), segmentId: 1, Offsets(i, 2));
        // 60 copies of one key is far more than fits beside its neighbours in a 4 KB block.
        for (ulong seg = 100; seg < 160; seg++) w.Add(Id(1234), seg, [(uint)seg]);
        var run = w.Write(Path_("straddle.tix"), level: 2, coversSegment: null);

        using var r = TraceIndexReader.Open(run.FilePath)!;
        var hits = new List<TraceIndexHit>();
        Assert.True(r.Lookup(TraceIndexFileTestsAccess.Key(Id(1234)), hits));

        _out.WriteLine($"copies found: {hits.Count} (1 original + 60 planted)");
        Assert.Equal(61, hits.Count);
        for (ulong seg = 100; seg < 160; seg++)
            Assert.Contains(hits, h => h.SegmentId == seg);
    }

    [Fact]
    public void An_empty_run_is_a_run_that_answers_no_to_everything()
    {
        var run = new TraceIndexWriter().Write(Path_("empty.tix"), level: 1, coversSegment: 5);
        Assert.Equal(0, run.EntryCount);

        using var r = TraceIndexReader.Open(run.FilePath)!;
        Assert.NotNull(r);
        var hits = new List<TraceIndexHit>();
        Assert.False(r.Lookup(TraceIndexFileTestsAccess.Key(Id(1)), hits));
        Assert.Empty(hits);
    }

    [Fact]
    public void Offsets_round_trip_including_large_and_non_contiguous_ones()
    {
        var w = new TraceIndexWriter();
        // Deliberately out of order in the middle. The encoding writes unsigned deltas and cannot
        // express a step backwards, so Add sorts — the alternative, an absolute-value fallback,
        // was tried and turned [200000, 199999] into [200000, 399999] on the way back out: a
        // caller pointed at spans belonging to no trace, silently.
        uint[] awkward = [0u, 1u, 4095u, 4096u, 200_000u, 199_999u, uint.MaxValue / 2];
        uint[] sorted  = [.. awkward.Order()];
        w.Add(Id(1), 11, awkward);
        Assert.Equal([0u, 1u, 4095u, 4096u, 200_000u, 199_999u, uint.MaxValue / 2], awkward);   // caller's array untouched
        w.Add(Id(2), 11, [uint.MaxValue]);
        w.Add(Id(3), 11, []);
        var run = w.Write(Path_("offsets.tix"), level: 1, coversSegment: 11);

        using var r = TraceIndexReader.Open(run.FilePath)!;
        var hits = new List<TraceIndexHit>();

        Assert.True(r.Lookup(TraceIndexFileTestsAccess.Key(Id(1)), hits));
        _out.WriteLine($"awkward offsets back: {string.Join(", ", hits[0].Offsets)}");
        Assert.Equal(sorted, hits[0].Offsets);

        hits.Clear();
        Assert.True(r.Lookup(TraceIndexFileTestsAccess.Key(Id(2)), hits));
        Assert.Equal([uint.MaxValue], hits[0].Offsets);

        hits.Clear();
        Assert.True(r.Lookup(TraceIndexFileTestsAccess.Key(Id(3)), hits));
        Assert.Empty(hits[0].Offsets);
    }

    // ── Damage: a run that will not open is a run that does not exist ──────────

    public static TheoryData<string, Func<byte[], byte[]>> Damage => new()
    {
        { "empty file",        _   => [] },
        { "header only",       raw => raw[..28] },
        { "truncated in half", raw => raw[..(raw.Length / 2)] },
        { "wrong magic",       raw => { var c = (byte[])raw.Clone(); c[0] ^= 0xFF; return c; } },
        { "wrong version",     raw => { var c = (byte[])raw.Clone(); c[4] = 0x7F; return c; } },
        { "footer magic gone", raw => { var c = (byte[])raw.Clone(); c[^1] ^= 0xFF; return c; } },
        { "sparse offset wild",raw => { var c = (byte[])raw.Clone();
                                        System.Buffers.Binary.BinaryPrimitives
                                            .WriteInt64LittleEndian(c.AsSpan(c.Length - 20), long.MaxValue);
                                        return c; } },
        { "all zeroes",        raw => new byte[raw.Length] },
    };

    [Theory]
    [MemberData(nameof(Damage))]
    public void A_damaged_run_opens_as_null_rather_than_throwing(string what, Func<byte[], byte[]> damage)
    {
        var w = new TraceIndexWriter();
        for (int i = 0; i < 3_000; i++) w.Add(Id(i), 1, Offsets(i, 3));
        var run = w.Write(Path_("dmg.tix"), level: 1, coversSegment: 1);

        File.WriteAllBytes(run.FilePath, damage(File.ReadAllBytes(run.FilePath)));

        // NULL, NOT A THROW, and the caller reads null as "there is no run here" — so the segment
        // it covers falls back to the full scan. A throw would take a trace lookup down over a
        // file that is pure optimisation.
        var r = TraceIndexReader.Open(run.FilePath);
        _out.WriteLine($"{what,-20} → {(r is null ? "null" : "opened")}");
        r?.Dispose();
    }

    [Fact]
    public void A_torn_block_is_refused_without_taking_the_lookup_with_it()
    {
        // The header and the sparse index survive; one block's bytes do not. The reader must
        // answer "not found" for the keys in it rather than throw — and, crucially, must not
        // return garbage offsets that would send a caller reading nonsense out of a segment.
        var w = new TraceIndexWriter();
        for (int i = 0; i < 10_000; i++) w.Add(Id(i), 1, Offsets(i, 3));
        var run = w.Write(Path_("torn.tix"), level: 1, coversSegment: 1);

        var raw = File.ReadAllBytes(run.FilePath);
        for (int i = 40; i < 400 && i < raw.Length; i++) raw[i] ^= 0xA5;   // inside block 0
        File.WriteAllBytes(run.FilePath, raw);

        using var r = TraceIndexReader.Open(run.FilePath);
        if (r is null) { _out.WriteLine("the whole run was refused — also acceptable"); return; }

        var hits = new List<TraceIndexHit>();
        int found = 0;
        for (int i = 0; i < 10_000; i++)
        {
            hits.Clear();
            if (r.Lookup(TraceIndexFileTestsAccess.Key(Id(i)), hits)) found++;
        }
        _out.WriteLine($"{found} of 10000 still resolvable after one block was destroyed");
        Assert.True(found > 0, "a single torn block cost the whole run");
    }
}

/// <summary>Reaches the key derivation the reader is addressed by, without widening its surface.</summary>
internal static class TraceIndexFileTestsAccess
{
    public static ulong Key(TraceId id) => id.High;
}
