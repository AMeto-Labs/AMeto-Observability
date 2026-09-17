using System.Globalization;
using Ameto.Core;
using Ameto.Ingestion;
using Xunit;
using Xunit.Abstractions;

namespace Ameto.Perf;

/// <summary>
/// The ingest payload arena is sized to the back-pressure ceiling — 512 MB by default — on the
/// stated assumption that it is reserved address space whose pages fault in on demand. That held
/// on Linux, where a large NativeMemory.Alloc is an anonymous mapping, and not on Windows, where
/// a block that size is VirtualAlloc(MEM_COMMIT): the server took the whole commit charge at
/// startup, before a single event had arrived.
///
/// <para>These pin the property that replaces it — the arena is paid for as the buffer grows into
/// it — and, more importantly, that payloads still round-trip byte for byte, because a slab handed
/// out before its pages exist is an access violation, not a slow path.</para>
/// </summary>
public sealed class SlabArenaCommitTests
{
    private const long MB = 1024 * 1024;

    private readonly ITestOutputHelper _out;
    public SlabArenaCommitTests(ITestOutputHelper output) => _out = output;

    private static byte[] Payload(int len, byte seed)
    {
        var p = new byte[len];
        for (int i = 0; i < len; i++) p[i] = (byte)(seed + i);
        return p;
    }

    private static bool Enqueue(IngestionRingBuffer ring, byte[] payload) =>
        ring.TryEnqueue(DateTimeOffset.UtcNow.UtcTicks, (byte)LogLevel.Information, 0, "t", null, payload);

    /// <summary>Drains everything and returns the payloads in order.</summary>
    private static List<byte[]> DrainAll(IngestionRingBuffer ring, int maxPayload)
    {
        var got = new List<byte[]>();
        var buf = new byte[maxPayload];
        while (ring.TryDequeue(out _, out _, out _, out _, out _, buf, out int len,
                               out _, out _, out _, out _))
            got.Add(buf.AsSpan(0, len).ToArray());
        return got;
    }

    [Fact]
    public void A_fresh_arena_commits_nothing()
    {
        using var ring = new IngestionRingBuffer(1 << 12, 64 * 1024, 256 * MB);
        _out.WriteLine($"commit-on-demand={ring.ArenaCommitsOnDemand} committed={ring.ArenaCommittedBytes:N0} B " +
                       $"slabs={ring.SlabCapacity}");

        Assert.True(ring.SlabCapacity > 1000, "the arena must still OFFER its full capacity");
        if (!ring.ArenaCommitsOnDemand) return;   // platforms where the pages were already lazy

        Assert.Equal(0, ring.ArenaCommittedBytes);
    }

    [Fact]
    public void Commit_tracks_the_high_water_mark_not_the_ceiling()
    {
        using var ring = new IngestionRingBuffer(1 << 12, 64 * 1024, 256 * MB);
        long ceiling = (long)ring.SlabCapacity * 64 * 1024;

        for (int i = 0; i < 64; i++) Assert.True(Enqueue(ring, Payload(100, (byte)i)));
        _out.WriteLine($"64 small events: committed {ring.ArenaCommittedBytes:N0} B of a {ceiling:N0} B ceiling");

        if (!ring.ArenaCommitsOnDemand) return;

        Assert.True(ring.ArenaCommittedBytes > 0, "64 enqueued payloads must have committed something");
        Assert.True(ring.ArenaCommittedBytes < ceiling / 4,
            $"committed {ring.ArenaCommittedBytes} of {ceiling} for 64 small events");
    }

    /// <summary>
    /// The failure this change could introduce is a slab handed out before its pages exist, so
    /// the bytes have to come back exactly — across enough events to cross several commit chunks.
    /// </summary>
    [Fact]
    public void Payloads_round_trip_across_many_commit_chunks()
    {
        using var ring = new IngestionRingBuffer(1 << 12, 64 * 1024, 256 * MB);

        const int n = 600;
        for (int i = 0; i < n; i++)
            Assert.True(Enqueue(ring, Payload(1024, (byte)i)), $"enqueue {i} was refused");

        var got = DrainAll(ring, 64 * 1024);
        Assert.Equal(n, got.Count);
        for (int i = 0; i < n; i++) Assert.Equal(Payload(1024, (byte)i), got[i]);
    }

    /// <summary>
    /// A LIFO free list re-hands the slabs it already committed, so draining and refilling must
    /// not walk any deeper into the arena.
    /// </summary>
    [Fact]
    public void Reuse_does_not_grow_the_commit()
    {
        using var ring = new IngestionRingBuffer(1 << 12, 64 * 1024, 256 * MB);

        for (int i = 0; i < 200; i++) Enqueue(ring, Payload(512, (byte)i));
        DrainAll(ring, 64 * 1024);
        long afterFirstBurst = ring.ArenaCommittedBytes;

        for (int round = 0; round < 10; round++)
        {
            for (int i = 0; i < 200; i++) Enqueue(ring, Payload(512, (byte)i));
            DrainAll(ring, 64 * 1024);
        }

        Assert.Equal(afterFirstBurst, ring.ArenaCommittedBytes);
    }

    /// <summary>
    /// Every slab is still reachable and back-pressure still arrives where it did: the
    /// reservation must bound nothing the committed allocation did not.
    /// </summary>
    [Fact]
    public void The_whole_arena_is_still_usable_and_back_pressure_is_unchanged()
    {
        using var ring = new IngestionRingBuffer(1 << 12, 64 * 1024, 16L * 64 * 1024);
        Assert.Equal(16, ring.SlabCapacity);

        for (int i = 0; i < 16; i++)
            Assert.True(Enqueue(ring, Payload(64 * 1024, (byte)i)), $"slab {i} should be available");

        Assert.False(Enqueue(ring, Payload(64 * 1024, 99)));     // no slab left: refused, not crashed
        Assert.Equal(1, ring.DroppedNoSlab);

        var got = DrainAll(ring, 64 * 1024);
        Assert.Equal(16, got.Count);
        for (int i = 0; i < 16; i++) Assert.Equal(Payload(64 * 1024, (byte)i), got[i]);
    }

    /// <summary>
    /// Concurrent producers cross the commit boundary together — the growth path is the only
    /// place in this buffer that takes a lock, and it must be idempotent under a race.
    /// </summary>
    [Fact]
    public void Concurrent_producers_crossing_a_commit_boundary_lose_nothing()
    {
        using var ring = new IngestionRingBuffer(1 << 14, 64 * 1024, 256 * MB);

        const int threads = 8, each = 200;
        var start = new Barrier(threads);
        int accepted = 0;
        var workers = new Thread[threads];
        for (int t = 0; t < threads; t++)
        {
            int id = t;
            workers[t] = new Thread(() =>
            {
                var payload = Payload(4096, (byte)id);
                start.SignalAndWait();
                for (int i = 0; i < each; i++)
                    if (Enqueue(ring, payload)) Interlocked.Increment(ref accepted);
            });
            workers[t].Start();
        }
        foreach (var w in workers) w.Join();

        var got = DrainAll(ring, 64 * 1024);
        _out.WriteLine($"{threads}x{each}: accepted {accepted}, drained {got.Count}, " +
                       $"committed {ring.ArenaCommittedBytes:N0} B");

        Assert.Equal(accepted, got.Count);
        foreach (var p in got)
        {
            Assert.Equal(4096, p.Length);
            Assert.Equal(Payload(4096, p[0]), p);        // the seed is the first byte
        }
    }

    // ── Transparent huge pages (Linux) ─────────────────────────────────────────

    /// <summary>
    /// madvise takes a page-aligned start and NativeMemory.Alloc promises none, so the Linux
    /// arena is advised over the whole pages inside it: start rounded up, end rounded down, and
    /// nothing at all when no whole page fits. Pure arithmetic, so it runs everywhere.
    /// </summary>
    [Fact]
    public void The_huge_page_advice_covers_only_whole_pages_inside_the_allocation()
    {
        const ulong Page = 4096;

        static (ulong Start, ulong Length) Align(ulong address, ulong bytes, ulong page)
        {
            var (s, l) = SlabArena.PageAlignInward((nuint)address, (nuint)bytes, (nuint)page);
            return (s, l);
        }

        // Exact multiples: the range is its own answer.
        Assert.Equal((2 * Page, 3 * Page), Align(2 * Page, 3 * Page, Page));

        // A start 16 bytes past a boundary (glibc's mmap'd chunk): the partial first and last
        // pages are left out.
        Assert.Equal((4 * Page, 9 * Page), Align(3 * Page + 16, 10 * Page, Page));

        // An aligned start with a ragged end: the end is rounded down.
        Assert.Equal((8 * Page, 2 * Page), Align(8 * Page, 2 * Page + 100, Page));

        // Shorter than a page, aligned or not, or straddling a boundary without a whole page.
        Assert.Equal((0UL, 0UL), Align(2 * Page, Page - 1, Page));
        Assert.Equal((0UL, 0UL), Align(Page + 100, 1000, Page));
        Assert.Equal((0UL, 0UL), Align(Page + 4000, 200, Page));
        Assert.Equal((0UL, 0UL), Align(Page + 1, Page, Page));   // a page long, but no page inside

        // Nothing, and a range that would wrap the address space.
        Assert.Equal((0UL, 0UL), Align(2 * Page, 0, Page));
        if (IntPtr.Size == 8)
            Assert.Equal((0UL, 0UL), Align(ulong.MaxValue - 10, 100, Page));

        // A 16 KB page (some arm64 kernels).
        Assert.Equal((3 * 16384UL, 2 * 16384UL), Align(2 * 16384 + 1, 3 * 16384, 16384));
    }

    /// <summary>
    /// With transparent_hugepage=always the first write in a 2 MB-aligned range can take a whole
    /// huge page, so a burst of small events touching every range could make most of the arena
    /// resident. The Linux arena is advised MADV_NOHUGEPAGE at creation; this checks the advice
    /// was accepted, and that it was THAT advice: the kernel marks the arena's mapping <c>nh</c>
    /// in /proc/self/smaps. Linux-only; returns early elsewhere, and on a kernel built without
    /// THP, where madvise answers EINVAL and there is nothing to opt out of.
    ///
    /// <para>AN EARLY RETURN IS REPORTED AS PASSED, so on its own this test cannot say whether it
    /// checked anything — and the backend CI job is Windows, where it never does. The Linux job in
    /// .github/workflows/tests.yml exists to run it for real and sets <see cref="RequireHugePageCheck"/>:
    /// there, every road that stands down without checking the advice (not Linux, a kernel without
    /// THP, no smaps to confirm which advice) fails instead, so a runner image that changes under
    /// the job turns it red rather than quietly green.</para>
    /// </summary>
    [Fact]
    public unsafe void On_Linux_the_arena_is_opted_out_of_transparent_huge_pages()
    {
        if (!OperatingSystem.IsLinux())
        {
            NotChecked("not Linux");

            // Nothing is advised elsewhere, and nothing claims it was.
            using var plain = SlabArena.Create((nuint)(8 * MB), (nuint)MB, reserve: false);
            Assert.False(plain.HugePagesDisabled);
            Assert.Equal(SlabArena.NoHugePageOptOut, plain.HugePageOptOutErrno);
            return;
        }
        if (!Directory.Exists("/sys/kernel/mm/transparent_hugepage"))
        {
            NotChecked("this kernel has no transparent huge pages (/sys/kernel/mm/transparent_hugepage is absent)");
            return;
        }

        using var arena = SlabArena.Create((nuint)(64 * MB), (nuint)MB);
        _out.WriteLine($"madvise result {arena.HugePageOptOutErrno}");
        Assert.False(arena.IsCommitOnDemand);
        Assert.True(arena.HugePagesDisabled, $"madvise(MADV_NOHUGEPAGE) failed: {arena.HugePageOptOutErrno}");

        // madvise answering 0 says only that SOME advice was taken: MADV_HUGEPAGE (14) answers 0
        // as well, and would make the arena huge-page-eager instead. The kernel's record of which
        // advice is the mapping's VmFlags: nh for MADV_NOHUGEPAGE, hg for MADV_HUGEPAGE. Looked up
        // at the first and last whole pages of the arena, the range the advice covers, not at
        // Base: NativeMemory.Alloc starts a few bytes into a page, and madvise splits that partial
        // page off into a mapping of its own that is not advised.
        string? smaps = ReadSmaps();
        if (smaps is null)
        {
            NotChecked("/proc/self/smaps unavailable: which advice took effect is not checked");
        }
        else
        {
            nuint @base = (nuint)arena.Base;
            var (start, length) = SlabArena.PageAlignInward(@base, (nuint)(64 * MB), (nuint)Environment.SystemPageSize);
            Assert.True(length > 0, "setup: a 64 MB arena holds whole pages");
            _out.WriteLine($"VmFlags at Base 0x{(ulong)@base:x} (not asserted): {VmFlagsAt(smaps, @base)}");

            foreach (ulong address in new[] { (ulong)start, (ulong)(start + length - 1) })
            {
                string? flags = VmFlagsAt(smaps, address);
                _out.WriteLine($"VmFlags at 0x{address:x}: {flags}");
                Assert.NotNull(flags);
                Assert.Contains("nh", flags.Split(' ', StringSplitOptions.RemoveEmptyEntries));
            }
        }

        using var ring = new IngestionRingBuffer(1 << 12, 64 * 1024, 64 * MB);
        Assert.Equal(0, ring.ArenaHugePageOptOutErrno);
    }

    /// <summary>
    /// <c>AMETO_REQUIRE_THP=1</c>: this run exists to check the Linux huge-page opt-out, so a road
    /// that does not check it is a failure, not a pass. Set by the Linux CI job only; unset, the test
    /// stands down where there is nothing to check, as it always has.
    /// </summary>
    private static bool RequireHugePageCheck => Environment.GetEnvironmentVariable("AMETO_REQUIRE_THP") == "1";

    /// <summary>Says why the huge-page check was not made, and fails when <see cref="RequireHugePageCheck"/> is set.</summary>
    private void NotChecked(string why)
    {
        _out.WriteLine($"huge-page opt-out not checked: {why}");
        if (RequireHugePageCheck)
            Assert.Fail($"AMETO_REQUIRE_THP=1 but the huge-page opt-out was not checked: {why}");
    }

    /// <summary>
    /// Finding a mapping's flags by address in smaps text is plain parsing, so it is checked on
    /// every platform against a canned snippet: inside a range, outside every range, and on a
    /// boundary between two adjacent mappings (a range's end is exclusive).
    /// </summary>
    [Fact]
    public void The_smaps_lookup_finds_the_flags_of_the_mapping_that_contains_an_address()
    {
        const string smaps = """
            00400000-00452000 r-xp 00000000 08:02 173521                     /usr/bin/dbus-daemon
            Size:                328 kB
            KernelPageSize:        4 kB
            VmFlags: rd ex mr mw me dw
            7f0000000000-7f0000001000 rw-p 00000000 00:00 0
            Size:                  4 kB
            VmFlags: rd wr mr mw me ac
            7f0000001000-7f0004000000 rw-p 00000000 00:00 0
            Size:              65532 kB
            AnonHugePages:         0 kB
            THPeligible:    0
            VmFlags: rd wr mr mw me ac nh
            7f0004000000-7f0004001000 rw-p 00000000 00:00 0
            Size:                  4 kB
            VmFlags: rd wr mr mw me ac
            7ffc1a2b3000-7ffc1a2d4000 rw-p 00000000 00:00 0                  [stack]
            Size:                132 kB
            VmFlags: rd wr mr mw me gd ac
            """;

        // Inside a range.
        Assert.Equal("rd ex mr mw me dw",       VmFlagsAt(smaps, 0x00400010));
        Assert.Equal("rd wr mr mw me ac nh",    VmFlagsAt(smaps, 0x7f0002345678));
        Assert.Equal("rd wr mr mw me gd ac",    VmFlagsAt(smaps, 0x7ffc1a2d3fff));

        // Outside every range: below the first, in a gap, past the last.
        Assert.Null(VmFlagsAt(smaps, 0x1000));
        Assert.Null(VmFlagsAt(smaps, 0x00452000));   // the first range's end
        Assert.Null(VmFlagsAt(smaps, 0x7f0005000000));
        Assert.Null(VmFlagsAt(smaps, ulong.MaxValue));

        // On boundaries between adjacent mappings: the start belongs to the range, the end does not.
        Assert.Equal("rd wr mr mw me ac",       VmFlagsAt(smaps, 0x7f0000000fff));
        Assert.Equal("rd wr mr mw me ac nh",    VmFlagsAt(smaps, 0x7f0000001000));
        Assert.Equal("rd wr mr mw me ac nh",    VmFlagsAt(smaps, 0x7f0003ffffff));
        Assert.Equal("rd wr mr mw me ac",       VmFlagsAt(smaps, 0x7f0004000000));
        Assert.Null(VmFlagsAt(smaps, 0x7ffc1a2d4000));   // the last range's end
    }

    private static string? ReadSmaps()
    {
        try { return File.ReadAllText("/proc/self/smaps"); }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException) { return null; }
    }

    /// <summary>
    /// The VmFlags of the mapping in <paramref name="smaps"/> (/proc/[pid]/smaps text) whose range
    /// contains <paramref name="address"/>, or null when no mapping does. Each mapping opens with a
    /// header line <c>start-end perms offset dev inode [path]</c>, start and end in hex and end
    /// exclusive, followed by its <c>Name: value</c> fields, VmFlags among them.
    /// </summary>
    internal static string? VmFlagsAt(string smaps, ulong address)
    {
        bool inside = false;
        foreach (ReadOnlySpan<char> line in smaps.AsSpan().EnumerateLines())
        {
            if (TryParseMappingHeader(line, out ulong start, out ulong end))
                inside = address >= start && address < end;
            else if (inside && line.StartsWith("VmFlags:", StringComparison.Ordinal))
                return line["VmFlags:".Length..].Trim().ToString();
        }
        return null;
    }

    /// <summary>A header's first token is <c>start-end</c> in hex; a field line's is <c>Name:</c>.</summary>
    private static bool TryParseMappingHeader(ReadOnlySpan<char> line, out ulong start, out ulong end)
    {
        start = end = 0;
        int space = line.IndexOf(' ');
        ReadOnlySpan<char> range = space < 0 ? line : line[..space];
        int dash = range.IndexOf('-');
        return dash > 0
            && ulong.TryParse(range[..dash], NumberStyles.AllowHexSpecifier, CultureInfo.InvariantCulture, out start)
            && ulong.TryParse(range[(dash + 1)..], NumberStyles.AllowHexSpecifier, CultureInfo.InvariantCulture, out end);
    }
}
