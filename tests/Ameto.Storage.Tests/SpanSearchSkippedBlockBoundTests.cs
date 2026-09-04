using System.Buffers.Binary;
using Ameto.Tracing;
using Ameto.Tracing.Storage;
using Xunit.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// THE BLOCK NOBODY READS IS STILL A BLOCK THE WALK STEPS OVER BY A NUMBER FROM THE FILE.
///
/// <para>Both block walks in <c>SpanReader</c> fork on "is anything in this block wanted?" and
/// both exits use the same field: the read RENTS <c>compSize</c> bytes, the skip SEEKS
/// <c>compSize</c> bytes. The bound that this branch added stood after the fork, on the read path
/// only — so a torn length in a block the search FILTERED OUT was believed without question.</para>
///
/// <para>That is not the rare path, it is the main one. <c>allowedBlocks</c> is what the service
/// index and the attribute blooms produce, so on a selective search almost every block is stepped
/// over and almost none are read. And the failure is silent by construction: the seek lands past
/// the end of the file, <c>fs.Position &lt; traceIdxOffset</c> is false, the walk ENDS — no
/// exception, no fault, no floor. The caller gets a short answer indistinguishable from a complete
/// one, which is the exact claim this branch has spent every round closing.</para>
///
/// <para>Measured here, on a two-block segment whose FIRST block is the one being skipped: with the
/// bound back below the fork, the filtered search returns <b>0 of 4096</b> readable spans and
/// reports success.</para>
///
/// <para>THE TRACE WALK IS A CONTROL, NOT A SECOND REGRESSION — said plainly because the two loops
/// look identical and it would be easy to claim both. <c>ReadTraceAsync</c> runs the geometry pass
/// and then, if it did not place every offset the index promised, runs an EXACT walk that reads
/// every block; the truncated seek fails that count test, so the fallback already converted the bad
/// skip into a loud refusal. <see cref="The_trace_walk_is_loud_on_both_sides_of_this_fix"/> is green
/// with the bound in either position, and says so. Moving the bound above the fork there buys
/// something smaller and still worth having: the geometry pass refuses the file itself instead of
/// depending on a second full-file pass to notice.</para>
/// </summary>
public sealed class SpanSearchSkippedBlockBoundTests : IDisposable
{
    /// <summary>Two full blocks. SpanWriter's BlockSize is 4096, so this is exactly blocks 0 and 1.</summary>
    private const int PerBlock = 4096;

    /// <summary>Byte offset of block 0's <c>compSize</c>: 27-byte header, then uncompSize.</summary>
    private const int Block0CompSizeAt = 27 + 4;

    private static readonly DateTimeOffset Base = new(2026, 8, 5, 9, 0, 0, TimeSpan.Zero);
    private static long StartNano(int i) => Base.ToUnixTimeMilliseconds() * 1_000_000L + i * 1_000_000L;

    /// <summary>A trace whose every span is in block 1, so the geometry pass skips block 0.</summary>
    private static readonly TraceId InSecondBlock = new(0x5EC0_0000_0001UL, 0x2);

    private readonly string            _dir;
    private readonly string            _path;
    private readonly ITestOutputHelper _out;

    public SpanSearchSkippedBlockBoundTests(ITestOutputHelper output)
    {
        _out = output;
        _dir = Path.Combine(Path.GetTempPath(), "ameto-skipbound-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_dir);

        // Block 0 is "checkout", block 1 is "billing" — one service per block, so a search for
        // either produces an allowedBlocks set of exactly one and genuinely skips the other. The
        // writer sorts by start time, so the time order below IS the block layout.
        var corpus = new List<SpanRecord>(PerBlock * 2);
        for (int i = 0; i < PerBlock * 2; i++)
        {
            bool second = i >= PerBlock;
            corpus.Add(new SpanRecord
            {
                TraceId           = second && i % 512 == 0
                                        ? InSecondBlock
                                        : new TraceId(0xF00DUL, (ulong)(i + 1)),
                SpanId            = new SpanId((ulong)(i + 1)),
                ParentSpanId      = default,
                StartTimeUnixNano = StartNano(i),
                DurationNanos     = 3_000_000L,
                Name              = "GET /orders",
                ServiceName       = second ? "billing" : "checkout",
                Kind              = SpanKind.Server,
                Status            = SpanStatusCode.Ok,
            });
        }

        _path = SpanWriter.Write(_dir, corpus).FilePath;
    }

    public void Dispose() { try { Directory.Delete(_dir, true); } catch { } }

    /// <summary>One flipped field, in the block that will be skipped rather than read.</summary>
    private void TearBlock0CompSize()
    {
        using var fs = new FileStream(_path, FileMode.Open, FileAccess.ReadWrite);
        fs.Seek(Block0CompSizeAt, SeekOrigin.Begin);
        Span<byte> four = stackalloc byte[4];
        fs.ReadExactly(four);
        uint real = BinaryPrimitives.ReadUInt32LittleEndian(four);

        // The single-bit tear this branch keeps meeting, not an invented giant: setting the top
        // bit of a real length is one flipped byte on the wire or on the platter.
        uint torn = real | 0x4000_0000u;
        BinaryPrimitives.WriteUInt32LittleEndian(four, torn);
        fs.Seek(Block0CompSizeAt, SeekOrigin.Begin);
        fs.Write(four);

        _out.WriteLine($"block 0 compSize {real} -> {torn} (file is {new FileInfo(_path).Length} bytes)");
    }

    private async Task<int> CountBillingAsync() =>
        await CountAsync(SpanReader.SearchAsync(
            _path, long.MinValue, long.MaxValue,
            serviceName: "billing", spanName: null, status: null, httpStatusCode: null,
            minDurationNanos: null, maxDurationNanos: null, attrHints: null, ct: default));

    private static async Task<int> CountAsync(IAsyncEnumerable<SpanRecord> src)
    {
        int n = 0;
        await foreach (var _ in src) n++;
        return n;
    }

    [Fact]
    public async Task The_healthy_file_still_answers_a_filtered_search_from_the_second_block()
    {
        // The control, and it is not a formality: a bound moved above the fork could just as
        // easily refuse every skip. The filtered search must still read block 1 while genuinely
        // stepping over block 0.
        int n = await CountBillingAsync();
        _out.WriteLine($"healthy filtered search: {n} spans");
        Assert.Equal(PerBlock, n);
    }

    [Fact]
    public async Task A_torn_length_in_a_SKIPPED_block_is_refused_not_stepped_over()
    {
        TearBlock0CompSize();

        // Before the bound moved above the fork: 0 spans and no exception — a filtered search
        // that should return 4096 rows returned none and called itself finished.
        var ex = await Assert.ThrowsAnyAsync<Exception>(async () => await CountBillingAsync());
        _out.WriteLine($"{ex.GetType().Name}: {ex.Message}");

        // And it has to be CONTENT-shaped, or the classifier upstream files this as "ask me
        // again" and the engine retries a file that will never be different.
        Assert.True(Ameto.Core.FileBounds.DescribesContent(ex),
            $"a torn block length classified as a transient fault: {ex.GetType().Name}");
    }

    [Fact]
    public async Task The_trace_walk_is_loud_on_both_sides_of_this_fix()
    {
        // THE TWIN LOOP, AND THE HONEST RESULT: this one is green with the bound in either
        // position, and it is here to record that rather than to imply otherwise.
        //
        // The geometry pass steps over block 0 by the same unchecked number and ends early — but
        // ReadTraceAsync then checks that every offset the trace index promised was actually
        // placed, fails that check, and re-runs the walk with useBlockGeometry: false, which reads
        // block 0 and meets the bound on the read path. So the empty-trace answer this test was
        // first written to catch never reaches a caller; the fallback catches it.
        //
        // Moving the bound above the fork here is therefore not a bug fix but a removal of a
        // dependency: the geometry pass stops needing a second full-file pass to discover that the
        // file is torn. Worth doing — the two loops are the same shape and the next reader should
        // not have to work out which one is load-bearing — and worth not overstating.
        int healthy = await CountAsync(SpanReader.ReadTraceAsync(_path, InSecondBlock, default));
        _out.WriteLine($"healthy trace read: {healthy} spans");
        Assert.Equal(PerBlock / 512, healthy);

        TearBlock0CompSize();

        var ex = await Assert.ThrowsAnyAsync<Exception>(async () =>
            await CountAsync(SpanReader.ReadTraceAsync(_path, InSecondBlock, default)));
        _out.WriteLine($"{ex.GetType().Name}: {ex.Message}");
        Assert.True(Ameto.Core.FileBounds.DescribesContent(ex),
            $"a torn block length classified as a transient fault: {ex.GetType().Name}");
    }
}
