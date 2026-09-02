using System.Buffers.Binary;
using Ameto.Tracing;
using Ameto.Tracing.Storage;
using Xunit.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// A segment with ONE BLOCK BIGGER THAN 10 MB — the file the two block-size bounds in
/// <c>SpanReader</c> disagree about, and the reason that disagreement is written down rather
/// than justified away.
///
/// <para>Nothing truncates attribute values on ingest, so a service that puts a 4 KB SQL
/// statement on every span writes a 4096-span block past 10 MB as a matter of course. The search
/// path admits it (its bound is 64 MB — 16 KB per span, corruption rather than data) and
/// <c>ReadAll</c> refuses it, by THROWING out of the middle of a merge. <c>ReadAll</c>'s only
/// caller is compaction, which logs and moves on, so such a file never merges, never migrates to
/// a newer format and never shrinks: it is queryable for ever and compactable never.</para>
///
/// <para>That is the state this test pins. It is not being claimed as a good design — the point
/// is that both halves are deliberate and neither may drift silently. Raise the search bound to
/// 10 MB and the file becomes unqueryable, which is a worse day than a file that will not merge;
/// raise <c>ReadAll</c>'s and compaction starts loading blocks it has no budget for.</para>
/// </summary>
public sealed class SpanSearchFatBlockTests : IDisposable
{
    /// <summary>One full block, so the fat span shape lands in a single block.</summary>
    private const int Spans = 4096;

    /// <summary>Per-span attribute payload. 4096 × ~4 KB clears ReadAll's 10 MB comfortably.</summary>
    private const int StatementBytes = 4000;

    private static readonly DateTimeOffset Base = new(2026, 8, 3, 7, 0, 0, TimeSpan.Zero);
    private static long StartNano(int i) => Base.ToUnixTimeMilliseconds() * 1_000_000L + i * 1_000_000L;

    private static readonly TraceId Wanted = new(0x0FA7B10C_0000_01UL, 0x7);

    private readonly string            _dir;
    private readonly string            _path;
    private readonly ITestOutputHelper _out;

    public SpanSearchFatBlockTests(ITestOutputHelper output)
    {
        _out = output;
        _dir = Path.Combine(Path.GetTempPath(), "ameto-fatblock-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_dir);

        // The statement varies per span so the block cannot compress to nothing — the bound the
        // readers test is the UNCOMPRESSED size, but a fixture whose file is 400 bytes on disk
        // invites the reader to be "fixed" by testing the compressed one instead.
        var corpus = new List<SpanRecord>(Spans);
        for (int i = 0; i < Spans; i++)
            corpus.Add(new SpanRecord
            {
                TraceId           = i == 17 || i == 4095 ? Wanted : new TraceId(0xF00DUL, (ulong)(i + 1)),
                SpanId            = new SpanId((ulong)(i + 1)),
                ParentSpanId      = default,
                StartTimeUnixNano = StartNano(i),
                DurationNanos     = 5_000_000L,
                Name              = "SELECT payments",
                ServiceName       = "billing",
                Kind              = SpanKind.Client,
                Status            = SpanStatusCode.Unset,
                Attributes        = new Dictionary<string, object?>(2, StringComparer.Ordinal)
                {
                    ["db.system"]    = "mssql",
                    ["db.statement"] = Statement(i),
                },
            });

        _path = SpanWriter.Write(_dir, corpus).FilePath;
        corpus.Clear();
    }

    /// <summary>A statement of <see cref="StatementBytes"/> ASCII bytes, different for every span.</summary>
    private static string Statement(int i)
    {
        var sb = new System.Text.StringBuilder(StatementBytes);
        sb.Append("SELECT /* ").Append(i).Append(" */ ");
        int seed = i * 2654435761u.GetHashCode();
        while (sb.Length < StatementBytes)
            sb.Append("col_").Append(unchecked(seed += 7919) & 0xFFFFF).Append(", ");
        return sb.ToString(0, StatementBytes);
    }

    public void Dispose() { try { Directory.Delete(_dir, true); } catch { } }

    /// <summary>Uncompressed size of the first span block, straight out of its length prefix.</summary>
    private uint FirstBlockUncompressedSize()
    {
        using var fs = new FileStream(_path, FileMode.Open, FileAccess.Read, FileShare.Read);
        fs.Seek(27, SeekOrigin.Begin);          // past the 27-byte header
        Span<byte> b = stackalloc byte[4];
        fs.ReadExactly(b);
        return BinaryPrimitives.ReadUInt32LittleEndian(b);
    }

    [Fact]
    public void The_fixture_really_does_write_a_block_over_ten_megabytes()
    {
        uint uncomp = FirstBlockUncompressedSize();
        _out.WriteLine($"block 0 = {uncomp / 1048576.0:N2} MB uncompressed, "
                     + $"file = {new FileInfo(_path).Length / 1048576.0:N2} MB on disk");

        // Everything below is vacuous without this: a fixture that quietly fell under 10 MB would
        // make the search assertion trivial and the compaction assertion fail for the wrong reason.
        Assert.True(uncomp > 10_000_000,
            $"block 0 is only {uncomp:N0} B — the fixture no longer produces the file this test is about");
        Assert.True(uncomp < 64 * 1024 * 1024,
            $"block 0 is {uncomp:N0} B, past the search reader's own bound — this fixture would be refused everywhere");
    }

    [Fact]
    public async Task Search_reads_a_fat_block_rather_than_refusing_the_file()
    {
        int n = 0;
        await foreach (var s in SpanReader.SearchAsync(
                           _path, long.MinValue, long.MaxValue,
                           serviceName: "billing", spanName: null, status: null, httpStatusCode: null,
                           minDurationNanos: null, maxDurationNanos: null, attrHints: null,
                           ct: CancellationToken.None))
        {
            Assert.Equal("mssql", s.Attributes!["db.system"]);
            n++;
        }

        Assert.Equal(Spans, n);
    }

    [Fact]
    public async Task Opening_a_trace_inside_a_fat_block_works_too()
    {
        var got = new List<SpanRecord>();
        await foreach (var s in SpanReader.ReadTraceAsync(_path, Wanted, CancellationToken.None))
            got.Add(s);

        Assert.Equal(2, got.Count);
        Assert.Equal(StartNano(17),   got[0].StartTimeUnixNano);
        Assert.Equal(StartNano(4095), got[1].StartTimeUnixNano);
    }

    [Fact]
    public void Compaction_refuses_the_same_file_by_throwing_out_of_the_middle_of_the_merge()
    {
        // The other half of the asymmetry, asserted so it cannot be described as "compaction
        // already refuses those files" without anyone checking what refusing means. It is not a
        // skip and not a log line at the planner: it is an exception out of ReadAll, which
        // CompactOnePass catches per file — so the segment stays exactly where it is, pass after
        // pass, for as long as it exists.
        var ex = Assert.Throws<InvalidDataException>(() => SpanReader.ReadAll(_path));
        Assert.Contains("too large", ex.Message, StringComparison.OrdinalIgnoreCase);
    }
}

/// <summary>
/// THE CALL SITE THE BOUND WAS NOT APPLIED TO. <c>MaxBlockBytes</c> exists to stop a garbage
/// length prefix being handed straight to <c>ArrayPool.Rent</c>, and every span-block reader tests
/// it — but <c>ReadTraceOffsets</c>, which runs FIRST on every single trace lookup, took both of
/// its lengths straight out of the file untested.
///
/// <para>It degrades rather than crashes, which is why it survived: a prefix past
/// <c>int.MaxValue</c> casts negative and <c>Rent</c> throws <c>ArgumentOutOfRange</c>, caught by
/// the engine and logged as a skipped segment. The values IN BETWEEN are the ones that cost
/// something — a corrupt prefix of a few hundred megabytes is a few hundred megabytes actually
/// rented, per lookup, in the reader whose entire purpose is holding one block at a time.</para>
///
/// <para>Three lengths, three tests, because they fail in three different places: the compressed
/// prefix (which the pre-fix code rented on), the uncompressed prefix (which it read and threw
/// AWAY, so nothing checked it at all), and the size declared INSIDE the LZ4 payload, which no
/// prefix test can see.</para>
/// </summary>
public sealed class TraceIndexLengthPrefixTests : IDisposable
{
    /// <summary><c>SpanReader.MaxBlockBytes</c>, which is private — pinned here on purpose so a
    /// change to it fails this test rather than silently widening what a lookup may rent.</summary>
    private const int MaxBlockBytes = 64 * 1024 * 1024;

    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-trcidx-" + Guid.NewGuid().ToString("N"));

    public TraceIndexLengthPrefixTests() => Directory.CreateDirectory(_dir);
    public void Dispose() { try { Directory.Delete(_dir, true); } catch { } }

    /// <summary>
    /// The smallest v3 file <see cref="SpanReader.ReadTraceAsync"/> will walk into: a 27-byte
    /// header, a trace index written EXACTLY as the caller asks (prefixes included, however
    /// untrue), empty service and bloom indexes, and a valid footer. No span blocks — the lookup
    /// reads the index before it reads anything else, so the fault fires before geometry matters.
    /// </summary>
    private string WriteSegmentWithTraceIndex(string name, uint uncompSize, uint compSize, byte[] payload)
    {
        string path = Path.Combine(_dir, name + ".trc");
        using (var fs = new FileStream(path, FileMode.CreateNew, FileAccess.Write, FileShare.None))
        using (var bw = new BinaryWriter(fs))
        {
            bw.Write(0x52_44_54_43u);          // "RDTC"
            bw.Write((ushort)3);               // version
            bw.Write(0u);                      // span count
            bw.Write(0L);                      // minNano
            bw.Write(0L);                      // maxNano
            bw.Write((byte)0);                 // flags — 27 bytes so far

            long traceIdxOffset = fs.Position;
            bw.Write(uncompSize);
            bw.Write(compSize);
            bw.Write(payload);

            long svcIdxOffset = fs.Position;
            bw.Write(0u);                      // no services

            long bloomIdxOffset = fs.Position;
            bw.Write(0u);                      // no blocks

            bw.Write((ulong)traceIdxOffset);
            bw.Write((ulong)svcIdxOffset);
            bw.Write((ulong)bloomIdxOffset);
            bw.Write(0x52_44_54_46u);          // "RDTF"
        }
        return path;
    }

    /// <summary>A well-formed trace index holding exactly one trace with one offset.</summary>
    private static (uint Uncomp, byte[] Payload) RealIndex(TraceId id)
    {
        var raw = new byte[4 + 16 + 4 + 4];
        BinaryPrimitives.WriteUInt32LittleEndian(raw.AsSpan(0), 1);          // one trace
        id.WriteTo(raw.AsSpan(4, 16));
        BinaryPrimitives.WriteUInt32LittleEndian(raw.AsSpan(20), 1);         // one offset
        BinaryPrimitives.WriteUInt32LittleEndian(raw.AsSpan(24), 0);         // span 0
        return ((uint)raw.Length, K4os.Compression.LZ4.LZ4Pickler.Pickle(raw));
    }

    private static async Task<List<SpanRecord>> ReadTrace(string path, TraceId id)
    {
        var got = new List<SpanRecord>();
        await foreach (var s in SpanReader.ReadTraceAsync(path, id, CancellationToken.None)) got.Add(s);
        return got;
    }

    [Fact]
    public async Task A_compressed_length_past_the_bound_is_refused_before_anything_is_rented()
    {
        // One byte over. Without the test this Rents 64 MB and only then fails, on the SHORT READ
        // rather than on the length — a different exception, after the damage.
        var (uncomp, payload) = RealIndex(new TraceId(1, 2));
        string path = WriteSegmentWithTraceIndex("fat-comp", uncomp, MaxBlockBytes + 1u, payload);

        var ex = await Assert.ThrowsAsync<InvalidDataException>(
            () => ReadTrace(path, new TraceId(1, 2)));
        Assert.Contains("too large", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task An_uncompressed_length_past_the_bound_is_refused_although_nothing_uses_it()
    {
        // The prefix the pre-fix reader discarded outright — `br.ReadUInt32(); // uncompSize`. A
        // file claiming a gigabyte-wide index is corrupt whether or not this particular reader
        // happens to need the number, and letting it through means the ONE bound in the file has
        // an exception nobody wrote down.
        var (_, payload) = RealIndex(new TraceId(3, 4));
        string path = WriteSegmentWithTraceIndex("fat-uncomp", MaxBlockBytes + 1u, (uint)payload.Length, payload);

        var ex = await Assert.ThrowsAsync<InvalidDataException>(
            () => ReadTrace(path, new TraceId(3, 4)));
        Assert.Contains("too large", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task A_payload_that_DECOMPRESSES_past_the_bound_is_refused_too()
    {
        // Neither prefix can catch this one: LZ4 carries the decompressed size inside the payload,
        // so a payload that passes BOTH length tests can still ask for eighty megabytes on the
        // heap. Zeros compress hard, which is exactly the shape a hostile or torn file takes.
        byte[] payload = K4os.Compression.LZ4.LZ4Pickler.Pickle(new byte[80 * 1024 * 1024]);

        // The fixture is only the case under test if it clears the two prefix guards on its own —
        // otherwise this passes on whichever of them fired first and proves nothing about the
        // length that lives inside the payload.
        Assert.True(payload.Length < MaxBlockBytes,
            $"the compressed fixture is {payload.Length:N0} B — the compSize guard would fire first");
        Assert.True(K4os.Compression.LZ4.LZ4Pickler.UnpickledSize(payload) > MaxBlockBytes,
            "the fixture no longer declares a decompressed size past the bound");

        string path = WriteSegmentWithTraceIndex("fat-raw", 1024, (uint)payload.Length, payload);

        var ex = await Assert.ThrowsAsync<InvalidDataException>(
            () => ReadTrace(path, new TraceId(5, 6)));
        Assert.Contains("decompresses to", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task An_ordinary_index_is_still_read()
    {
        // The control, and it is not a formality: a bound that refuses honest files is worse than
        // no bound. The index resolves, the walk finds no block to satisfy offset 0 (the fixture
        // has none), and the lookup ends empty rather than throwing.
        var id = new TraceId(7, 8);
        var (uncomp, payload) = RealIndex(id);
        string path = WriteSegmentWithTraceIndex("ordinary", uncomp, (uint)payload.Length, payload);

        Assert.Empty(await ReadTrace(path, id));
    }
}
