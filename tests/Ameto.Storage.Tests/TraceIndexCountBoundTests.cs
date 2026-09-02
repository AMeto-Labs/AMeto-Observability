using System.Buffers.Binary;
using Ameto.Tracing;
using Ameto.Tracing.Storage;
using Xunit.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// THE LENGTHS THAT ARE NOT <c>Rent</c> ARGUMENTS, and were therefore left without the bound every
/// <c>Rent</c> argument beside them got.
///
/// <para><c>SpanSearchFatBlockTests.TraceIndexLengthPrefixTests</c> pins three: the trace index's
/// compressed prefix, its uncompressed prefix, and the size declared inside its LZ4 payload. All
/// three are handed to <c>ArrayPool.Shared.Rent</c>, which is what got them looked at. Four lines
/// past the last of them, a COUNT out of the same untrusted block is handed to a
/// <c>List&lt;uint&gt;</c> constructor — a capacity, not a rent, and so not covered by the search
/// that found the other three. Two more of the same shape sit one file and one method away.</para>
///
/// <para>WHAT THEY COST, measured on the pre-fix build before any of these tests existed:</para>
/// <list type="bullet">
///   <item>a 91-byte <c>.trc</c> whose 4-byte offset count reads 1 073 741 823 allocated
///   4 295 035 576 bytes and then threw <c>ArgumentOutOfRangeException</c>; at 50 000 000 it
///   allocated 200 082 712. This runs FIRST on every single trace lookup — one click on one row —
///   on the 512 MB server the whole branch exists to keep alive;</item>
///   <item>a 10-byte <c>.stats</c> sidecar whose service count reads 500 000 000 allocated
///   4 000 005 640 bytes and then returned an EMPTY list, because <c>ReadStats</c> ends in
///   <c>catch { return []; }</c>. The sidecar reported "no stats" and the box reported four
///   gigabytes.</item>
/// </list>
///
/// <para>Each bound is exact rather than a guessed ceiling, and each is derived from evidence the
/// file cannot forge: a count of fixed-width records cannot exceed the bytes left to hold them.</para>
/// </summary>
public sealed class TraceIndexCountBoundTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-idxcount-" + Guid.NewGuid().ToString("N"));
    private readonly ITestOutputHelper _out;

    public TraceIndexCountBoundTests(ITestOutputHelper output)
    {
        _out = output;
        Directory.CreateDirectory(_dir);
    }

    public void Dispose() { try { Directory.Delete(_dir, true); } catch { } }

    // ── Fixtures ──────────────────────────────────────────────────────────────

    /// <summary>
    /// The same minimal v3 segment <c>TraceIndexLengthPrefixTests</c> builds: a 27-byte header, a
    /// trace index written exactly as asked, empty service and bloom indexes, a valid footer. No
    /// span blocks — the index is read before anything else, so the fault fires first.
    /// </summary>
    private string WriteV3(string name, uint uncompSize, uint compSize, byte[] payload)
    {
        string path = Path.Combine(_dir, name + ".trc");
        using var fs = new FileStream(path, FileMode.CreateNew, FileAccess.Write, FileShare.None);
        using var bw = new BinaryWriter(fs);

        bw.Write(0x52_44_54_43u);          // "RDTC"
        bw.Write((ushort)3);
        bw.Write(0u);                      // span count
        bw.Write(0L);                      // minNano
        bw.Write(0L);                      // maxNano
        bw.Write((byte)0);                 // flags — 27 bytes

        long traceIdx = fs.Position;
        bw.Write(uncompSize);
        bw.Write(compSize);
        bw.Write(payload);

        long svcIdx = fs.Position;
        bw.Write(0u);
        long bloomIdx = fs.Position;
        bw.Write(0u);

        bw.Write((ulong)traceIdx);
        bw.Write((ulong)svcIdx);
        bw.Write((ulong)bloomIdx);
        bw.Write(0x52_44_54_46u);          // "RDTF"
        return path;
    }

    /// <summary>
    /// A v3 index holding one trace whose OFFSET COUNT is whatever the caller says — the four bytes
    /// at payload offset 20, which is the field a torn write lands on.
    /// </summary>
    private string V3WithOffsetCount(string name, TraceId id, uint offsetCnt, int realOffsets = 1)
    {
        var raw = new byte[4 + 16 + 4 + 4 * realOffsets];
        BinaryPrimitives.WriteUInt32LittleEndian(raw.AsSpan(0), 1);              // one trace
        id.WriteTo(raw.AsSpan(4, 16));
        BinaryPrimitives.WriteUInt32LittleEndian(raw.AsSpan(20), offsetCnt);     // ← the torn field
        for (int i = 0; i < realOffsets; i++)
            BinaryPrimitives.WriteUInt32LittleEndian(raw.AsSpan(24 + 4 * i), (uint)i);

        var payload = K4os.Compression.LZ4.LZ4Pickler.Pickle(raw);
        return WriteV3(name, (uint)raw.Length, (uint)payload.Length, payload);
    }

    /// <summary>The legacy v2 shape: 20-byte footer, and a trace index that is NOT compressed.</summary>
    private string V2WithOffsetCount(string name, TraceId id, uint offsetCnt)
    {
        string path = Path.Combine(_dir, name + ".trc");
        using var fs = new FileStream(path, FileMode.CreateNew, FileAccess.Write, FileShare.None);
        using var bw = new BinaryWriter(fs);

        bw.Write(0x52_44_54_43u);
        bw.Write((ushort)2);               // ← v2
        bw.Write(0u);
        bw.Write(0L);
        bw.Write(0L);
        bw.Write((byte)0);

        long traceIdx = fs.Position;
        bw.Write(1u);                      // one trace
        Span<byte> idBytes = stackalloc byte[16];
        id.WriteTo(idBytes);
        bw.Write(idBytes);
        bw.Write(offsetCnt);               // ← the torn field
        bw.Write(0u);                      // one real offset

        long svcIdx = fs.Position;
        bw.Write(0u);

        bw.Write((ulong)traceIdx);
        bw.Write((ulong)svcIdx);
        bw.Write(0x52_44_54_46u);
        return path;
    }

    private static async Task<(long Allocated, string Outcome)> MeasureTraceRead(string path, TraceId id)
    {
        long before = GC.GetTotalAllocatedBytes(precise: true);
        string outcome = "read to the end";
        try
        {
            await foreach (var _ in SpanReader.ReadTraceAsync(path, id, CancellationToken.None)) { }
        }
        catch (Exception ex) { outcome = ex.GetType().Name; }
        return (GC.GetTotalAllocatedBytes(precise: true) - before, outcome);
    }

    // ── The trace index's offset count ────────────────────────────────────────

    /// <summary>
    /// 4 MB is a ceiling with nothing real anywhere near it — the honest cost of this lookup is a
    /// few tens of kilobytes — and it is three orders of magnitude below the smaller of the two
    /// measured failures, so it cannot be met by an implementation that merely allocates less.
    /// </summary>
    private const long BoundedBytes = 4L * 1024 * 1024;

    [Theory]
    [InlineData(1_073_741_823u)]   // measured pre-fix: 4 295 035 576 B from a 91-byte file
    [InlineData(50_000_000u)]      // measured pre-fix:   200 082 712 B from a 91-byte file
    [InlineData(uint.MaxValue)]
    public async Task An_offset_count_past_the_index_block_is_refused_before_a_list_is_sized(uint cnt)
    {
        var id = new TraceId(1, 2);
        string path = V3WithOffsetCount("cnt-" + cnt, id, cnt);
        long fileLen = new FileInfo(path).Length;

        var (allocated, outcome) = await MeasureTraceRead(path, id);
        _out.WriteLine($"offsetCnt={cnt:N0}  file={fileLen} B  allocated={allocated:N0} B  then {outcome}");

        // The bound is what this test is for; the exception type is how the caller learns about it.
        Assert.True(allocated < BoundedBytes,
            $"a {fileLen}-byte file with a torn offset count allocated {allocated:N0} bytes — "
          + "the count is still reaching the List constructor unbounded");
        Assert.Equal(nameof(InvalidDataException), outcome);
    }

    [Fact]
    public async Task The_v2_index_bounds_its_offset_count_against_the_file_it_is_in()
    {
        // The same defect four lines further down the same method, on the uncompressed legacy
        // index. Its bound is the bytes left in the FILE rather than in a decompressed block, and
        // leaving it out would keep the hole open for every install still carrying v2 segments.
        var id = new TraceId(9, 9);
        string path = V2WithOffsetCount("v2-cnt", id, 900_000_000u);
        long fileLen = new FileInfo(path).Length;

        var (allocated, outcome) = await MeasureTraceRead(path, id);
        _out.WriteLine($"v2 offsetCnt=900,000,000  file={fileLen} B  allocated={allocated:N0} B  then {outcome}");

        Assert.True(allocated < BoundedBytes,
            $"a {fileLen}-byte v2 file allocated {allocated:N0} bytes on a torn offset count");
        Assert.Equal(nameof(InvalidDataException), outcome);
    }

    [Fact]
    public async Task An_honest_index_is_still_read_out_in_full()
    {
        // The control. A bound that refuses real files is worse than no bound, and the count here
        // sits exactly at what the block can hold — the boundary the guard must admit, not refuse.
        var id = new TraceId(4, 4);
        string path = V3WithOffsetCount("honest", id, 3, realOffsets: 3);

        var (_, outcome) = await MeasureTraceRead(path, id);
        Assert.Equal("read to the end", outcome);   // no span blocks to satisfy them — but no throw
    }

    // ── The stats sidecar's service count ─────────────────────────────────────

    [Fact]
    public void A_stats_service_count_past_the_sidecar_is_refused_before_a_list_is_sized()
    {
        // Same shape, one file over, and it degrades even more quietly than the index: ReadStats
        // ends in `catch { return []; }`, so the four gigabytes were spent and then reported as
        // "this segment has no per-service stats".
        string trc = Path.Combine(_dir, "statsbound.trc");
        File.WriteAllBytes(trc, [0]);

        string stats = Path.ChangeExtension(trc, ".stats");
        using (var fs = new FileStream(stats, FileMode.CreateNew, FileAccess.Write, FileShare.None))
        using (var bw = new BinaryWriter(fs))
        {
            bw.Write(0x52_44_54_53u);      // "RDTS"
            bw.Write((ushort)1);           // version
            bw.Write(500_000_000u);        // ← the torn count; no entries follow it
        }

        long before = GC.GetTotalAllocatedBytes(precise: true);
        var rows = SpanReader.ReadStats(trc);
        long allocated = GC.GetTotalAllocatedBytes(precise: true) - before;

        _out.WriteLine($"stats count=500,000,000  file={new FileInfo(stats).Length} B  "
                     + $"allocated={allocated:N0} B  rows={rows.Count}");

        Assert.True(allocated < BoundedBytes,
            $"a 10-byte .stats sidecar allocated {allocated:N0} bytes before deciding it was corrupt");
        Assert.Empty(rows);
    }

    [Fact]
    public void An_honest_stats_sidecar_is_still_read()
    {
        // The control for the stats bound: a real sidecar written by the real writer, whose count
        // the guard has to admit.
        var corpus = new List<SpanRecord>
        {
            new()
            {
                TraceId = new TraceId(7, 7), SpanId = new SpanId(1), ParentSpanId = default,
                StartTimeUnixNano = new DateTimeOffset(2026, 8, 3, 7, 0, 0, TimeSpan.Zero).ToUnixTimeMilliseconds() * 1_000_000L,
                DurationNanos = 5_000_000L, Name = "GET /orders", ServiceName = "billing",
                Kind = SpanKind.Server, Status = SpanStatusCode.Ok,
            },
        };
        string path = SpanWriter.Write(_dir, corpus).FilePath;

        var rows = SpanReader.ReadStats(path);
        Assert.Single(rows);
        Assert.Equal("billing", rows[0].ServiceName);
    }

    // ── The walk's own pre-allocation ─────────────────────────────────────────

    [Fact]
    public async Task A_huge_but_LEGAL_offset_count_does_not_size_a_record_list_by_it()
    {
        // The third site, and the one the first fix does not cover. With the offset count bounded
        // against the index block, a file may still legally declare 16 777 216 offsets — a 64 MB
        // index is inside MaxBlockBytes — and `new List<SpanRecord>(offsets.Count)` then sized a
        // reference array by it, 8 bytes per offset, on top of the offsets themselves.
        //
        // Four million offsets keeps the fixture cheap (16 MB of zeros, a few hundred KB pickled)
        // and the difference unmistakable: 32 MB of SpanRecord references that the walk cannot
        // possibly fill, since the file has no span blocks at all.
        const int Offsets = 4_000_000;

        var id  = new TraceId(5, 5);
        var raw = new byte[4 + 16 + 4 + 4 * Offsets];
        BinaryPrimitives.WriteUInt32LittleEndian(raw.AsSpan(0), 1);
        id.WriteTo(raw.AsSpan(4, 16));
        BinaryPrimitives.WriteUInt32LittleEndian(raw.AsSpan(20), Offsets);
        // The offsets themselves stay zero — every one of them points at span 0, which is legal
        // and, in a file with no blocks, resolves to nothing.

        var payload = K4os.Compression.LZ4.LZ4Pickler.Pickle(raw);
        string path = WriteV3("legal-huge", (uint)raw.Length, (uint)payload.Length, payload);

        _out.WriteLine($"index: {Offsets:N0} offsets, {raw.Length:N0} B raw, {payload.Length:N0} B pickled, "
                     + $"file {new FileInfo(path).Length:N0} B");

        var (allocated, outcome) = await MeasureTraceRead(path, id);
        _out.WriteLine($"allocated={allocated:N0} B  then {outcome}");

        // THE BUDGET IS MEASURED, NOT WRITTEN DOWN, and that is the whole point of these three
        // lines. A fixed megabyte constant here was calibrated on an optimised build and went red
        // in Debug — which is what `dotnet test` builds by default, so CI and every reviewer saw
        // it fail: the same walk costs about 16 extra bytes per offset under <Optimize>false</
        // Optimize>, from the span slice in the offset loop. That is a property of the compiler,
        // not of the bound under test, so the assertion cannot be phrased in absolute bytes.
        //
        // The honest cost this file legitimately describes is the raw index block plus one uint
        // per offset. What must NOT be there is a THIRD array sized by the same count — 8 bytes an
        // offset of SpanRecord references the walk cannot possibly fill, since the file has no
        // span blocks at all. Half of that is the smallest thing worth failing on, and it is far
        // above the Debug/Release spread.
        // A CLAMP HERE WAS TRIED AND MADE IT WORSE, which is worth writing down because the idea
        // is an obvious one. Preallocating the offset list by a count that is already bounded
        // against the bytes left in the block cannot over-reserve: sixteen million declared
        // offsets means sixty-four megabytes of offsets really are in the file, so the list fills
        // either way. Capping the capacity only replaces one right-sized array with a doubling
        // chain — measured on this fixture, 32 MB of intermediate arrays turned 48 MB into 69 MB.
        // The residual the clamp was reaching for (a legal 64 MB index costing ~400 MB, eight ways
        // in GetTraceAsync's fan-out) is the size of MaxBlockBytes, not of this line.
        long honest    = raw.Length + 4L * Offsets;
        long refArray  = 8L * Offsets;
        long budget    = honest + refArray / 2;
        _out.WriteLine($"honest≈{honest:N0} B  budget={budget:N0} B  measured={allocated:N0} B");

        Assert.True(allocated < budget,
            $"a lookup on an index of {Offsets:N0} offsets allocated {allocated:N0} bytes against a "
          + $"budget of {budget:N0} — the record list is still being sized by the offset count");
    }
}
