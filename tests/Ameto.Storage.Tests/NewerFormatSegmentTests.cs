using System.Buffers.Binary;
using Microsoft.Extensions.Logging.Abstractions;
using Ameto.Tracing;
using Ameto.Tracing.Storage;
using Xunit.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// A SEGMENT THIS BUILD CANNOT PARSE IS NOT A SEGMENT THIS BUILD MAY DELETE.
///
/// <para>The v4 rollback fix has two halves and only the easy one was pinned. That the default
/// write version is 3 is asserted in two places; that this build LEAVES ALONE a file whose version
/// it does not recognise — <c>SpanReader.LooksLikeNewerFormat</c> plus the first branch of the
/// cold-tier damage classifier — was asserted nowhere at all. The asymmetry is the wrong way
/// round: what was pinned is cheap to check, and what was not costs the data.</para>
///
/// <para>The blast radius is zero today, because this build knows version 4 and so never meets a
/// version it does not know among files it wrote itself. The branch exists for the day there is a
/// v5, and without a test its first real exercise would be in production, on someone's rollback,
/// deleting the segments the newer binary had written. That is precisely the accident the fix was
/// written to prevent — reproduced by the fix's own absence of coverage.</para>
///
/// <para>The classifier's other branches must keep working: a file that is damaged rather than
/// merely newer is still deleted, because a <c>.trc</c> that cannot be read and cannot be
/// explained is dead weight the cold tier has to stop tripping over.</para>
/// </summary>
public sealed class NewerFormatSegmentTests : IDisposable
{
    private const long Ms = 1_000_000L;
    private static readonly DateTimeOffset Base = new(2026, 8, 1, 12, 0, 0, TimeSpan.Zero);

    private readonly string            _root = Path.Combine(Path.GetTempPath(), "ameto-newer-" + Guid.NewGuid().ToString("N"));
    private readonly ITestOutputHelper _out;
    private readonly long              _baseNano = Base.ToUnixTimeMilliseconds() * Ms;

    public NewerFormatSegmentTests(ITestOutputHelper output)
    {
        _out = output;
        Directory.CreateDirectory(_root);
    }

    public void Dispose() { try { Directory.Delete(_root, true); } catch { } }

    private string Dir(string name)
    {
        string d = Path.Combine(_root, name);
        Directory.CreateDirectory(d);
        return d;
    }

    private static TraceId Id(int i) => new(
        unchecked((ulong)(i * 6364136223846793005L + 1442695040888963407L)),
        unchecked((ulong)(i * 2862933555777941757L + 3037000493L)));

    /// <summary>
    /// A real segment with its version bumped past what this build knows — which is exactly what a
    /// future writer leaves behind, and what a rolled-back binary would meet.
    /// </summary>
    private string WriteFromTheFuture(string dir, ushort version = 9)
    {
        var corpus = new List<SpanRecord>(300);
        for (int t = 0; t < 100; t++)
            for (int k = 0; k < 3; k++)
                corpus.Add(new SpanRecord
                {
                    TraceId = Id(t), SpanId = new SpanId((ulong)(t * 3 + k + 1)), ParentSpanId = default,
                    StartTimeUnixNano = _baseNano + (t * 10 + k) * Ms, DurationNanos = 2 * Ms,
                    Name = "GET /orders", ServiceName = "billing",
                    Kind = SpanKind.Server, Status = SpanStatusCode.Ok,
                });

        var info = SpanWriter.Write(dir, corpus);
        string named = Path.Combine(dir, "spans-20260801-120000.trc");
        if (!string.Equals(info.FilePath, named, StringComparison.Ordinal)) File.Move(info.FilePath, named);

        // Everything else about the file stays valid — magic, footer, blocks. Only the version says
        // "written by something you have not met", which is the whole premise.
        byte[] raw = File.ReadAllBytes(named);
        BinaryPrimitives.WriteUInt16LittleEndian(raw.AsSpan(4), version);
        File.WriteAllBytes(named, raw);
        return named;
    }

    [Fact]
    public void A_segment_from_a_newer_build_is_recognised_as_newer_and_not_as_damage()
    {
        string dir  = Dir("unit");
        string path = WriteFromTheFuture(dir);

        Assert.True(SpanReader.LooksLikeNewerFormat(path));
        _out.WriteLine($"version {SpanWriter.NewestVersion + 5} → LooksLikeNewerFormat = true");

        // And it does not fire on anything this build writes, which is what keeps the branch from
        // quietly disabling the deletion of genuinely dead files.
        var ok3 = SpanWriter.Write(Dir("v3ok"), [new SpanRecord
        {
            TraceId = Id(1), SpanId = new SpanId(1), ParentSpanId = default,
            StartTimeUnixNano = _baseNano, DurationNanos = Ms,
            Name = "GET /", ServiceName = "billing", Kind = SpanKind.Server, Status = SpanStatusCode.Ok,
        }]);
        Assert.False(SpanReader.LooksLikeNewerFormat(ok3.FilePath));

        var ok4 = SpanWriter.Write(Dir("v4ok"), [new SpanRecord
        {
            TraceId = Id(2), SpanId = new SpanId(2), ParentSpanId = default,
            StartTimeUnixNano = _baseNano, DurationNanos = Ms,
            Name = "GET /", ServiceName = "billing", Kind = SpanKind.Server, Status = SpanStatusCode.Ok,
        }], version: SpanWriter.NewestVersion);
        Assert.False(SpanReader.LooksLikeNewerFormat(ok4.FilePath),
            "the version this build writes was classified as coming from the future");

        // Not a .trc at all is not "newer" either — it is damage, and must stay classifiable as such.
        string junk = Path.Combine(Dir("junk"), "spans-nonsense.trc");
        File.WriteAllBytes(junk, [.. Enumerable.Repeat((byte)0xEE, 4096)]);
        Assert.False(SpanReader.LooksLikeNewerFormat(junk));
    }

    [Fact]
    public void The_cold_scan_leaves_a_newer_segment_on_disk_and_reports_the_tier_short()
    {
        // THE FINDING, END TO END, and the accident it prevents: roll back one container command
        // the day after an upgrade, and the previous build reads version 4 as corruption, logs
        // "likely format v1", and deletes the .trc with its three sidecars. That day is gone.
        string dir  = Dir("scan");
        string path = WriteFromTheFuture(dir);
        long   size = new FileInfo(path).Length;

        using var e = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance);
        e.LoadColdSegments();

        _out.WriteLine($"after the cold scan: file present {File.Exists(path)}, "
                     + $"tier incomplete {e.ColdTierIncompleteForTest}, "
                     + $"segments {e.ColdSegmentCountForTest}");

        Assert.True(File.Exists(path), "a segment written by a newer build was DELETED");
        Assert.Equal(size, new FileInfo(path).Length);          // and not truncated either

        // It cannot be read, so it must not be silently absent: the tier says it is short, which is
        // what makes a query over that window admit it is incomplete instead of answering fewer
        // spans and looking healthy.
        Assert.True(e.ColdTierIncompleteForTest,
            "the segment is unreadable and the tier reported itself complete");
        Assert.Equal(0, e.ColdSegmentCountForTest);
    }

    [Fact]
    public void A_genuinely_damaged_segment_is_still_deleted()
    {
        // THE GUARD MUST NOT BECOME A BLANKET AMNESTY. The classifier has three branches and only
        // the first is new; this pins that the second still deletes. A file with an intact header
        // and a destroyed body is the v1-migration case the deletion path exists for — its window
        // is recordable, so the loss is reportable, so removing it is honest rather than silent.
        //
        // The version this writes is one this build KNOWS, which is what separates it from the test
        // above: same unreadability, opposite verdict, and the version byte is the only difference.
        string dir  = Dir("damaged");
        string path = WriteFromTheFuture(dir, version: SpanWriter.DefaultVersion);
        byte[] raw  = File.ReadAllBytes(path);
        File.WriteAllBytes(path, raw.AsSpan(0, 40).ToArray());   // header survives, nothing else does

        using var e = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance);
        e.LoadColdSegments();

        _out.WriteLine($"header-only .trc after the cold scan: present {File.Exists(path)}");
        Assert.False(File.Exists(path), "a file that is damaged rather than newer was kept forever");
    }

    [Fact]
    public void The_version_byte_is_the_only_difference_between_kept_and_deleted()
    {
        // The pair, side by side in one catalog, because the whole fix is one branch and the
        // cheapest way to state it is to change nothing but the version and watch the verdict flip.
        string dir  = Dir("pair");
        string keep = WriteFromTheFuture(dir, version: 9);
        string kill = Path.Combine(dir, "spans-20260801-130000.trc");
        File.Copy(keep, kill);

        byte[] raw = File.ReadAllBytes(kill);
        BinaryPrimitives.WriteUInt16LittleEndian(raw.AsSpan(4), SpanWriter.DefaultVersion);
        File.WriteAllBytes(kill, raw.AsSpan(0, 40).ToArray());   // known version, destroyed body

        using var e = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance);
        e.LoadColdSegments();

        _out.WriteLine($"newer kept: {File.Exists(keep)}; damaged deleted: {!File.Exists(kill)}");
        Assert.True(File.Exists(keep));
        Assert.False(File.Exists(kill));
    }
}
