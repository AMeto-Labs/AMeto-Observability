using Microsoft.Extensions.Logging.Abstractions;
using Ameto.Tracing;
using Ameto.Tracing.Storage;

namespace Ameto.Storage.Tests;

/// <summary>
/// SpanWriter's temp-name publish protocol: a flush builds every file of the batch at a
/// .tmp name, fsyncs, and renames — sidecars first, the .trc last — so a crash can never
/// leave a half-written file at a name the background cold scan reads (or, failing on
/// the missing footer, deletes), and the engine constructor sweeps whatever a died
/// flush left behind.
/// </summary>
public sealed class TraceTempFileProtocolTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-trctmp-" + Guid.NewGuid().ToString("N"));
    private readonly List<TraceStorageEngine> _engines = [];

    public TraceTempFileProtocolTests() => Directory.CreateDirectory(_dir);

    public void Dispose()
    {
        foreach (var e in _engines)
            try { e.Dispose(); } catch { }
        try { Directory.Delete(_dir, true); } catch { }
    }

    private static List<SpanRecord> Corpus(int n)
    {
        var spans = new List<SpanRecord>(n);
        long baseNano = 1_756_000_000_000_000_000;
        for (int i = 0; i < n; i++)
        {
            spans.Add(new SpanRecord
            {
                TraceId           = new TraceId(1, (ulong)(i / 4 + 1)),
                SpanId            = new SpanId((ulong)(i + 1)),
                ParentSpanId      = default,
                StartTimeUnixNano = baseNano + i * 1_000_000L,
                DurationNanos     = 5_000_000,
                Name              = "op-" + i % 7,
                ServiceName       = "svc-" + i % 3,
                Kind              = SpanKind.Server,
                Status            = SpanStatusCode.Unset,
                HttpStatusCode    = 200,
            });
        }
        return spans;
    }

    [Fact]
    public void Write_leaves_no_temp_files_and_all_finals_in_place()
    {
        var info = SpanWriter.Write(_dir, Corpus(64));

        Assert.True(File.Exists(info.FilePath));
        Assert.Empty(Directory.GetFiles(_dir, "*.tmp"));
        // Sidecars landed under their final names beside the .trc.
        Assert.True(File.Exists(Path.ChangeExtension(info.FilePath, ".tracesum")));
        Assert.True(File.Exists(Path.ChangeExtension(info.FilePath, ".stats")));
    }

    [Fact]
    public void Constructor_sweeps_dead_flush_residue()
    {
        File.WriteAllBytes(Path.Combine(_dir, "spans-1-2-3-deadbeef.trc.tmp"),      [1, 2, 3]);
        File.WriteAllBytes(Path.Combine(_dir, "spans-1-2-3-deadbeef.stats.tmp"),    [1]);
        File.WriteAllBytes(Path.Combine(_dir, "spans-1-2-3-deadbeef.tracesum.tmp"), [1]);

        var engine = new TraceStorageEngine(_dir, NullLogger<TraceStorageEngine>.Instance);
        _engines.Add(engine);

        Assert.Empty(Directory.GetFiles(_dir, "spans-*.tmp"));
    }

    [Fact]
    public void Constructor_recovers_a_complete_segment_whose_rename_was_lost()
    {
        // A power loss can persist every byte of a flush (each file is fsynced) and still
        // lose the rename itself — on Linux that needs a parent-directory fsync .NET cannot
        // issue. Deleting the temp there would destroy the whole flush, so a temp that
        // PARSES is renamed into place instead.
        var info = SpanWriter.Write(_dir, Corpus(48));
        string baseP = info.FilePath[..^".trc".Length];
        foreach (var ext in new[] { ".stats", ".svcgraph", ".tracesum" })
            if (File.Exists(baseP + ext)) File.Move(baseP + ext, baseP + ext + ".tmp");
        File.Move(info.FilePath, info.FilePath + ".tmp");        // the rename that "did not survive"

        var engine = new TraceStorageEngine(_dir, NullLogger<TraceStorageEngine>.Instance);
        _engines.Add(engine);

        Assert.True(File.Exists(info.FilePath));                 // recovered, not deleted
        Assert.True(File.Exists(baseP + ".tracesum"));           // sidecars came with it
        Assert.Empty(Directory.GetFiles(_dir, "spans-*.tmp"));

        engine.LoadColdSegments();
        Assert.Equal(1, engine.ColdSegmentCountForTest);
    }

    [Fact]
    public void Cold_scan_neither_loads_nor_deletes_an_inflight_temp_file()
    {
        SpanWriter.Write(_dir, Corpus(32)); // one real, complete segment

        var engine = new TraceStorageEngine(_dir, NullLogger<TraceStorageEngine>.Instance);
        _engines.Add(engine);

        // A writer mid-flush, exactly as the background scan can encounter it —
        // planted AFTER the constructor so its sweep (legitimate) doesn't collect it.
        string inflight = Path.Combine(_dir, "spans-9-9-9-cafebabe.trc.tmp");
        File.WriteAllBytes(inflight, [0xDE, 0xAD]);

        engine.LoadColdSegments();

        Assert.Equal(1, engine.ColdSegmentCountForTest);
        Assert.True(File.Exists(inflight)); // the scan must never touch an in-flight temp
    }
}
