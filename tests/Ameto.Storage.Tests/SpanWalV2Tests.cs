using System.Buffers.Binary;
using System.Diagnostics;
using System.Text;
using Ameto.Core;
using Ameto.Tracing;
using Ameto.Tracing.Storage;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// SPAN WAL v2: a CRC32C per entry, and the v1 logs the previous release left behind still replay.
///
/// <para>v1 had no checksum anywhere. The bounds check could say only that the declared lengths
/// FIT, so a torn append — mmap pages reaching the disk in whatever order the OS picks — replayed as
/// a span with garbage name, service and attributes straight into the hot tier. Every fact below is
/// about the first entry that does not verify ENDING the replay, and about the one thing a format
/// change must never do: silently drop the log the process before it acknowledged.</para>
///
/// <para>Also pinned: what a commit forces to disk (a RANGE, the relocated tail with its terminator
/// and the header page — not the whole view), in which order relative to the header stamp, and that
/// the header's drive flush runs with the append lock free. All judged at seams; no timers.</para>
/// </summary>
public sealed class SpanWalV2Tests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-swal2-" + Guid.NewGuid().ToString("N"));

    private readonly ITestOutputHelper _out;

    public SpanWalV2Tests(ITestOutputHelper output)
    {
        _out = output;
        Directory.CreateDirectory(_dir);
    }
    public void Dispose() { try { Directory.Delete(_dir, true); } catch { } }

    private string WalPath => Path.Combine(_dir, "spans.wal");

    private const int  FileHeader   = 32;
    private const int  V1EntryHead  = 64;
    private const int  V2EntryHead  = 68;   // 64 checksummed bytes + the CRC
    private const long BaseNano     = 1_784_800_000_000_000_000L;

    private static SpanIngestItem Item(int i, string? name = null, byte[]? attrs = null) => new()
    {
        TraceId           = new TraceId((ulong)(i + 1) * 0x9E3779B97F4A7C15UL, (ulong)(i + 7)),
        SpanId            = new SpanId((ulong)(i + 100)),
        ParentSpanId      = i == 0 ? default : new SpanId((ulong)(i + 99)),
        StartTimeUnixNano = BaseNano + i,
        DurationNanos     = 1_000_000L + i,
        Name              = name ?? $"GET /api/thing/{i}",
        ServiceName       = i % 2 == 0 ? "MintRoute.API" : "KioskAgent.API",
        Kind              = i % 3 == 0 ? SpanKind.Server : SpanKind.Client,
        Status            = i % 5 == 0 ? SpanStatusCode.Error : SpanStatusCode.Unset,
        HttpStatusCode    = (short)(i % 3 == 0 ? 200 : 0),
        AttributesBytes   = attrs ?? [0x81, 0xA1, (byte)'k', (byte)i],   // {"k": i}
    };

    /// <summary>Appends <paramref name="n"/> spans and returns each entry's FILE offset.</summary>
    private static long[] AppendAll(SpanWriteAheadLog wal, int n)
    {
        var at = new long[n];
        for (int i = 0; i < n; i++)
        {
            at[i] = FileHeader + wal.WrittenBytes;
            wal.Append(Item(i));
        }
        return at;
    }

    private void FlipByte(long fileOffset)
    {
        using var fs = new FileStream(WalPath, FileMode.Open, FileAccess.ReadWrite);
        fs.Position = fileOffset;
        int b = fs.ReadByte();
        fs.Position = fileOffset;
        fs.WriteByte((byte)(b ^ 0x5A));
    }

    private List<SpanIngestItem> Reopen()
    {
        using var wal = SpanWriteAheadLog.Open(WalPath);
        return wal.ReadAll();
    }

    // ── The checksum ─────────────────────────────────────────────────────────

    /// <summary>
    /// A FLIPPED BYTE ANYWHERE IN AN ENTRY ENDS THE REPLAY AT THAT ENTRY. Header field, the CRC
    /// itself, the name, the service, the attributes: each is covered, and the entries before the
    /// damaged one come back intact. Without the CRC check the damaged span replays with the
    /// corruption in it (and everything after it with it) — all five come back.
    /// </summary>
    [Theory]
    [InlineData("header: start time", 32)]
    [InlineData("header: span id",    16)]
    [InlineData("the crc itself",     64)]
    [InlineData("name",               V2EntryHead + 2)]
    [InlineData("service",            -1)]
    [InlineData("attributes",         -2)]
    public void A_flipped_byte_ends_the_replay_at_the_entry_it_is_in(string where, int offsetInEntry)
    {
        long[] at;
        using (var wal = SpanWriteAheadLog.Open(WalPath)) at = AppendAll(wal, 5);

        var damaged = Item(3);
        int nameLen = Encoding.UTF8.GetByteCount(damaged.Name);
        int svcLen  = Encoding.UTF8.GetByteCount(damaged.ServiceName);
        int inEntry = offsetInEntry switch
        {
            -1 => V2EntryHead + nameLen + 1,                 // inside the service
            -2 => V2EntryHead + nameLen + svcLen + 3,        // the attribute value byte
            _  => offsetInEntry,
        };
        FlipByte(at[3] + inEntry);

        var replayed = Reopen();

        Assert.True(3 == replayed.Count, $"{where}: replayed {replayed.Count}, expected the three before the damage");
        Assert.Equal([100UL, 101UL, 102UL], replayed.Select(static s => s.SpanId.RawValue));
        Assert.Equal(Item(2).Name, replayed[2].Name);
    }

    /// <summary>
    /// A TORN TAIL — the last entry's payload page never reached the disk, but the header's offset
    /// did — replays as the entries before it, not as a span of zeroes. And the replay truncates the
    /// log to where it stopped, so the next append overwrites the torn entry instead of landing
    /// behind it, where the following replay could never reach it.
    /// </summary>
    [Fact]
    public void A_torn_last_entry_is_dropped_and_the_next_append_takes_its_place()
    {
        long[] at;
        long end;
        using (var wal = SpanWriteAheadLog.Open(WalPath))
        {
            at  = AppendAll(wal, 5);
            end = FileHeader + wal.WrittenBytes;
        }
        using (var fs = new FileStream(WalPath, FileMode.Open, FileAccess.ReadWrite))
        {
            fs.Position = at[4] + V2EntryHead;               // the payload, not the header
            fs.Write(new byte[end - fs.Position]);           // never written back
        }

        using (var wal = SpanWriteAheadLog.Open(WalPath))
        {
            Assert.Equal(4, wal.ReadAll().Count);
            wal.Append(Item(9));
        }

        var replayed = Reopen();
        Assert.Equal([100UL, 101UL, 102UL, 103UL, 109UL], replayed.Select(static s => s.SpanId.RawValue));
    }

    /// <summary>The UTF-8 overload writes what it is given, byte for byte, and replay decodes it.</summary>
    [Fact]
    public void The_utf8_append_round_trips_its_bytes()
    {
        byte[] name = Encoding.UTF8.GetBytes("платёж → провайдер 🚀");
        byte[] svc  = Encoding.UTF8.GetBytes("MintRoute.API");
        byte[] attr = [0x80];
        using (var wal = SpanWriteAheadLog.Open(WalPath))
            wal.Append(new TraceId(1, 2), new SpanId(3), default, BaseNano, 5, SpanKind.Server,
                       SpanStatusCode.Error, 503, name, svc, attr);

        var s = Assert.Single(Reopen());
        Assert.Equal("платёж → провайдер 🚀", s.Name);
        Assert.Equal("MintRoute.API", s.ServiceName);
        Assert.Equal(attr, s.AttributesBytes);
        Assert.Equal((short)503, s.HttpStatusCode);
        Assert.Equal(new TraceId(1, 2), s.TraceId);
    }

    // ── v1 still replays ─────────────────────────────────────────────────────

    /// <summary>Writes a v1 log by hand: the layout the previous release wrote, no checksums.</summary>
    private void WriteV1Log(uint headerGeneration, params (SpanIngestItem Span, uint Generation)[] entries)
    {
        using var ms = new MemoryStream();
        ms.Write(new byte[FileHeader]);
        foreach (var (s, gen) in entries)
        {
            byte[] name = Encoding.UTF8.GetBytes(s.Name);
            byte[] svc  = Encoding.UTF8.GetBytes(s.ServiceName);
            var h = new byte[V1EntryHead];
            s.TraceId.WriteTo(h.AsSpan(0, 16));
            BinaryPrimitives.WriteUInt64LittleEndian(h.AsSpan(16), s.SpanId.RawValue);
            BinaryPrimitives.WriteUInt64LittleEndian(h.AsSpan(24), s.ParentSpanId.RawValue);
            BinaryPrimitives.WriteInt64LittleEndian (h.AsSpan(32), s.StartTimeUnixNano);
            BinaryPrimitives.WriteInt64LittleEndian (h.AsSpan(40), s.DurationNanos);
            BinaryPrimitives.WriteUInt32LittleEndian(h.AsSpan(48), (uint)s.AttributesBytes.Length);
            BinaryPrimitives.WriteUInt16LittleEndian(h.AsSpan(52), (ushort)name.Length);
            BinaryPrimitives.WriteUInt16LittleEndian(h.AsSpan(54), (ushort)svc.Length);
            BinaryPrimitives.WriteInt16LittleEndian (h.AsSpan(56), s.HttpStatusCode);
            h[58] = (byte)s.Kind;
            h[59] = (byte)s.Status;
            BinaryPrimitives.WriteUInt32LittleEndian(h.AsSpan(60), gen);
            ms.Write(h); ms.Write(name); ms.Write(svc); ms.Write(s.AttributesBytes);
        }
        long writeOffset = ms.Length;
        ms.Write(new byte[64 * 1024]);                       // the mapped slack a real log carries

        byte[] file = ms.ToArray();
        BinaryPrimitives.WriteUInt32LittleEndian(file.AsSpan(0), 0x52_44_53_57);   // "RDSW"
        BinaryPrimitives.WriteUInt16LittleEndian(file.AsSpan(4), 1);               // VERSION 1
        BinaryPrimitives.WriteInt64LittleEndian (file.AsSpan(8), writeOffset);
        BinaryPrimitives.WriteUInt32LittleEndian(file.AsSpan(16), headerGeneration);
        File.WriteAllBytes(WalPath, file);
    }

    /// <summary>
    /// A v1 LOG LEFT BY THE PREVIOUS RELEASE REPLAYS — every field, under the same generation rule
    /// (the header's generation and its successor live, an older one dead) — and is a v2 log from
    /// then on: a second open replays the same spans, and an append after the upgrade survives.
    /// Treating version 1 as unknown, which is what the code did before, re-initialises the file and
    /// replays nothing: every span the old process acknowledged and never flushed, silently gone.
    /// </summary>
    [Fact]
    public void A_v1_log_opens_replays_and_is_upgraded_in_place()
    {
        WriteV1Log(headerGeneration: 7,
            (Item(0), 6),                                    // an older, committed generation: dead
            (Item(1), 7), (Item(2), 7),                      // the header's
            (Item(3, name: "", attrs: []), 8),               // its successor, mid-flush; empty fields
            (Item(4, name: "платёж 🚀"), 8));

        List<SpanIngestItem> first;
        using (var wal = SpanWriteAheadLog.Open(WalPath))
        {
            Assert.Equal((ushort)2, wal.HeaderForTest.Version);
            Assert.Equal(7u, wal.HeaderForTest.Generation);
            first = wal.ReadAll();
            wal.Append(Item(5));
        }

        Assert.Equal([101UL, 102UL, 103UL, 104UL], first.Select(static s => s.SpanId.RawValue));
        for (int k = 0; k < first.Count; k++)
        {
            var want = Item(k + 1, name: k == 2 ? "" : k == 3 ? "платёж 🚀" : null, attrs: k == 2 ? [] : null);
            var got  = first[k];
            Assert.Equal(want.TraceId, got.TraceId);
            Assert.Equal(want.ParentSpanId, got.ParentSpanId);
            Assert.Equal(want.StartTimeUnixNano, got.StartTimeUnixNano);
            Assert.Equal(want.DurationNanos, got.DurationNanos);
            Assert.Equal(want.Name, got.Name);
            Assert.Equal(want.ServiceName, got.ServiceName);
            Assert.Equal(want.Kind, got.Kind);
            Assert.Equal(want.Status, got.Status);
            Assert.Equal(want.HttpStatusCode, got.HttpStatusCode);
            Assert.Equal(want.AttributesBytes, got.AttributesBytes);
        }

        var again = Reopen();
        Assert.Equal([101UL, 102UL, 103UL, 104UL, 105UL], again.Select(static s => s.SpanId.RawValue));
        Assert.False(File.Exists(WalPath + SpanWriteAheadLog.UpgradeSuffix));
    }

    /// <summary>
    /// AN UPGRADE THAT CANNOT WRITE ITS COPY LEAVES THE v1 LOG EXACTLY AS IT WAS, and says so by
    /// throwing — it does not re-initialise the file to make the open succeed. The copy is blocked
    /// here by a directory squatting on its path (the full disk of production, reproducibly). Once
    /// the obstacle is gone the next open upgrades and replays everything.
    /// </summary>
    [Fact]
    public void A_failed_upgrade_leaves_the_v1_log_untouched()
    {
        WriteV1Log(headerGeneration: 3, (Item(0), 3), (Item(1), 3));
        byte[] before = File.ReadAllBytes(WalPath);
        Directory.CreateDirectory(WalPath + SpanWriteAheadLog.UpgradeSuffix);

        Assert.ThrowsAny<Exception>(() => SpanWriteAheadLog.Open(WalPath).Dispose());

        // Byte for byte what it was — the open may have extended the file to its mapped size, with
        // zeroes, and that is all it may have done.
        byte[] after = File.ReadAllBytes(WalPath);
        Assert.True(after.Length >= before.Length);
        Assert.Equal(before, after[..before.Length]);
        Assert.True(after.AsSpan(before.Length).IndexOfAnyExcept((byte)0) < 0);

        Directory.Delete(WalPath + SpanWriteAheadLog.UpgradeSuffix);
        Assert.Equal(2, Reopen().Count);
    }

    // ── What a commit forces to disk ─────────────────────────────────────────

    /// <summary>
    /// THE COMMIT'S TWO BARRIERS ARE RANGES, IN ORDER. First the relocated tail with its
    /// generation-0 terminator — a range starting at byte 0, so the header page rides along — msynced
    /// and drive-flushed while the header still carries the OLD generation. Only then the header is
    /// stamped, and its page alone is msynced. The whole 8 MB view is never handed to
    /// FlushViewOfFile. A barrier that stopped short of the terminator, or that came after the stamp,
    /// is the power-loss ordering bug the class comment describes: it fails here.
    /// </summary>
    [Fact]
    public void A_commit_msyncs_the_relocated_tail_then_the_header_page_and_nothing_else()
    {
        using var wal = SpanWriteAheadLog.Open(WalPath);
        AppendAll(wal, 50);
        wal.BeginFlush();
        long boundary = wal.WrittenBytes;

        // ONE tail entry sized so the relocated tail ends 8 bytes short of the first page boundary:
        // its generation-0 terminator then straddles into the second page. A barrier that stopped
        // at the tail would round up to one page and leave the terminator's generation unflushed.
        long page0 = Environment.SystemPageSize;
        var  blob  = new byte[page0 - FileHeader - 8 - V2EntryHead];
        wal.Append(new TraceId(9, 9), new SpanId(999), default, BaseNano, 1, SpanKind.Internal,
                   SpanStatusCode.Unset, 0, [], [], blob);
        long tail      = wal.WrittenBytes - boundary;
        Assert.Equal(page0 - FileHeader - 8, tail);
        uint flushedGen = wal.HeaderForTest.Generation;

        var events = new List<string>();
        var ranges = new List<(long Offset, long Length, uint GenAtFlush)>();
        wal.RangeFlushedForTest     = (off, len) => { ranges.Add((off, len, wal.HeaderForTest.Generation)); events.Add("R"); };
        wal.HandleFlushHookForTest  = h => { events.Add("H"); h.Flush(flushToDisk: true); };
        long failuresBefore = wal.RangeFlushFailures;

        wal.CommitFlush();

        Assert.Equal(["R", "H", "R", "H"], events);
        Assert.Equal(failuresBefore, wal.RangeFlushFailures);                    // no whole-view fallback

        long page      = Environment.SystemPageSize;
        long mustCover = FileHeader + tail + V2EntryHead;                        // tail + its terminator
        long fileSize  = FileHeader + wal.CapacityForTest;

        var data = ranges[0];
        Assert.Equal(0, data.Offset);                                            // header page included
        Assert.True(data.Length >= mustCover, $"barrier covers {data.Length} B, the tail and terminator need {mustCover}");
        Assert.True(data.Length <= (mustCover + page - 1) / page * page, $"barrier covers {data.Length} B — more than the tail's pages");
        Assert.True(data.Length < fileSize);                                     // not the whole view
        Assert.Equal(flushedGen, data.GenAtFlush);                               // BEFORE the header stamp

        var header = ranges[1];
        Assert.Equal(0, header.Offset);
        Assert.Equal(Math.Min(page, fileSize), header.Length);
        Assert.NotEqual(flushedGen, header.GenAtFlush);                          // AFTER the stamp

        Assert.Equal(999UL, Assert.Single(wal.ReadAll()).SpanId.RawValue);
    }

    /// <summary>
    /// THE HEADER'S DRIVE FLUSH RUNS WITH THE APPEND LOCK FREE. It is parked here, and while it is
    /// parked another thread must be able to take the append lock without waiting. The FIRST drive
    /// flush stays under the lock (the relocation is not known durable until it returns — see
    /// CommitFlush), which the engine keeps away from its readers instead: pinned by the next fact.
    /// </summary>
    [Fact]
    public async Task The_header_drive_flush_does_not_hold_the_append_lock()
    {
        using var wal = SpanWriteAheadLog.Open(WalPath);
        AppendAll(wal, 5);
        wal.BeginFlush();
        wal.Append(Item(7));

        using var parked  = new ManualResetEventSlim();
        using var release = new ManualResetEventSlim();
        int calls = 0;
        wal.HandleFlushHookForTest = h =>
        {
            if (++calls == 2) { parked.Set(); release.Wait(TimeSpan.FromSeconds(30)); }
            h.Flush(flushToDisk: true);
        };

        var commit = Task.Run(wal.CommitFlush);
        Assert.True(parked.Wait(TimeSpan.FromSeconds(30)), "hang guard: the header flush never ran");

        bool appendable = false;
        var probe = new Thread(() =>
        {
            if (wal.TryEnterAppendScope(out var scope)) { appendable = true; scope.Dispose(); }
        });
        probe.Start();
        probe.Join();
        release.Set();
        await commit.WaitAsync(TimeSpan.FromSeconds(30));

        Assert.True(appendable, "the append lock was held across the header's drive flush");
    }

    /// <summary>
    /// A DRAINER THAT FINDS THE LOG BUSY WAITS WITHOUT THE ENGINE LOCK. The log's append lock is
    /// held here from the test thread, as CommitFlush's first barrier holds it for a drive flush.
    /// When the drainer reaches its wait, a reader must be able to take the engine's read lock at
    /// once. Waiting for the log inside the engine's write lock — the obvious code — fails it: every
    /// query of the hot tier would sit out the fsync.
    /// </summary>
    [Fact]
    public async Task A_drainer_waiting_for_the_log_does_not_hold_the_engine_lock()
    {
        using var engine = new TraceStorageEngine(_dir, NullLogger<TraceStorageEngine>.Instance);
        var wal = engine.WalForTest;

        using var waiting = new ManualResetEventSlim();
        wal._appendWaitingForTest = waiting.Set;

        Task<int> writer;
        bool readerGotIn;
        using (wal.EnterAppendScope())
        {
            writer = Task.Run(() => engine.WriteSpans(new[] { Item(0), Item(1) }));
            Assert.True(waiting.Wait(TimeSpan.FromSeconds(30)), "hang guard: the drainer never waited for the log");
            readerGotIn = engine.LockForTest.TryEnterReadLock(0);
            if (readerGotIn) engine.LockForTest.ExitReadLock();
        }
        wal._appendWaitingForTest = null;

        int taken = await writer.WaitAsync(TimeSpan.FromSeconds(30));
        Assert.True(readerGotIn, "the drainer held the engine's write lock while it waited for the log");
        Assert.Equal(2, taken);
        Assert.Equal(2, wal.ReadAll().Count);
    }

    /// <summary>
    /// AFTER A FAILED COMMIT BARRIER THE LOG CONTINUES FROM THE RELOCATED END. The relocation is done
    /// in memory when the drive flush fails, and a generation-0 terminator sits right after it. The
    /// code before v2 left the write offset where it was — past that terminator — so every span
    /// appended afterwards was acknowledged, queryable, and gone at the next restart, because replay
    /// stops at the terminator in front of it.
    /// </summary>
    [Fact]
    public void Spans_appended_after_a_failed_commit_barrier_survive_a_restart()
    {
        using (var wal = SpanWriteAheadLog.Open(WalPath))
        {
            AppendAll(wal, 5);
            wal.BeginFlush();
            wal.Append(Item(5));                                                 // the tail
            wal.HandleFlushHookForTest = static _ => throw new IOException("injected: drive flush failed");
            Assert.Throws<IOException>(wal.CommitFlush);
            wal.HandleFlushHookForTest = null;

            wal.Append(Item(6));                                                 // after the failure
        }

        var replayed = Reopen();
        Assert.Contains(replayed, static s => s.SpanId.RawValue == 106);
        Assert.Contains(replayed, static s => s.SpanId.RawValue == 105);
    }

    // ── Growth ───────────────────────────────────────────────────────────────

    /// <summary>
    /// GROWTH DOUBLES UP TO THE HOT-TIER CAP AND THEN STEPS BY IT. A 64 MB log that needs one more
    /// span grows to 64 MB + one 27 MB step, not to 128 MB; a small log still doubles; and an append
    /// bigger than a step still fits. Pure doubling fails the first line.
    /// </summary>
    [Fact]
    public void Growth_is_stepped_by_the_hot_tier_cap_past_it()
    {
        long cap = MemoryBudgets.TraceHotTierCapBytes;
        const long MB = 1024 * 1024;

        Assert.Equal(RoundPage(64 * MB + cap), SpanWriteAheadLog.NextCapacity(64 * MB, 64 * MB + 1, cap));
        Assert.Equal(16 * MB, SpanWriteAheadLog.NextCapacity(8 * MB, 8 * MB + 1, cap));
        Assert.Equal(RoundPage(200 * MB), SpanWriteAheadLog.NextCapacity(64 * MB, 200 * MB, cap));

        // And through the log itself, with a step small enough to take several of them.
        using (var wal = SpanWriteAheadLog.Open(WalPath, 8 * 1024, growthStepCap: 16 * 1024))
        {
            for (int i = 0; i < 2_000; i++) wal.Append(Item(i));
            long c = wal.CapacityForTest;
            Assert.True(c - wal.WrittenBytes <= 16 * 1024 + 4096, $"capacity {c} overshoots {wal.WrittenBytes} by more than a step");
        }
        Assert.Equal(2_000, Reopen().Count);

        static long RoundPage(long v) => (v + 4095) & ~4095L;
    }

    // ── Probe ────────────────────────────────────────────────────────────────

    /// <summary>
    /// WHAT v2 COSTS PER APPEND AND WHAT A COMMIT HOLDS THE LOG FOR. Printed, not asserted: a
    /// 49 000-span log of the eight-attribute shape (~600 B of attributes), then a flush whose
    /// commit relocates a 1 000-span tail. Per-append ns are the writer thread's own; the commit is
    /// split into the part under the append lock (both range msyncs and the relocation's drive
    /// flush) and the header's drive flush, which now runs after the lock is released.
    ///
    /// <para>Release, a shared and loaded box, A/B against the same file with the CRC and the ranges
    /// patched out: append 540-1 060 ns/span with the checksum, 210-460 without, in runs whose
    /// spread is most of that gap; the checksum itself measures 60 ns over the 698-byte entry. Commit
    /// 80-300 ms either way, all of it but the 2-4 ms header drive flush under the append lock:
    /// FlushFileBuffers writes the whole dirty file whatever range was msynced before it.</para>
    /// </summary>
    [Fact]
    public void Probe_append_and_commit_cost()
    {
        var attrs = new byte[600];
        for (int i = 0; i < attrs.Length; i++) attrs[i] = (byte)(i * 31);
        var items = new SpanIngestItem[50_000];
        for (int i = 0; i < items.Length; i++) items[i] = Item(i, attrs: attrs);

        using var wal = SpanWriteAheadLog.Open(WalPath);
        for (int i = 0; i < items.Length; i++) wal.Append(items[i]);    // warm the JIT, grow the file, fault the pages in
        wal.Reset();

        var sw = Stopwatch.StartNew();
        for (int i = 0; i < 49_000; i++) wal.Append(items[i]);
        double nsPerAppend = sw.Elapsed.TotalNanoseconds / 49_000;

        // The checksum alone, over one entry's worth of bytes, for scale.
        var entryBytes = new byte[68 + 30 + attrs.Length];
        uint sink = 0;
        sw.Restart();
        for (int i = 0; i < 49_000; i++) sink ^= Crc32c.Append((uint)i, entryBytes);
        double nsPerCrc = sw.Elapsed.TotalNanoseconds / 49_000;

        wal.BeginFlush();
        for (int i = 49_000; i < 50_000; i++) wal.Append(items[i]);

        long offLockTicks = 0;
        int  calls        = 0;
        wal.HandleFlushHookForTest = h =>
        {
            long t0 = Stopwatch.GetTimestamp();
            h.Flush(flushToDisk: true);
            if (++calls == 2) offLockTicks = Stopwatch.GetTimestamp() - t0;
        };
        long c0 = Stopwatch.GetTimestamp();
        wal.CommitFlush();
        long total = Stopwatch.GetTimestamp() - c0;

        double ms(long t) => t * 1000.0 / Stopwatch.Frequency;
        _out.WriteLine($"log {wal.CapacityForTest / 1048576.0:N1} MB mapped; append {nsPerAppend:N0} ns/span, of which CRC32C over {entryBytes.Length} B ~{nsPerCrc:N0} ns");
        GC.KeepAlive(sink);
        _out.WriteLine($"commit {ms(total):N2} ms: under the append lock {ms(total - offLockTicks):N2} ms, "
                     + $"header drive flush off it {ms(offLockTicks):N2} ms; range msyncs {wal.RangeFlushCount}, "
                     + $"whole-view fallbacks {wal.RangeFlushFailures}");
        Assert.Equal(1_000, wal.ReadAll().Count);
    }
}
