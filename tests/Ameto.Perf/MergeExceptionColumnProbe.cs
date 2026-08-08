using System.Buffers;
using MessagePack;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit.Abstractions;
using Ameto.Core;
using Ameto.Storage;

namespace Ameto.Perf;

/// <summary>
/// What the exception column costs a merge, per event, with and without an index sink.
///
/// <para>It was the ONE field that was not zero-copy: the merge source decoded it into an
/// <see cref="ExceptionInfo"/> graph and the writer re-serialised it, per row. It lands squarely
/// on the compaction path, because level-split flush makes an Error segment 100 % exceptions and
/// compaction is what gathers them.</para>
///
/// <para>MEASURED on this shape (8000 events, a framed 12-line stack and one inner exception
/// each, no index sink):</para>
/// <list type="bullet">
/// <item>decode + re-encode — 11116 B/event, against 211 B/event for the same merge without
///       exceptions: 53×;</item>
/// <item>bytes copied through — 327 B/event, against 244 B/event without: 1.3×, i.e. what
///       carrying that many more bytes of any column costs.</item>
/// </list>
///
/// <para>The decode now happens only where the object graph is actually needed — the index
/// build, which indexes type, message and inner type as strings — so a merge without an index
/// sink never decodes at all and one with a sink decodes once instead of once plus re-encoding.</para>
/// </summary>
public sealed class MergeExceptionColumnProbe : IAsyncLifetime
{
    private readonly List<string> _dirs = new();
    private readonly ITestOutputHelper _out;
    private StorageEngine _engine = null!;

    public MergeExceptionColumnProbe(ITestOutputHelper output) => _out = output;

    public Task InitializeAsync() { NewEngine(); return Task.CompletedTask; }

    private void NewEngine()
    {
        string dir = Path.Combine(Path.GetTempPath(), "ameto-excmerge-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        _dirs.Add(dir);
        _engine = new StorageEngine(
            Options.Create(new ServerOptions { DataDirectory = dir }),
            new RetentionStore(new ServerOptions { DataDirectory = dir }, NullLogger<RetentionStore>.Instance),
            NullLogger<StorageEngine>.Instance)
        {
            _allowIndexlessMerge = true,
        };
    }

    public async Task DisposeAsync()
    {
        await _engine.DisposeAsync();
        foreach (var d in _dirs) { try { Directory.Delete(d, true); } catch { } }
    }

    private static byte[] Props(int n)
    {
        var buf = new ArrayBufferWriter<byte>(256);
        var w = new MessagePackWriter(buf);
        w.WriteMapHeader(2);
        w.Write("n");      w.Write((long)n);
        w.Write("wallet"); w.Write("wallet:" + n);
        w.Flush();
        return buf.WrittenSpan.ToArray();
    }

    /// <summary>A realistic exception: a framed stack, a message, and one inner.</summary>
    private static ExceptionInfo Exception(int n) => new()
    {
        Type       = "System.InvalidOperationException",
        Message    = "wallet " + n + " is not in a state that permits settlement",
        StackTrace = string.Join('\n', Enumerable.Range(0, 12).Select(f =>
            $"   at Ameto.Payments.Settlement.Step{f}(WalletContext ctx, Int32 attempt) in /src/Settlement.cs:line {100 + f}")),
        Inner      = new ExceptionInfo { Type = "System.TimeoutException", Message = "ledger did not answer in 30s" },
    };

    private async Task<long> MergeAllocationPerEventAsync(bool withExceptions, int events)
    {
        long old = BucketStart(DateTime.UtcNow.Ticks - 30 * TimeSpan.TicksPerDay);
        int  n   = 0;
        for (int seg = 0; seg < 8; seg++)
        {
            for (int i = 0; i < events / 8; i++, n++)
                Assert.True(_engine.TryWrite(new LogEventHeader
                {
                    Id                       = new EventId(0u, (uint)n).RawValue,
                    TimestampUtcTicks        = old + seg * TimeSpan.TicksPerHour + i * TimeSpan.TicksPerSecond,
                    Level                    = LogLevel.Error,
                    MessageTemplatePoolIndex = _engine.TemplatePool.Intern("settlement failed for {wallet}"),
                    ServiceNamePoolIndex     = _engine.TemplatePool.Intern("Svc.Payments"),
                }, Props(n), exception: withExceptions ? Exception(n) : null));
            await _engine.FlushHotTierAsync();
        }

        GC.Collect(); GC.WaitForPendingFinalizers(); GC.Collect();
        long before = GC.GetTotalAllocatedBytes(precise: true);
        Assert.True(await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None));
        long alloc = GC.GetTotalAllocatedBytes(precise: true) - before;

        var merged = _engine.ListSegments().Single();
        Assert.Equal((uint)events, merged.EventCount);
        return alloc / events;
    }

    private static long BucketStart(long ticks)
    {
        long w = StorageEngine.MergeBucketTicks(RetentionPolicy.Default.GetTtl(LogLevel.Error));
        return ticks / w * w;
    }

    [Fact]
    public async Task ExceptionsDoNotDominateMergeAllocation()
    {
        long withExc = await MergeAllocationPerEventAsync(withExceptions: true, events: 8000);
        _out.WriteLine($"merge with exceptions:    {withExc,6} B/event");

        // Fresh engine, same shape without the exception column.
        await _engine.DisposeAsync();
        NewEngine();
        long noExc = await MergeAllocationPerEventAsync(withExceptions: false, events: 8000);
        _out.WriteLine($"merge without exceptions: {noExc,6} B/event  ({withExc / (double)Math.Max(1, noExc):F1}x)");

        // The column is copied, not rebuilt, so carrying an exception costs a merge roughly what
        // carrying the same bytes of any other column does. The guard is set well above the
        // measured 1.3× and far below the 53× a decode-and-re-encode reads.
        Assert.True(withExc < noExc * 6,
            $"exceptions cost {withExc} B/event against {noExc} B/event without — the column is not being copied through");
    }
}
