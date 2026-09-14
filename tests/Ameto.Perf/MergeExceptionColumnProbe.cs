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

    public Task InitializeAsync() { NewEngine(withSink: false); return Task.CompletedTask; }

    /// <param name="withSink">
    /// With the production index sink wired, so the merge builds the three index sections
    /// per group — which is where the exception column's bytes are actually READ (type,
    /// message, inner type). Without it the merge only copies the column through, so the
    /// index-less number says nothing about what an Error-level compaction costs.
    /// </param>
    private void NewEngine(bool withSink)
    {
        string dir = Path.Combine(Path.GetTempPath(), "ameto-excmerge-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        _dirs.Add(dir);
        _engine = new StorageEngine(
            Options.Create(new ServerOptions { DataDirectory = dir }),
            new RetentionStore(new ServerOptions { DataDirectory = dir }, NullLogger<RetentionStore>.Instance),
            NullLogger<StorageEngine>.Instance)
        {
            _allowIndexlessMerge = !withSink,
        };
        if (withSink)
        {
            var hints = new Ameto.Indexing.IndexBuildHints();
            _engine.IndexSinkFactory = (events, terms) => new Ameto.Indexing.SegmentIndexBuilder(events, 5, terms, hints);
        }
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
        NewEngine(withSink: false);
        long noExc = await MergeAllocationPerEventAsync(withExceptions: false, events: 8000);
        _out.WriteLine($"merge without exceptions: {noExc,6} B/event  ({withExc / (double)Math.Max(1, noExc):F1}x)");

        // The column is copied, not rebuilt, so carrying an exception costs a merge roughly what
        // carrying the same bytes of any other column does. The guard is set well above the
        // measured 1.3× and far below the 53× a decode-and-re-encode reads.
        Assert.True(withExc < noExc * 6,
            $"exceptions cost {withExc} B/event against {noExc} B/event without — the column is not being copied through");
    }

    /// <summary>
    /// The same merge WITH the index sink — the production shape. The index reads the
    /// exception's type, message and inner type per row; it used to get them by decoding the
    /// whole object graph, stack trace included (<c>ExceptionInfo.FromBytes</c>: a payload
    /// copy, four key strings and 1-5 KB of UTF-16 stack per row), which the index-less
    /// number above never saw. MEASURED before the span read: 5 036 B/event with exceptions
    /// against 157 B/event without — 4 879 B per exception-carrying row for three short
    /// strings; after: 417 against 174 B/event, the remainder being what the message's
    /// trigrams and the two extra properties cost the (pooled, cold here) accumulators.
    /// </summary>
    [Fact]
    public async Task ExceptionsDoNotDominateMergeAllocation_WithIndexSink()
    {
        await _engine.DisposeAsync();
        NewEngine(withSink: true);
        long withExc = await MergeAllocationPerEventAsync(withExceptions: true, events: 8000);
        _out.WriteLine($"indexed merge with exceptions:    {withExc,6} B/event");

        await _engine.DisposeAsync();
        NewEngine(withSink: true);
        long noExc = await MergeAllocationPerEventAsync(withExceptions: false, events: 8000);
        _out.WriteLine($"indexed merge without exceptions: {noExc,6} B/event  ({withExc / (double)Math.Max(1, noExc):F1}x, +{withExc - noExc} B per exception row)");

        // The index reads three short strings out of each exception; it must not pay for the
        // stack trace it never indexes. 600 B per row is a ceiling on the terms it files
        // (measured 243), an order of magnitude under the 4 879 the full decode cost.
        Assert.True(withExc - noExc < 600,
            $"indexing an exception costs {withExc - noExc} B/event — the index is decoding more of it than it reads");
    }
}
