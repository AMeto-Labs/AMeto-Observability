using Ameto.Core;
using Ameto.Ingestion;
using Ameto.Storage;
using Microsoft.Extensions.Logging.Abstractions;

namespace Ameto.Integration.Tests;

/// <summary>
/// Host shutdown disposes the drainer from two chains at once: disposing a
/// <c>WebApplicationFactory</c> stops the host, and <c>app.Run()</c>, woken by
/// ApplicationStopping, stops it again. The chain whose call returned first went on to dispose the
/// container — the ring and the storage engine — under the final drain still reading one and
/// writing the other. So a dispose that returns must mean the drain loop has exited, whichever
/// caller it returns to.
///
/// <para>The drain is held inside a write, not raced: the storage engine's
/// <see cref="StorageEngine.EventWritten"/> hook runs inside <see cref="StorageEngine.TryWrite"/>
/// on the drain thread, so blocking it holds the loop at a known point for as long as the test
/// needs.</para>
/// </summary>
public sealed class IngestionDrainerDisposeTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-drainer-dispose-" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort */ }
    }

    [Fact]
    public async Task A_second_dispose_waits_for_the_drain_loop_the_first_one_is_stopping()
    {
        Directory.CreateDirectory(_dir);
        var opts = new ServerOptions { DataDirectory = _dir };
        await using var storage = new StorageEngine(
            Microsoft.Extensions.Options.Options.Create(opts),
            new RetentionStore(opts, NullLogger<RetentionStore>.Instance),
            NullLogger<StorageEngine>.Instance);

        using var entered = new ManualResetEventSlim();
        using var release = new ManualResetEventSlim();
        storage.EventWritten = (_, _) =>
        {
            entered.Set();
            release.Wait();
        };

        int slab = opts.Ingestion.MaxEventPayloadBytes;
        var ring    = new IngestionRingBuffer(1024, slab, 16L * slab);
        var drainer = new IngestionDrainer(ring, storage, opts, NullLogger<IngestionDrainer>.Instance);

        Task? first = null;
        try
        {
            Assert.True(ring.TryEnqueue(
                DateTime.UtcNow.Ticks, (byte)LogLevel.Information, -1, "held {N}", null, [0x80]));
            drainer.NotifyEnqueued();
            Assert.True(entered.Wait(TimeSpan.FromSeconds(30)), "the drain loop never reached the write");

            first = drainer.DisposeAsync().AsTask();
            var second = drainer.DisposeAsync().AsTask();

            // Observed at the instant each completes, on the thread completing it.
            var loopDoneAtFirst  = first.ContinueWith(_ => drainer.DrainLoop.IsCompleted, TaskContinuationOptions.ExecuteSynchronously);
            var loopDoneAtSecond = second.ContinueWith(_ => drainer.DrainLoop.IsCompleted, TaskContinuationOptions.ExecuteSynchronously);

            // The loop is parked inside the write and cannot exit until `release` is set, so a
            // call that has already returned did so without waiting for it.
            Assert.False(second.IsCompleted, "the second DisposeAsync returned while the drain loop was still running");
            Assert.False(first.IsCompleted);

            release.Set();
            await Task.WhenAll(first, second).WaitAsync(TimeSpan.FromSeconds(30));

            Assert.True(await loopDoneAtFirst,  "the first DisposeAsync completed before the drain loop exited");
            Assert.True(await loopDoneAtSecond, "the second DisposeAsync completed before the drain loop exited");
        }
        finally
        {
            // However the test left, the loop must be out of the ring before the ring is freed.
            release.Set();
            if (first is not null) await first;
            await drainer.DrainLoop.WaitAsync(TimeSpan.FromSeconds(30));
            ring.Dispose();
        }
    }
}
