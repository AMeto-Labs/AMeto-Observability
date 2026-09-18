using Ameto.Server;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Ameto.Integration.Tests;

/// <summary>
/// The rolling file logger drains its queue on a pool thread parked in
/// <c>GetConsumingEnumerable</c> until the provider is disposed. It was registered as an instance,
/// which the container never disposes, so every host this suite starts and stops left one such
/// thread behind for the life of the test process — thirty in one crash dump — and kept its log
/// file open under a data directory the fixture then could not delete.
/// </summary>
public sealed class FileLoggerLifetimeTests
{
    /// <summary>
    /// How long the drain may take to exit once the host is gone. A wait rather than a check at the
    /// instant <c>DisposeAsync</c> returns: a <c>WebApplicationFactory</c> host is disposed by two
    /// chains at once — the factory's, and <c>app.Run()</c>'s once ApplicationStopping wakes it —
    /// and the container's disposal returns immediately to whichever of them arrives second. The
    /// provider can therefore still be being disposed by the other chain when the factory's call
    /// returns here (3 of 12 full-suite runs checked too early). A provider nobody disposes never
    /// exits, so the bound only decides how long a failure takes to report.
    /// </summary>
    private static readonly TimeSpan DrainExitPatience = TimeSpan.FromSeconds(30);

    [Fact]
    public async Task Disposing_the_host_disposes_its_file_logger_and_ends_the_drain_thread()
    {
        var factory = new AmetoWebAppFactory();
        FileLoggerProvider provider;
        try
        {
            provider = Assert.Single(factory.Services.GetServices<ILoggerProvider>().OfType<FileLoggerProvider>());
            Assert.False(provider.Drain.IsCompleted, "precondition: the drain runs while the host does");
        }
        finally
        {
            await factory.DisposeAsync();
        }

        var exited = await Task.WhenAny(provider.Drain, Task.Delay(DrainExitPatience));
        Assert.True(ReferenceEquals(exited, provider.Drain),
            "the host was disposed but its file logger's drain is still waiting on the queue");
    }

    /// <summary>
    /// A line logged after the provider is disposed is dropped, never thrown. Loggers outlive their
    /// provider: a flush left running past the host's shutdown budget still logs "Flushed segment"
    /// through the <see cref="ILogger"/> it was handed at startup. Enqueue used to ask the disposed
    /// queue whether adding was completed, which throws ObjectDisposedException, and MEL rethrows a
    /// provider's throw to the caller as an AggregateException — so that flush skipped its WAL
    /// delete and leaked its tier.
    /// </summary>
    [Fact]
    public void A_line_logged_through_an_existing_logger_after_the_provider_is_disposed_is_dropped_not_thrown()
    {
        string dir = Path.Combine(Path.GetTempPath(), "ameto-filelog-" + Guid.NewGuid().ToString("N"));
        var provider = new FileLoggerProvider(dir, LogLevel.Information);
        try
        {
            // A provider handed to the factory's constructor stays the caller's to dispose.
            using var factory = new LoggerFactory([provider]);
            var logger = factory.CreateLogger("Ameto.Storage.StorageEngine");
            logger.LogInformation("before dispose");

            provider.Dispose();
            Assert.True(provider.Drain.IsCompleted, "precondition: the drain finished, so Dispose went on to dispose the queue");

            var thrown = Record.Exception(() => logger.LogInformation("after dispose"));

            Assert.Null(thrown);
            string written = string.Concat(Directory.GetFiles(dir, "ameto-*.log").Select(File.ReadAllText));
            Assert.Contains("before dispose", written);
            Assert.DoesNotContain("after dispose", written);
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch { /* best-effort */ }
        }
    }
}
