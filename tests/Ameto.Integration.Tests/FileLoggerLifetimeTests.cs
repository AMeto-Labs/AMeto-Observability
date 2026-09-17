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
}
