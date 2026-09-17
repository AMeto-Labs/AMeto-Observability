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
    [Fact]
    public async Task Disposing_the_host_disposes_its_file_logger_and_ends_the_drain_thread()
    {
        var factory = new AmetoWebAppFactory();
        FileLoggerProvider provider;
        try
        {
            provider = Assert.Single(factory.Services.GetServices<ILoggerProvider>().OfType<FileLoggerProvider>());
            Assert.False(provider.DrainExited, "precondition: the drain runs while the host does");
        }
        finally
        {
            await factory.DisposeAsync();
        }

        Assert.True(provider.DrainExited, "the host was disposed but its file logger's drain is still waiting on the queue");
    }
}
