using System.Buffers;
using System.Net;
using MessagePack;
using Microsoft.Extensions.DependencyInjection;
using Ameto.Core;
using Ameto.Storage;
using LogLevel = Ameto.Core.LogLevel;

namespace Ameto.Integration.Tests;

/// <summary>
/// A live tail whose poll outruns its budget must END, and say why exactly once. The budget
/// can run out on any of three paths — mid-poll (the executor yields early, or the write
/// throws), after the poll but before Disarm, or with its timer only queued when the next
/// poll tries to re-arm — and each owes the client one terminal <c>query-error</c> frame.
/// A tail that stopped without one looks exactly like a quiet live view; one that sent two
/// would be reporting a single failure twice.
/// </summary>
public sealed class LiveTailTimeoutTests : IClassFixture<LiveTailTimeoutTests.TinyBudgetFactory>
{
    /// <summary>A 1 ms search budget: every poll over a real backlog runs out of it.</summary>
    public sealed class TinyBudgetFactory : AmetoWebAppFactory
    {
        protected override QueryOptions ConfiguredQuery => new() { Timeout = TimeSpan.FromMilliseconds(1) };
    }

    private readonly TinyBudgetFactory _factory;

    public LiveTailTimeoutTests(TinyBudgetFactory factory) => _factory = factory;

    [Fact]
    public async Task A_tail_whose_poll_outruns_its_budget_ends_with_exactly_one_query_error_frame()
    {
        // A backlog the poll has to walk event by event: the filter matches nothing and gives
        // the header nothing to decide, so every event is materialised (six properties
        // decoded) and evaluated — far past a 1 ms budget, and past the OS timer's
        // resolution, on any machine. Kept inside ONE hot-tier chunk (16 384 events, the
        // whole of this factory's 8 MB tier): past it TryWrite refuses until a flush.
        const int Backlog = 16_000;
        var  engine = _factory.Services.GetRequiredService<StorageEngine>();
        long start  = DateTimeOffset.UtcNow.AddMinutes(-10).UtcTicks;
        int  tmpl   = engine.TemplatePool.Intern("backlog {n}");
        var  buf    = new ArrayBufferWriter<byte>(128);
        for (int i = 0; i < Backlog; i++)
        {
            buf.Clear();
            var w = new MessagePackWriter(buf);
            w.WriteMapHeader(6);
            w.Write("n");        w.Write((long)i);
            w.Write("Route");    w.Write("/api/payments/" + (i % 97));
            w.Write("Elapsed");  w.Write(i * 0.25);
            w.Write("Tenant");   w.Write("tenant-" + (i % 13));
            w.Write("Attempt");  w.Write((long)(i % 5));
            w.Write("Ok");       w.Write(i % 3 != 0);
            w.Flush();

            Assert.True(engine.TryWrite(new LogEventHeader
            {
                TimestampUtcTicks        = start + i * (TimeSpan.TicksPerMillisecond / 10),
                Level                    = LogLevel.Information,
                MessageTemplatePoolIndex = tmpl,
            }, buf.WrittenSpan, "backlog {n}"));
        }

        using var client = _factory.CreateClient();
        string from = new DateTimeOffset(start, TimeSpan.Zero).AddMinutes(-1).ToString("O");
        string url  = "/api/events/live?filter=" + Uri.EscapeDataString("n < 0")
                    + "&from=" + Uri.EscapeDataString(from);

        // Read to the END of the body: a tail that broke off silently would close the stream
        // with no terminal frame, and one that never ended would trip this bound instead.
        var capture = await TestHelpers.CaptureSseAsync(client, url, TimeSpan.FromSeconds(60));

        Assert.Equal(HttpStatusCode.OK, capture.Status);
        Assert.Empty(capture.Rows);
        Assert.Equal("query-error", capture.Terminal);
        Assert.Equal(1, capture.TerminalCount);
        Assert.NotNull(capture.Error);
        Assert.Contains("budget", capture.Error);
    }
}
