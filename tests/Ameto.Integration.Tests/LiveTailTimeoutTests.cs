using System.Buffers;
using System.Net;
using System.Runtime.CompilerServices;
using MessagePack;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Ameto.Core;
using Ameto.Query;
using Ameto.Storage;
using LogLevel = Ameto.Core.LogLevel;

namespace Ameto.Integration.Tests;

/// <summary>
/// A live tail whose poll outruns its budget must END, and say why exactly once: a tail that
/// stopped without a terminal <c>query-error</c> frame looks exactly like a quiet live view, and
/// one that sent two would be reporting a single failure twice.
///
/// <para>WHAT THIS ONE REACHES, which is less than the rule. The budget can run out on three
/// paths — mid-poll (the executor yields early, or the write throws), after the poll but before
/// Disarm, and with its timer only queued when the next poll tries to re-arm. This end-to-end
/// test reaches the MID-POLL path only. Reverting QueryGuard.cs does not fail it, so read it as a
/// smoke test over the whole wire. The other two paths and the re-arm rule itself are pinned as
/// unit tests by <c>QueryDeadlineTests</c> — in particular
/// <c>A_rearm_refused_by_a_queued_budget_timer_is_a_timeout_before_the_token_says_so</c>, which
/// is what actually fails on a revert.</para>
///
/// <para>EVERY POLL IS HELD UNTIL ITS BUDGET HAS RUN OUT (<see cref="BudgetFirstExecutor"/>).
/// This test used to race a 1 ms budget against the scan of a 16 000-event backlog and call the
/// scan "past the OS timer's resolution, on any machine". It is not. A 1 ms
/// <c>CancelAfter</c> on Windows is serviced by the timer queue a whole clock tick (~15.6 ms)
/// later, and a warm Debug poll of that backlog takes about 8 ms: the poll Disarms before the
/// timer runs, the timer never fires, and the tail parks for <c>LiveTail.MaxWait</c> and tries
/// again. The filter matches nothing, so the cursor never moves and every poll is the same scan
/// with the same odds. Run alone, the first poll pays for JIT and loses the race, which is why it
/// passed locally. Run after the suite has warmed the scan, most polls win it, and twelve wins in
/// a row is the 60 s capture bound: CI on 5619445, and five runs in twenty on two pinned cores.
/// No backlog outruns a clock tick on every machine and in every build; holding the poll
/// does.</para>
/// </summary>
public sealed class LiveTailTimeoutTests : IClassFixture<LiveTailTimeoutTests.TinyBudgetFactory>
{
    /// <summary>A 1 ms search budget, and polls that are still running when it runs out.</summary>
    public sealed class TinyBudgetFactory : AmetoWebAppFactory
    {
        protected override QueryOptions ConfiguredQuery => new() { Timeout = TimeSpan.FromMilliseconds(1) };

        protected override void ConfigureWebHost(IWebHostBuilder builder)
        {
            base.ConfigureWebHost(builder);

            // Registered after the server's own, so the endpoint resolves the hold; behind it is
            // the executor the server would have used.
            builder.ConfigureTestServices(static services =>
            {
                services.AddSingleton<QueryExecutor>();
                services.AddSingleton<IQueryExecutor>(
                    static sp => new BudgetFirstExecutor(sp.GetRequiredService<QueryExecutor>()));
            });
        }
    }

    /// <summary>
    /// A poll that outruns its budget on any machine, in any build, at any timer resolution: it
    /// waits for the budget before it scans, then hands the spent token to the real executor,
    /// which ends its own scan exactly as it does when a real scan runs long. The wait is on the
    /// poll's own token and nothing else: a poll whose budget is never armed parks here until
    /// the client leaves, and the capture bound fails the test.
    /// </summary>
    private sealed class BudgetFirstExecutor(IQueryExecutor inner) : IQueryExecutor
    {
        public async IAsyncEnumerable<LogEvent> ExecuteAsync(
            QueryRequest request, [EnumeratorCancellation] CancellationToken ct = default)
        {
            try { await Task.Delay(Timeout.InfiniteTimeSpan, ct); }
            catch (OperationCanceledException) { }

            await foreach (var ev in inner.ExecuteAsync(request, ct))
                yield return ev;
        }
    }

    private readonly TinyBudgetFactory _factory;

    public LiveTailTimeoutTests(TinyBudgetFactory factory) => _factory = factory;

    [Fact]
    public async Task A_tail_whose_poll_outruns_its_budget_ends_with_exactly_one_query_error_frame()
    {
        // A backlog the scan has to walk event by event: the filter matches nothing and gives the
        // header nothing to decide, so the executor reaches its per-event budget check with the
        // token already spent. Kept inside ONE hot-tier chunk (16 384 events, the whole of this
        // factory's 8 MB tier): past it TryWrite refuses until a flush.
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
