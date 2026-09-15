using System.Buffers;
using System.Diagnostics;
using System.Net;
using System.Runtime.CompilerServices;
using System.Text.Json;
using MessagePack;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Ameto.Core;
using Ameto.Storage;
using LogLevel = Ameto.Core.LogLevel;

namespace Ameto.Integration.Tests;

/// <summary>
/// The live tail writes its rows through the row writer, which COALESCES: a frame waits in the
/// buffer for 16 KB, for a 100 ms hold, or for a moment when the scan makes the writer wait. The tail
/// used to send every row the moment it was written, so none of its timing depended on a buffer. Now
/// three things do, and these pin them through the real handler with a scripted executor — the
/// storage-backed <see cref="LiveTailPushTests"/> cannot tell which rule delivered a row:
/// <list type="bullet">
/// <item>every row of a poll reaches the client before the tail waits again, and the next poll
/// continues from the last row written;</item>
/// <item>rows a poll has found go out while its scan is still working;</item>
/// <item>a poll that runs out of budget delivers the rows it found exactly once, ahead of exactly one
/// query-error, and the tail ends there.</item>
/// </list>
/// </summary>
public sealed class LiveTailRowWriterTests : IClassFixture<LiveTailRowWriterTests.ScriptedTailFactory>
{
    /// <summary>Per-poll budget. Only the spent-budget script runs into it; it waits it out.</summary>
    private static readonly TimeSpan Budget = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Well under LiveTail.MaxWait (5 s) — how long a row left in the buffer would sit until the
    /// keepalive that ends the park carried it out. A row inside this bound was sent by its own poll.
    /// </summary>
    private static readonly TimeSpan PushBudget = TimeSpan.FromSeconds(2);

    private readonly ScriptedTailFactory _factory;

    public LiveTailRowWriterTests(ScriptedTailFactory factory) => _factory = factory;

    /// <summary>The ordinary test host, a one-second search budget, and the executor swapped for <see cref="ScriptedTail"/>.</summary>
    public sealed class ScriptedTailFactory : AmetoWebAppFactory
    {
        public ScriptedTail Tail { get; } = new();

        protected override QueryOptions ConfiguredQuery => new() { Timeout = Budget };

        protected override void ConfigureWebHost(IWebHostBuilder builder)
        {
            base.ConfigureWebHost(builder);
            builder.ConfigureTestServices(services => services.AddSingleton<IQueryExecutor>(Tail));
        }
    }

    /// <summary>
    /// Answers a tail's poll by its filter text — each test opens its tail with its own — and nothing
    /// else in the host: any other query gets no rows.
    /// </summary>
    public sealed class ScriptedTail : IQueryExecutor
    {
        public const string Burst       = "@mt = 'scripted burst'";
        public const string SlowScan    = "@mt = 'scripted slow scan'";
        public const string SpentBudget = "@mt = 'scripted spent budget'";

        private int _spentBudgetPolls;

        /// <summary>How many polls the spent-budget tail made. One: it ends after the first.</summary>
        public int SpentBudgetPolls => Volatile.Read(ref _spentBudgetPolls);

        /// <summary>The burst tail's second poll, which carries the cursor the first one left.</summary>
        public TaskCompletionSource<QueryRequest> BurstFollowUp { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Completed by the test when the client has read the slow scan's second row.</summary>
        public TaskCompletionSource SlowScanRowSeen { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>How the slow scan's wait ended: true when the client had its second row first.</summary>
        public TaskCompletionSource<bool> SlowScanWaitEnded { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public static LogEvent Row(uint seq) => new()
        {
            Id              = new EventId(7u, seq),
            Timestamp       = new DateTimeOffset(2026, 9, 15, 12, 0, 0, TimeSpan.Zero).AddMilliseconds(seq),
            Level           = LogLevel.Information,
            MessageTemplate = "row " + seq,
        };

        public IAsyncEnumerable<LogEvent> ExecuteAsync(QueryRequest request, CancellationToken ct = default)
        {
            switch (request.Filter)
            {
                case Burst when request.AfterEventId is null:
                    return new Inline([Row(0), Row(1), Row(2)]);
                case Burst:
                    BurstFollowUp.TrySetResult(request);
                    return new Inline([]);
                case SlowScan when request.AfterEventId is null:
                    return SlowScanAsync(ct);
                case SpentBudget:
                    Interlocked.Increment(ref _spentBudgetPolls);
                    return SpentBudgetAsync(ct);
                default:
                    return new Inline([]);
            }
        }

        /// <summary>
        /// Two rows back to back — the second is held by every on-write rule — and then the scan goes on
        /// working asynchronously, as ScanPace makes a real one wait, until the client has the second row
        /// or the budget runs out.
        /// </summary>
        private async IAsyncEnumerable<LogEvent> SlowScanAsync([EnumeratorCancellation] CancellationToken ct)
        {
            yield return Row(0);
            yield return Row(1);

            bool seen;
            try
            {
                await SlowScanRowSeen.Task.WaitAsync(ct);
                seen = true;
            }
            catch (OperationCanceledException)
            {
                seen = false;
            }
            SlowScanWaitEnded.TrySetResult(seen);
        }

        /// <summary>
        /// Two rows, and then the budget runs out while the scan is busy SYNCHRONOUSLY — decoding,
        /// evaluating — so nothing gives the writer a moment to send before it does. Only then does the
        /// step go asynchronous: the writer's send starts under the spent token, is refused before the
        /// body takes a byte, and both rows stay buffered for the handler's query-error to carry out.
        /// </summary>
        private static async IAsyncEnumerable<LogEvent> SpentBudgetAsync([EnumeratorCancellation] CancellationToken ct)
        {
            yield return Row(0);
            yield return Row(1);
            ct.WaitHandle.WaitOne(Budget * 10);
            await Task.Delay(50, CancellationToken.None);
        }

        /// <summary>Rows handed over with no wait before, between or after them — a hot-tier page.</summary>
        private sealed class Inline(LogEvent[] rows) : IAsyncEnumerable<LogEvent>
        {
            public IAsyncEnumerator<LogEvent> GetAsyncEnumerator(CancellationToken ct = default) => new Enumerator(rows);

            private sealed class Enumerator(LogEvent[] rows) : IAsyncEnumerator<LogEvent>
            {
                private int _next = -1;
                public LogEvent Current => rows[_next];
                public ValueTask<bool> MoveNextAsync() => new(++_next < rows.Length);
                public ValueTask DisposeAsync() => default;
            }
        }
    }

    // ── Harness ───────────────────────────────────────────────────────────────

    private sealed class Tail : IDisposable
    {
        public required HttpClient              Client   { get; init; }
        public required HttpResponseMessage     Response { get; init; }
        public required StreamReader            Reader   { get; init; }
        public required CancellationTokenSource Cts      { get; init; }

        public CancellationToken Token => Cts.Token;

        public void Dispose()
        {
            Cts.Cancel();
            Reader.Dispose();
            Response.Dispose();
            Client.Dispose();
            Cts.Dispose();
        }
    }

    /// <summary>Opens a tail on <paramref name="filter"/> and consumes the keepalive it opens with.</summary>
    private async Task<Tail> OpenTailAsync(string filter)
    {
        var cts    = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var client = _factory.CreateClient();

        using var request = new HttpRequestMessage(HttpMethod.Get, "/api/events/live?filter=" + Uri.EscapeDataString(filter));
        var response = await client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cts.Token);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);

        var reader = new StreamReader(await response.Content.ReadAsStreamAsync(cts.Token));
        string? hello = await reader.ReadLineAsync(cts.Token);
        Assert.StartsWith(":", hello);

        return new Tail { Client = client, Response = response, Reader = reader, Cts = cts };
    }

    /// <summary>Reads <paramref name="count"/> row frames and returns their templates, skipping everything else.</summary>
    private static async Task<List<string>> ReadRowsAsync(Tail tail, int count, Action<string>? onRow = null)
    {
        var rows = new List<string>(count);
        while (rows.Count < count && await tail.Reader.ReadLineAsync(tail.Token) is { } line)
        {
            if (!line.StartsWith("data: {\"@t\"", StringComparison.Ordinal)) continue;
            using var frame = JsonDocument.Parse(line["data: ".Length..]);
            string template = frame.RootElement.GetProperty("@mt").GetString()!;
            rows.Add(template);
            onRow?.Invoke(template);
        }
        return rows;
    }

    /// <summary>
    /// Everything the tail sends until it closes, in order: each row's template and each named event's
    /// name, plus the last query-error's message.
    /// </summary>
    private static async Task<(List<string> Frames, string? Error)> ReadToEndAsync(Tail tail)
    {
        var     frames = new List<string>();
        string? error  = null;
        bool    named  = false;
        while (await tail.Reader.ReadLineAsync(tail.Token) is { } line)
        {
            if (line.Length == 0) { named = false; continue; }
            if (line.StartsWith("event: ", StringComparison.Ordinal))
            {
                frames.Add(line["event: ".Length..]);
                named = true;
                continue;
            }
            if (!line.StartsWith("data: ", StringComparison.Ordinal)) continue;   // keepalive

            using var payload = JsonDocument.Parse(line["data: ".Length..]);
            if (named) error = payload.RootElement.GetProperty("error").GetString();
            else       frames.Add(payload.RootElement.GetProperty("@mt").GetString()!);
        }
        return (frames, error);
    }

    /// <summary>Any write moves the live-event signal, which wakes a parked tail for its next poll.</summary>
    private void WakeTheTail()
    {
        var engine = _factory.Services.GetRequiredService<StorageEngine>();

        var buf = new ArrayBufferWriter<byte>(16);
        var w   = new MessagePackWriter(buf);
        w.WriteMapHeader(0);
        w.Flush();

        Assert.True(engine.TryWrite(new LogEventHeader
        {
            TimestampUtcTicks        = DateTimeOffset.UtcNow.UtcTicks,
            Level                    = LogLevel.Information,
            MessageTemplatePoolIndex = engine.TemplatePool.Intern("wake the tail"),
        }, buf.WrittenSpan, "wake the tail"));
    }

    // ── Tests ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// EVERY ROW OF A POLL REACHES THE CLIENT BEFORE THE TAIL WAITS AGAIN. The burst is three rows handed
    /// over at once, straight after the opening keepalive: none is old enough for the hold rule, none makes
    /// 16 KB, and the scan never waits — so only the poll's own flush can send them. Without it they sit in
    /// the buffer while the tail parks, and arrive with the keepalive MaxWait later.
    ///
    /// <para>The poll after it must continue from the last row WRITTEN: its cursor is that row's
    /// (timestamp, id), or the tail would deliver the burst again or skip past it.</para>
    /// </summary>
    [Fact]
    public async Task Every_row_of_a_poll_reaches_the_client_before_the_tail_waits_again()
    {
        using var tail = await OpenTailAsync(ScriptedTail.Burst);

        var clock = Stopwatch.StartNew();
        var rows  = await ReadRowsAsync(tail, 3);
        clock.Stop();

        Assert.Equal(new[] { "row 0", "row 1", "row 2" }, rows);
        Assert.True(clock.Elapsed < PushBudget,
            $"the poll's rows took {clock.ElapsedMilliseconds} ms to reach the client — they waited in the " +
            "buffer for the keepalive after the park instead of going out with their poll");

        WakeTheTail();
        var next = await _factory.Tail.BurstFollowUp.Task.WaitAsync(TimeSpan.FromSeconds(10), tail.Token);
        Assert.Equal((EventId?)ScriptedTail.Row(2).Id, next.AfterEventId);
        Assert.Equal((long?)ScriptedTail.Row(2).Timestamp.UtcTicks, next.AfterTimestampTicks);
    }

    /// <summary>
    /// ROWS A POLL HAS FOUND GO OUT WHILE ITS SCAN IS STILL WORKING. The scan holds its poll open until the
    /// client has the second row, which only a send made while the scan waits can deliver: a tail that
    /// wrote rows in a loop of its own and flushed at the end of the poll would hold them until the budget
    /// ran out.
    /// </summary>
    [Fact]
    public async Task Rows_a_poll_has_found_go_out_while_its_scan_is_still_working()
    {
        using var tail = await OpenTailAsync(ScriptedTail.SlowScan);

        var rows = await ReadRowsAsync(tail, 2, template =>
        {
            if (template == "row 1") _factory.Tail.SlowScanRowSeen.TrySetResult();
        });

        Assert.Equal(new[] { "row 0", "row 1" }, rows);
        Assert.True(await _factory.Tail.SlowScanWaitEnded.Task.WaitAsync(TimeSpan.FromSeconds(10), tail.Token),
            "the client got row 1 only after the scan stopped waiting for it — the poll held its rows to the end");
    }

    /// <summary>
    /// A POLL THAT RUNS OUT OF BUDGET DELIVERS ITS ROWS ONCE, AHEAD OF ONE QUERY-ERROR, AND THE TAIL ENDS.
    /// The rows were found within budget but their send was refused by the spent token, so they are still
    /// in the writer's buffer: the query-error must carry them out, nothing may write them a second time,
    /// and the tail must not poll again.
    /// </summary>
    [Fact]
    public async Task A_poll_that_runs_out_of_budget_delivers_its_rows_once_ahead_of_one_query_error_and_the_tail_ends()
    {
        using var tail = await OpenTailAsync(ScriptedTail.SpentBudget);

        var (frames, error) = await ReadToEndAsync(tail);

        Assert.Equal(new[] { "row 0", "row 1", "query-error" }, frames);
        Assert.NotNull(error);
        Assert.Contains("budget", error);
        Assert.Equal(1, _factory.Tail.SpentBudgetPolls);
    }
}
