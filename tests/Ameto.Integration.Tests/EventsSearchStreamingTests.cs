using System.Collections.Concurrent;
using System.Net;
using System.Runtime.CompilerServices;
using System.Text.Json;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Ameto.Core;
using LogLevel = Ameto.Core.LogLevel;

namespace Ameto.Integration.Tests;

/// <summary>
/// The search stream coalesces rows, so something has to send the ones found so far when the
/// scan stops producing for a while — otherwise the tail of every burst sits in the buffer until
/// the next burst, or until <c>done</c>, while the Angular store is built to paint rows as they
/// arrive. <c>SseJsonWriter.WriteLogEventsAsync</c> does it on the handler's own call, each time
/// the scan makes it wait. Its unit tests pin the writer; this pins that <c>/api/events</c>
/// actually hands the scan to it. A handler that went back to writing rows one by one in its own
/// loop would still produce a correct, complete stream — only a late one — and no other test
/// would notice.
/// </summary>
public sealed class EventsSearchStreamingTests : IClassFixture<EventsSearchStreamingTests.SparseScanFactory>
{
    /// <summary>
    /// The page size that marks this suite's search. Anything else in the host that runs a query
    /// through the executor gets nothing, and cannot disturb the scan's bookkeeping.
    /// </summary>
    private const int ProbeCount = 4242;

    private readonly SparseScanFactory _factory;

    public EventsSearchStreamingTests(SparseScanFactory factory) => _factory = factory;

    /// <summary>The ordinary test host with the query executor swapped for <see cref="SparseScan"/>.</summary>
    public sealed class SparseScanFactory : AmetoWebAppFactory
    {
        public SparseScan Scan { get; } = new();

        protected override void ConfigureWebHost(IWebHostBuilder builder)
        {
            base.ConfigureWebHost(builder);
            builder.ConfigureTestServices(services => services.AddSingleton<IQueryExecutor>(Scan));
        }
    }

    /// <summary>
    /// A scan shaped like a sparse cold search: bursts of two rows found back to back, each
    /// followed by an ASYNCHRONOUS wait (a segment open, a prefilter). The wait ends as soon as the
    /// client has read the burst's second row, or after <see cref="WaitLimit"/> if it never does,
    /// and records which it was.
    /// </summary>
    public sealed class SparseScan : IQueryExecutor
    {
        public static readonly TimeSpan WaitLimit = TimeSpan.FromSeconds(3);

        private readonly ConcurrentDictionary<string, TaskCompletionSource> _seen = new();

        /// <summary>One entry per wait: true when the client had the burst's last row before it ended.</summary>
        public ConcurrentQueue<bool> SeenDuringWait { get; } = new();

        /// <summary>Called by the test for every row frame it reads.</summary>
        public void ClientSaw(string template) => Signal(template).TrySetResult();

        private TaskCompletionSource Signal(string template) =>
            _seen.GetOrAdd(template, static _ => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));

        public async IAsyncEnumerable<LogEvent> ExecuteAsync(
            QueryRequest request, [EnumeratorCancellation] CancellationToken ct = default)
        {
            if (request.Count != ProbeCount) yield break;

            for (uint burst = 0; burst < 3; burst++)
            {
                uint first = burst * 10, second = first + 1;
                yield return Row(first);
                yield return Row(second);          // right behind it: buffered by every on-write rule

                bool seen;
                try
                {
                    await Signal(Template(second)).Task.WaitAsync(WaitLimit, ct);
                    seen = true;
                }
                catch (TimeoutException)
                {
                    seen = false;
                }
                SeenDuringWait.Enqueue(seen);
            }
        }

        private static LogEvent Row(uint seq) => new()
        {
            Id              = new EventId(0u, seq),
            Timestamp       = new DateTimeOffset(2026, 9, 14, 12, 0, 0, TimeSpan.Zero).AddSeconds(-seq),
            Level           = LogLevel.Information,
            MessageTemplate = Template(seq),
        };
    }

    private static string Template(uint seq) => "row " + seq;

    /// <summary>
    /// A SPARSE SEARCH SHOWS EACH ROW BEFORE THE SCAN'S NEXT WAIT ENDS, NOT AT <c>done</c> — over
    /// HTTP, through the real handler, reading the stream as a client does. Each burst's second
    /// row can only reach the client in time if the handler sends while the scan waits; with rows
    /// written one by one in the handler's own loop it waits out the whole gap, and the last one
    /// waits for <c>done</c>.
    /// </summary>
    [Fact]
    public async Task A_sparse_search_shows_each_row_before_the_scans_next_wait_ends()
    {
        using var cts      = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        using var client   = _factory.CreateClient();
        using var request  = new HttpRequestMessage(HttpMethod.Get, $"/api/events?count={ProbeCount}");
        using var response = await client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cts.Token);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("text/event-stream", response.Content.Headers.ContentType?.MediaType);

        using var reader = new StreamReader(await response.Content.ReadAsStreamAsync(cts.Token));
        var    rows   = new List<string>();
        string? ending = null;
        while (await reader.ReadLineAsync(cts.Token) is { } line)
        {
            if (line.StartsWith("event: ", StringComparison.Ordinal))
            {
                ending = line;
                continue;
            }
            if (!line.StartsWith("data: {\"@t\"", StringComparison.Ordinal)) continue;

            using var frame = JsonDocument.Parse(line["data: ".Length..]);
            string template = frame.RootElement.GetProperty("@mt").GetString()!;
            rows.Add(template);
            _factory.Scan.ClientSaw(template);
        }

        Assert.Equal(new[] { true, true, true }, _factory.Scan.SeenDuringWait.ToArray());
        Assert.Equal(new[] { "row 0", "row 1", "row 10", "row 11", "row 20", "row 21" }, rows);
        Assert.Equal("event: done", ending);
    }
}
