using System.Buffers;
using System.Diagnostics;
using System.Net;
using System.Text.Json;
using MessagePack;
using Microsoft.Extensions.DependencyInjection;
using Ameto.Core;
using Ameto.Storage;
using LogLevel = Ameto.Core.LogLevel;

namespace Ameto.Integration.Tests;

/// <summary>
/// The live tail waits to be told that something was written instead of re-querying four
/// times a second. These pin the two things that break if the wake-up path does: an event
/// must reach an open tail promptly rather than on the next safety poll, and a tail that is
/// behind must keep reading rather than parking on a version that will not move again.
/// </summary>
public sealed class LiveTailPushTests : IClassFixture<AmetoWebAppFactory>
{
    private readonly AmetoWebAppFactory _factory;

    public LiveTailPushTests(AmetoWebAppFactory factory) => _factory = factory;

    /// <summary>
    /// Comfortably under LiveTail.MaxWait (5 s) — the interval a tail that missed its wake-up
    /// would fall back on. That is what makes this bound mean "pushed" and not "polled anyway".
    /// </summary>
    private static readonly TimeSpan PushBudget = TimeSpan.FromSeconds(2);

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
            Cts.Cancel();        // stops the server-side loop rather than leaving it parked
            Reader.Dispose();
            Response.Dispose();
            Client.Dispose();
            Cts.Dispose();
        }
    }

    private async Task<Tail> OpenTailAsync(string? filter, TimeSpan budget, string? levels = null)
    {
        var cts    = new CancellationTokenSource(budget);
        var client = _factory.CreateClient();

        var query = new List<string>();
        if (filter is not null) query.Add("filter=" + Uri.EscapeDataString(filter));
        if (levels is not null) query.Add("levels=" + Uri.EscapeDataString(levels));
        string url = "/api/events/live" + (query.Count == 0 ? "" : "?" + string.Join('&', query));

        using var request = new HttpRequestMessage(HttpMethod.Get, url);
        var response = await client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cts.Token);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("text/event-stream", response.Content.Headers.ContentType?.MediaType);

        var reader = new StreamReader(await response.Content.ReadAsStreamAsync(cts.Token));

        // The handler writes one keepalive as soon as the stream is committed. Consuming it
        // here is what makes the timings below meaningful: the tail is provably inside its
        // loop before the test writes anything, so a write cannot land before the first poll
        // and pass for a wake-up.
        string? hello = await reader.ReadLineAsync(cts.Token);
        Assert.StartsWith(":", hello);

        return new Tail { Client = client, Response = response, Reader = reader, Cts = cts };
    }

    /// <summary>Reads <paramref name="count"/> event frames, skipping keepalives and blanks.</summary>
    private static async Task<List<JsonElement>> ReadEventsAsync(Tail tail, int count)
    {
        var events = new List<JsonElement>(count);
        while (events.Count < count)
        {
            string? line = await tail.Reader.ReadLineAsync(tail.Token);
            if (line is null) break;                                   // stream closed
            if (!line.StartsWith("data: ", StringComparison.Ordinal))  // keepalive / frame separator
                continue;
            events.Add(JsonDocument.Parse(line["data: ".Length..]).RootElement.Clone());
        }
        return events;
    }

    /// <summary>
    /// Writes straight through StorageEngine — the same call the ingest drainer makes, so the
    /// write hook fires exactly as it does in production, without the ring's pacing in between.
    /// </summary>
    private void Write(int index, LogLevel level = LogLevel.Information)
    {
        var engine = _factory.Services.GetRequiredService<StorageEngine>();

        var buf = new ArrayBufferWriter<byte>(64);
        var w   = new MessagePackWriter(buf);
        w.WriteMapHeader(1);
        w.Write("n");
        w.Write((long)index);
        w.Flush();

        Assert.True(engine.TryWrite(new LogEventHeader
        {
            TimestampUtcTicks        = DateTimeOffset.UtcNow.UtcTicks,
            Level                    = level,
            MessageTemplatePoolIndex = engine.TemplatePool.Intern("tailed {n}"),
        }, buf.WrittenSpan, "tailed {n}"));
    }

    // ── Tests ─────────────────────────────────────────────────────────────────

    [Fact]
    public async Task An_event_written_after_the_tail_connected_arrives_without_waiting_for_a_poll()
    {
        using var tail = await OpenTailAsync(filter: null, TimeSpan.FromSeconds(20));

        var clock = Stopwatch.StartNew();
        Write(1);

        var events = await ReadEventsAsync(tail, 1);
        clock.Stop();

        Assert.Single(events);
        Assert.True(clock.Elapsed < PushBudget,
            $"the event took {clock.ElapsedMilliseconds} ms to reach the tail — that is the " +
            "safety-net poll, not the wake-up");
    }

    [Fact]
    public async Task A_tail_a_full_page_behind_keeps_reading_instead_of_parking()
    {
        // More than LiveTail.PageSize (500), so the first poll comes back full. A tail that
        // parked on an unchanged version here would sit on the remainder until some unrelated
        // write signalled again — the backlog is already committed, so nothing else will.
        const int Total = 620;

        using var tail = await OpenTailAsync(filter: null, TimeSpan.FromSeconds(30));

        for (int i = 0; i < Total; i++) Write(i);

        var clock  = Stopwatch.StartNew();
        var events = await ReadEventsAsync(tail, Total);
        clock.Stop();

        Assert.Equal(Total, events.Count);
        // Timed, or the assertion above would also pass on a tail that parked and was
        // rescued by the 5 s safety poll — which is the bug this test exists to catch.
        Assert.True(clock.Elapsed < PushBudget,
            $"the backlog took {clock.ElapsedMilliseconds} ms to drain — the tail parked instead " +
            "of going round again");
    }

    [Fact]
    public async Task The_level_selection_reaches_the_tail_too()
    {
        // The tail never had a `levels` parameter: the selector applied to the page's history
        // and was dropped the moment live started, so the view silently widened back to every
        // level. This is the one line of plumbing that carries it.
        using var tail = await OpenTailAsync(filter: null, TimeSpan.FromSeconds(20), levels: "Fatal");

        Write(1, LogLevel.Information);
        Write(2, LogLevel.Fatal);

        var events = await ReadEventsAsync(tail, 1);

        // Written first, so a tail that ignored `levels` delivers the Information event here.
        Assert.Equal("Fatal", events[0].GetProperty("@l").GetString());
    }

    [Fact]
    public async Task The_filter_still_decides_what_a_tail_sees()
    {
        using var tail = await OpenTailAsync("@l = 'Fatal'", TimeSpan.FromSeconds(20));

        Write(1, LogLevel.Information);
        Write(2, LogLevel.Fatal);
        Write(3, LogLevel.Information);

        var events = await ReadEventsAsync(tail, 1);

        Assert.Single(events);
        Assert.Equal("Fatal", events[0].GetProperty("@l").GetString());
    }
}
