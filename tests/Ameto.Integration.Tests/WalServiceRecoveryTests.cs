using System.Buffers;
using System.Globalization;
using System.Text;
using MessagePack;
using Microsoft.Extensions.Logging.Abstractions;
using Ameto.Core;
using Ameto.Core.Serialization;
using Ameto.Indexing;
using Ameto.Ingestion;
using Ameto.Otel;
using Ameto.Query;
using Ameto.Storage;

namespace Ameto.Integration.Tests;

/// <summary>
/// #111 end to end, through the real ingest roads rather than a hand-built header: CLEF events
/// whose service is given as <c>@service</c> or as <c>service.name</c> (the Serilog sink's
/// spelling) or not at all, and OTLP/JSON records whose resource names it — interned once per
/// resource block, the road that lost the service outright once #113 stopped keeping a copy of it
/// in the properties. Every event goes request → batch → <see cref="StorageEngine.WriteBatchAsync"/> → WAL;
/// the process is "killed" while the events are still only in the WAL, and the next start replays
/// it. Each event must then answer an <c>@service</c> filter with its own service, as it would have
/// had the flush written it.
/// </summary>
public sealed class WalServiceRecoveryTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-walsvc-e2e-" + Guid.NewGuid().ToString("N"));

    private string WalDir => Path.Combine(_dir, "wal");
    private string SegDir => Path.Combine(_dir, "segments");

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort */ }
    }

    private StorageEngine NewEngine(ServerOptions opts) => new(
        Microsoft.Extensions.Options.Options.Create(opts),
        new RetentionStore(opts, NullLogger<RetentionStore>.Instance),
        NullLogger<StorageEngine>.Instance);

    [Fact]
    public async Task Clef_and_otlp_events_keep_their_service_through_a_crash_replay()
    {
        Directory.CreateDirectory(_dir);
        var opts = new ServerOptions { DataDirectory = _dir };
        long t0  = DateTime.UtcNow.Ticks / TimeSpan.TicksPerMillisecond * TimeSpan.TicksPerMillisecond;
        long At(int ms) => t0 + ms * TimeSpan.TicksPerMillisecond;

        // Timestamp → the service the event was sent with (null: none).
        var expected = new Dictionary<long, string?>
        {
            [At(0)] = "Clef.Api",        // @service
            [At(1)] = "Clef.Legacy",     // service.name, the Serilog sink's key
            [At(2)] = null,              // neither
            [At(3)] = "Clef.Api",
            [At(4)] = "Otlp.Платежи 💳", // resource service.name, first resource block
            [At(5)] = "Otlp.Платежи 💳",
            [At(6)] = "Otlp.Other",      // second resource block
            [At(7)] = null,              // a resource without one
        };

        var engine = NewEngine(opts);
        try
        {
            int written = 0;
            engine.EventWritten = (_, _) => Interlocked.Increment(ref written);

            var endpoint = new IngestionEndpoint(engine, engine.TemplatePool, opts, NullLogger<IngestionEndpoint>.Instance);

            using (var clef = endpoint.BeginBatch())
            {
                Assert.Equal(4, LogEventSerializer.StreamBatch(ClefBatch(At), clef, out int clefDropped));
                Assert.Equal(0, clefDropped);
                Assert.Equal(new LogIngestResult(4, 0, Busy: false), await clef.CommitAsync());
            }

            using (var otlp = endpoint.BeginBatch())
            {
                Assert.Equal((4, 0), OtlpLogStreamParser.Parse(Encoding.UTF8.GetBytes(OtlpJson(At)), otlp));
                Assert.Equal(new LogIngestResult(4, 0, Busy: false), await otlp.CommitAsync());
            }

            // Answered means written: every event is in the tier and the WAL already.
            Assert.Equal(expected.Count, Volatile.Read(ref written));

            // kill -9: the events are in the WAL (and the hot tier) only. Keep the WAL's bytes,
            // let the shutdown flush and unlink it, then undo that flush.
            ulong  walId = engine.LiveWalSegmentId;
            string wal   = Directory.GetFiles(WalDir, "*.wal").Single();
            File.Copy(wal, wal + ".crash", overwrite: true);
            File.Copy(wal + ".pool", wal + ".pool.crash", overwrite: true);

            await engine.DisposeAsync();
            foreach (var f in Directory.GetFiles(SegDir, "*.seg"))
            {
                var parts = Path.GetFileNameWithoutExtension(f).Split('-');
                if (ulong.TryParse(parts[1], out var id) && id >= walId && id < walId + 6) File.Delete(f);
            }
            File.Move(wal + ".crash", wal, overwrite: true);
            File.Move(wal + ".pool.crash", wal + ".pool", overwrite: true);
        }
        finally
        {
            await engine.DisposeAsync();
        }

        await using var restarted = NewEngine(opts);
        await restarted.CatalogLoaded;
        var query = new QueryExecutor(restarted, new SegmentIndexReaderFactory(), NullLogger<QueryExecutor>.Instance);

        // Every event, back with its own service — the value the event JSON writes as @service.
        var events = await QueryAsync(query, filter: null);
        Assert.Equal(expected.Count, events.Count);
        foreach (var ev in events)
            Assert.Equal(expected[ev.Timestamp.UtcTicks], ev.ServiceName);

        // And to the filter, service by service.
        foreach (var service in expected.Values.OfType<string>().Distinct())
        {
            var found = await QueryAsync(query, $"@service = '{service}'");
            Assert.Equal(expected.Where(kv => kv.Value == service).Select(kv => kv.Key).Order(),
                         found.Select(e => e.Timestamp.UtcTicks).Order());
        }
    }

    /// <summary>
    /// Round-4 H2 at the door. An <c>@service</c> past <see cref="IngestionEndpoint.MaxServiceNameBytes"/>
    /// is cut — at a whole character — before it is interned, so the WAL logs it as one pool row, and
    /// every event keeps the cut name through a crash replay. The review's probe: 1 000 CLEF events with
    /// a 70 KB service made a 70 MB live WAL, and 3 500 a log whose replay ran a 384 MiB heap out at
    /// every start. (A service is an identifier; 1 KiB is far past any real one.)
    /// </summary>
    [Fact]
    public async Task A_service_past_the_limit_is_cut_at_the_door_and_logged_once()
    {
        Directory.CreateDirectory(_dir);
        var opts = new ServerOptions { DataDirectory = _dir };
        long t0  = DateTime.UtcNow.Ticks / TimeSpan.TicksPerMillisecond * TimeSpan.TicksPerMillisecond;
        const int Events = 200;
        // 1 023 bytes of 'a', then a two-byte 'é' across the cut, then 70 KB more: cut to the 'a's.
        string service = new string('a', IngestionEndpoint.MaxServiceNameBytes - 1) + "é" + new string('b', 70_000);
        string cut     = new string('a', IngestionEndpoint.MaxServiceNameBytes - 1);

        var engine = NewEngine(opts);
        try
        {
            var endpoint = new IngestionEndpoint(engine, engine.TemplatePool, opts, NullLogger<IngestionEndpoint>.Instance);
            using (var clef = endpoint.BeginBatch())
            {
                Assert.Equal(Events, LogEventSerializer.StreamBatch(LongServiceBatch(t0, Events, service), clef, out int dropped));
                Assert.Equal(0, dropped);
                Assert.Equal(new LogIngestResult(Events, 0, Busy: false), await clef.CommitAsync());
            }

            long walBytes = engine.LiveWalForTest!.WrittenBytes;
            Assert.True(walBytes < 64 * 1024, $"{walBytes:N0} B logged for {Events} events of one {service.Length:N0}-char service");

            // kill -9, as above: the events are in the WAL only when the next start comes.
            ulong  walId = engine.LiveWalSegmentId;
            string wal   = Directory.GetFiles(WalDir, "*.wal").Single();
            File.Copy(wal, wal + ".crash", overwrite: true);
            File.Copy(wal + ".pool", wal + ".pool.crash", overwrite: true);
            await engine.DisposeAsync();
            foreach (var f in Directory.GetFiles(SegDir, "*.seg"))
            {
                var parts = Path.GetFileNameWithoutExtension(f).Split('-');
                if (ulong.TryParse(parts[1], out var id) && id >= walId && id < walId + 6) File.Delete(f);
            }
            File.Move(wal + ".crash", wal, overwrite: true);
            File.Move(wal + ".pool.crash", wal + ".pool", overwrite: true);
        }
        finally
        {
            await engine.DisposeAsync();
        }

        await using var restarted = NewEngine(opts);
        await restarted.CatalogLoaded;
        var query  = new QueryExecutor(restarted, new SegmentIndexReaderFactory(), NullLogger<QueryExecutor>.Instance);
        var events = await QueryAsync(query, filter: null, count: Events + 1);
        Assert.Equal(Events, events.Count);
        Assert.All(events, ev => Assert.Equal(cut, ev.ServiceName));
    }

    /// <summary>
    /// The cut falls at the start of the character it would split — two-, three- and four-byte ones,
    /// at each byte they can be split on — and a name within the limit is kept whole.
    /// </summary>
    [Theory]
    [InlineData(0,    "",   1024)]   // 1 024 ASCII: whole
    [InlineData(1,    "",   1024)]   // 1 025 ASCII: the last byte goes
    [InlineData(-1,   "é",  1023)]   // 'é' over bytes 1023-1024
    [InlineData(-1,   "€",  1023)]   // '€' over 1023-1025
    [InlineData(-2,   "€",  1022)]   // '€' over 1022-1024
    [InlineData(-1,   "💳", 1023)]   // '💳' over 1023-1026
    [InlineData(-3,   "💳", 1021)]   // '💳' over 1021-1024
    [InlineData(-4,   "💳", 1024)]   // '💳' over 1020-1023: within the limit, whole
    public void ServiceName_cuts_at_the_start_of_the_character_it_would_split(int asciiPastLimit, string character, int expectedBytes)
    {
        byte[] name = Encoding.UTF8.GetBytes(new string('a', IngestionEndpoint.MaxServiceNameBytes + asciiPastLimit) + character + "tail");
        if (character.Length == 0) name = name[..(IngestionEndpoint.MaxServiceNameBytes + asciiPastLimit)];
        var cut = IngestionEndpoint.ServiceName(name);
        Assert.Equal(expectedBytes, cut.Length);
        Assert.Equal(name.AsSpan(0, expectedBytes).ToArray(), cut.ToArray());
        Assert.DoesNotContain('�', Encoding.UTF8.GetString(cut));
    }

    /// <summary><paramref name="count"/> CLEF events, one millisecond apart, all with <paramref name="service"/> as <c>@service</c>.</summary>
    private static ReadOnlyMemory<byte> LongServiceBatch(long t0, int count, string service)
    {
        var buf = new ArrayBufferWriter<byte>(1024);
        var w   = new MessagePackWriter(buf);
        w.WriteArrayHeader(count);
        for (int i = 0; i < count; i++)
        {
            w.WriteMapHeader(4);
            w.Write("@t");       w.Write(new DateTimeOffset(t0 + i * TimeSpan.TicksPerMillisecond, TimeSpan.Zero).ToString("O", CultureInfo.InvariantCulture));
            w.Write("@mt");      w.Write("clef {N}");
            w.Write("N");        w.Write(i);
            w.Write("@service"); w.Write(service);
        }
        w.Flush();
        return buf.WrittenMemory.ToArray();
    }

    private static async Task<List<LogEvent>> QueryAsync(QueryExecutor q, string? filter, int count = 100)
    {
        var res = new List<LogEvent>();
        await foreach (var ev in q.ExecuteAsync(new QueryRequest { Filter = filter, Count = count }))
            res.Add(ev);
        return res;
    }

    /// <summary>The CLEF batch as a sender posts it to /api/events: a msgpack array of maps.</summary>
    private static ReadOnlyMemory<byte> ClefBatch(Func<int, long> at)
    {
        var buf = new ArrayBufferWriter<byte>(512);
        var w   = new MessagePackWriter(buf);
        w.WriteArrayHeader(4);
        Clef(ref w, at(0), n: 1, ("@service", "Clef.Api"));
        Clef(ref w, at(1), n: 2, ("service.name", "Clef.Legacy"));
        Clef(ref w, at(2), n: 3);
        Clef(ref w, at(3), n: 4, ("@service", "Clef.Api"));
        w.Flush();
        return buf.WrittenMemory.ToArray();

        static void Clef(ref MessagePackWriter w, long ticks, int n, params (string Key, string Value)[] service)
        {
            w.WriteMapHeader(3 + service.Length);
            w.Write("@t");  w.Write(new DateTimeOffset(ticks, TimeSpan.Zero).ToString("O", CultureInfo.InvariantCulture));
            w.Write("@mt"); w.Write("clef {N}");
            w.Write("N");   w.Write(n);
            foreach (var (key, value) in service) { w.Write(key); w.Write(value); }
        }
    }

    /// <summary>An OTLP/JSON export of three resource blocks: two named services and one without.</summary>
    private static string OtlpJson(Func<int, long> at)
    {
        static string Nanos(long ticks) =>
            ((ticks - DateTime.UnixEpoch.Ticks) * 100).ToString(CultureInfo.InvariantCulture);
        static string Record(long ticks, string body) =>
            $$$"""{"timeUnixNano":"{{{Nanos(ticks)}}}","severityNumber":9,"body":{"stringValue":"{{{body}}}"}}""";
        static string Resource(string attributes, params string[] records) =>
            $$$"""{"resource":{"attributes":[{{{attributes}}}]},"scopeLogs":[{"logRecords":[{{{string.Join(",", records)}}}]}]}""";
        static string Attr(string key, string value) =>
            $$$"""{"key":"{{{key}}}","value":{"stringValue":"{{{value}}}"}}""";

        return $$"""
            {"resourceLogs":[
              {{Resource(Attr("service.name", "Otlp.Платежи 💳") + "," + Attr("host.name", "node-1"),
                         Record(at(4), "otlp one"), Record(at(5), "otlp two"))}},
              {{Resource(Attr("service.name", "Otlp.Other"), Record(at(6), "otlp three"))}},
              {{Resource(Attr("host.name", "node-2"), Record(at(7), "otlp serviceless"))}}
            ]}
            """;
    }
}
