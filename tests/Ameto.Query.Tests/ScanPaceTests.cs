using System.Diagnostics;
using System.Text;
using Microsoft.Extensions.Logging.Abstractions;
using Ameto.Core;
using Ameto.Indexing;

namespace Ameto.Query.Tests;

/// <summary>
/// The scan hands its consumer a pending step while it works (<see cref="ScanPace"/>).
///
/// <para>The events stream sends its buffered rows exactly when the scan makes it wait. Its own
/// tests drive it with scripted sources that wait asynchronously, and the real executor never
/// did: the segment reader, the hot tier, the merge and the priming are all synchronous, so no
/// step of a real query was ever pending (probed over the many-segments fixture: none, with or
/// without a filter, either direction). The last rows of every burst then waited for the next
/// row, 16 KB, or <c>done</c>. These tests pin the pacing to the real executor, not to a
/// script.</para>
/// </summary>
public sealed class ScanPaceTests
{
    /// <summary>
    /// Bounds the stretch only against a hang. Whether the pace handed the writer a moment is decided
    /// without a clock (see <see cref="StepWatch"/>); once it has, the send is made on the writer's
    /// own thread straight away, so the stretch reaching this bound means the send never came.
    /// </summary>
    private static readonly TimeSpan HangGuard = TimeSpan.FromSeconds(30);

    /// <summary>
    /// A ROW FOUND BEFORE A LONG SYNCHRONOUS STRETCH OF SCANNING IS ON THE WIRE BEFORE THAT
    /// STRETCH ENDS: the real executor, the real pace, the real writer. The hot tier yields two
    /// matching rows back to back (the second is buffered by every on-write rule), then rejected
    /// events until the client has the second row, then a third match. Nothing in the stretch waits;
    /// only the pace can give the writer a moment to send in.
    ///
    /// <para>The pace is set to yield at every clock read (64 events), as the cold-scan theory below
    /// does, so the test can tell a pace that never yields from a slow runner WITHOUT a clock: if the
    /// stretch is still running inside the writer's own <c>MoveNextAsync</c> call, on the writer's
    /// thread, hundreds of events after the pace was due, the scan never went pending and the writer
    /// never had its moment. The real 50 ms pace was raced against a 3 s Stopwatch instead, which a
    /// writer thread stalled at the wrong instant on a loaded runner could lose.</para>
    /// </summary>
    [Fact]
    public async Task A_row_found_before_a_long_synchronous_scan_is_on_the_wire_before_that_scan_ends()
    {
        var body  = new RecordingBody();
        var steps = new StepWatch();
        var tier  = new StretchedHotTier(body, Row("hit 1"), steps);
        var query = new QueryExecutor(new HotTierOnly(tier), new SegmentIndexReaderFactory(), NullLogger<QueryExecutor>.Instance)
        {
            ScanYieldInterval = TimeSpan.Zero,
        };
        using var sse = new SseJsonWriter(body);

        var request = new QueryRequest
        {
            Filter    = "@l = 'Error'",
            Count     = 100,
            Direction = QueryDirection.Forward,
        };
        await sse.WriteLogEventsAsync(steps.Watch(query.ExecuteAsync(request)), default);
        await sse.WriteDoneAsync(default);

        Assert.True(tier.ClientHadRowDuringStretch,
            $"hit 1 was still unsent after {tier.MissesServed} rejected events: "
          + (tier.StretchStayedInsideTheCall ? "the scan never went pending, so the writer had no moment to send" : "the hang guard ran out"));

        string all = body.All();
        Assert.Equal(3, Count(all, "data: {\"@t\""));
        int at0 = all.IndexOf(Row("hit 0"), StringComparison.Ordinal);
        int at1 = all.IndexOf(Row("hit 1"), StringComparison.Ordinal);
        int at2 = all.IndexOf(Row("hit 2"), StringComparison.Ordinal);
        Assert.True(0 <= at0 && at0 < at1 && at1 < at2, $"rows missing or out of order: {at0}, {at1}, {at2}");
        Assert.DoesNotContain(Row("miss"), all);
        Assert.EndsWith("event: done\ndata: {}\n\n", all);
    }

    private const string Sparse = "n = 5 or n = 6 or n = 505 or n = 506 or n = 905";

    /// <summary>
    /// …and the COLD scan does it too, through the merge and the lazy priming, without changing a
    /// single row. The fixture is forty small segments, far too little work to take 50 ms, so
    /// the pace is set to yield at every clock read; what is pinned here is that the segment scan
    /// consults it for every event it looks at, and that its yields reach the consumer as
    /// pending steps BETWEEN rows, where a sparse search spends its time.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task A_cold_scan_hands_its_consumer_pending_steps_between_rows_and_returns_the_same_rows(bool forward)
    {
        string dir = Path.Combine(Path.GetTempPath(), "ameto-scanpace-" + Guid.NewGuid().ToString("N"));
        var (engine, steady) = await QuerySegmentFixtures.ManySegmentsAsync(dir);
        try
        {
            int all   = QuerySegmentFixtures.ManySegments * QuerySegmentFixtures.EventsPerSegment;
            var paced = new QueryExecutor(engine, new SegmentIndexReaderFactory(), NullLogger<QueryExecutor>.Instance)
            {
                ScanYieldInterval = TimeSpan.Zero,
            };

            var expected = await QuerySegmentFixtures.RunAsync(steady, Sparse, all, forward: forward);

            var rows             = new List<LogEvent>();
            var pendingAfterRows = new List<int>();
            var request = new QueryRequest
            {
                Filter    = Sparse,
                Count     = all,
                Direction = forward ? QueryDirection.Forward : QueryDirection.Backward,
            };
            await using (var scan = paced.ExecuteAsync(request).GetAsyncEnumerator())
            {
                while (true)
                {
                    ValueTask<bool> next = scan.MoveNextAsync();
                    if (!next.IsCompleted) pendingAfterRows.Add(rows.Count);
                    if (!await next) break;
                    rows.Add(scan.Current);
                }
            }

            Assert.Equal(5, expected.Count);
            Assert.Equal(expected.Select(static e => e.Id.RawValue), rows.Select(static e => e.Id.RawValue));
            Assert.Contains(pendingAfterRows, r => r > 0 && r < rows.Count);
        }
        finally
        {
            await engine.DisposeAsync();
            QuerySegmentFixtures.DeleteDataDirectory(dir);
        }
    }

    private static string Row(string template) => $"\"@mt\":\"{template}\"";

    private static int Count(string haystack, string needle) => haystack.Split(needle).Length - 1;

    private static LogEvent Event(int second, LogLevel level, string template) => new()
    {
        Id              = new EventId(0u, (uint)second),
        Timestamp       = new DateTimeOffset(2026, 9, 15, 12, 0, 0, TimeSpan.Zero).AddSeconds(second),
        Level           = level,
        MessageTemplate = template,
    };

    /// <summary>
    /// Sits between the executor and the writer and records the call the writer is inside: the thread
    /// that called <c>MoveNextAsync</c>, until that call returns. Code running on that thread while the
    /// call is open is running SYNCHRONOUSLY inside the step — the writer cannot have looked at a
    /// pending step yet, let alone sent in it. Code running anywhere else, or after the call returned,
    /// runs after the step went pending. The wrapper hands the executor's own <c>ValueTask</c> through
    /// untouched, so the writer sees exactly what it would have.
    /// </summary>
    private sealed class StepWatch
    {
        private volatile bool _inCall;
        private volatile int  _callThread;

        public bool InsideTheCallOnThisThread => _inCall && _callThread == Environment.CurrentManagedThreadId;

        public IAsyncEnumerable<LogEvent> Watch(IAsyncEnumerable<LogEvent> source) => new Watched(source, this);

        private sealed class Watched(IAsyncEnumerable<LogEvent> source, StepWatch watch) : IAsyncEnumerable<LogEvent>
        {
            public IAsyncEnumerator<LogEvent> GetAsyncEnumerator(CancellationToken ct = default) =>
                new Enumerator(source.GetAsyncEnumerator(ct), watch);
        }

        private sealed class Enumerator(IAsyncEnumerator<LogEvent> inner, StepWatch watch) : IAsyncEnumerator<LogEvent>
        {
            public LogEvent Current => inner.Current;

            public ValueTask<bool> MoveNextAsync()
            {
                watch._callThread = Environment.CurrentManagedThreadId;
                watch._inCall     = true;
                try     { return inner.MoveNextAsync(); }
                finally { watch._inCall = false; }
            }

            public ValueTask DisposeAsync() => inner.DisposeAsync();
        }
    }

    /// <summary>
    /// A hot tier whose sorted read is one long synchronous stretch between the second and third
    /// match. It watches the body from inside the stretch, so it can tell the send happened while
    /// the scan was still working, not after.
    /// </summary>
    private sealed class StretchedHotTier(RecordingBody body, string awaitedRow, StepWatch steps) : IHotTierReader
    {
        public volatile bool ClientHadRowDuringStretch;
        public volatile bool StretchStayedInsideTheCall;
        public long MissesServed;

        public IEnumerable<LogEvent> ReadAll() => throw new NotSupportedException("the executor reads the hot tier sorted");

        public IEnumerable<LogEvent> ReadSorted(
            long fromTicks, long toTicks, long? afterTsTicks, ulong? afterIdRaw, bool forward, IReadOnlySet<LogLevel>? levels)
        {
            yield return Event(0, LogLevel.Error, "hit 0");
            yield return Event(1, LogLevel.Error, "hit 1");

            LogEvent miss    = Event(2, LogLevel.Information, "miss");   // the filter rejects it
            var      stretch = Stopwatch.StartNew();
            while (true)
            {
                // 256 events: four times the 64 after which a pace at interval zero is due.
                for (int i = 0; i < 256; i++) yield return miss;
                MissesServed += 256;

                if (body.Contains(awaitedRow)) { ClientHadRowDuringStretch = true; break; }

                // Still synchronous inside the writer's call after the pace was due: it never yielded,
                // and nothing will send the row until this stretch ends. No clock decides this.
                if (steps.InsideTheCallOnThisThread) { StretchStayedInsideTheCall = true; break; }

                if (stretch.Elapsed > HangGuard) break;
            }

            yield return Event(3, LogLevel.Error, "hit 2");
        }

        public IReadOnlySet<SegmentKey> CoveredSegmentKeys => EmptyCoveredSet.Instance;

        public void Dispose() { }
    }

    private sealed class HotTierOnly(IHotTierReader tier) : ISegmentProvider
    {
        public IReadOnlyList<SegmentInfo> GetSegments(DateTimeOffset? from, DateTimeOffset? to) => [];
        public IHotTierReader OpenHotTierReader() => tier;
    }

    /// <summary>A response body that completes inline and can be read from the scan's side.</summary>
    private sealed class RecordingBody : Stream
    {
        private readonly StringBuilder _all = new();

        public string All()                  { lock (_all) return _all.ToString(); }
        public bool   Contains(string needle) { lock (_all) return _all.ToString().Contains(needle, StringComparison.Ordinal); }

        public override void Write(ReadOnlySpan<byte> buffer)
        {
            string s = Encoding.UTF8.GetString(buffer);
            lock (_all) _all.Append(s);
        }
        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken ct = default)
        {
            if (ct.IsCancellationRequested) return ValueTask.FromCanceled(ct);
            Write(buffer.Span);
            return ValueTask.CompletedTask;
        }
        public override Task FlushAsync(CancellationToken ct) =>
            ct.IsCancellationRequested ? Task.FromCanceled(ct) : Task.CompletedTask;

        public override bool CanRead => false;
        public override bool CanSeek => false;
        public override bool CanWrite => true;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => 0; set => throw new NotSupportedException(); }
        public override void Flush() { }
        public override int Read(byte[] b, int o, int c) => throw new NotSupportedException();
        public override long Seek(long o, SeekOrigin r) => throw new NotSupportedException();
        public override void SetLength(long v) => throw new NotSupportedException();
        public override void Write(byte[] b, int o, int c) => Write(b.AsSpan(o, c));
    }
}
