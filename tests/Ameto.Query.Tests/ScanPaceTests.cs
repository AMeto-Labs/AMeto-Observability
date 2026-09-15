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
    private static readonly TimeSpan StretchLimit = TimeSpan.FromSeconds(3);

    /// <summary>
    /// A ROW FOUND BEFORE A LONG SYNCHRONOUS STRETCH OF SCANNING IS ON THE WIRE BEFORE THAT
    /// STRETCH ENDS: the real executor, the real 50 ms pace, the real writer. The hot tier yields
    /// two matching rows back to back (the second is buffered by every on-write rule), then
    /// rejected events for as long as it takes the client to have the second row, or three
    /// seconds, then a third match. Nothing in the stretch waits; only the pace can give the
    /// writer a moment to send in.
    /// </summary>
    [Fact]
    public async Task A_row_found_before_a_long_synchronous_scan_is_on_the_wire_before_that_scan_ends()
    {
        var body  = new RecordingBody();
        var tier  = new StretchedHotTier(body, Row("hit 1"), StretchLimit);
        var query = new QueryExecutor(new HotTierOnly(tier), new SegmentIndexReaderFactory(), NullLogger<QueryExecutor>.Instance);
        using var sse = new SseJsonWriter(body);

        var request = new QueryRequest
        {
            Filter    = "@l = 'Error'",
            Count     = 100,
            Direction = QueryDirection.Forward,
        };
        await sse.WriteLogEventsAsync(query.ExecuteAsync(request), default);
        await sse.WriteDoneAsync(default);

        Assert.True(tier.ClientHadRowDuringStretch,
            $"hit 1 was still unsent after {tier.MissesServed} rejected events and {StretchLimit.TotalSeconds:0} s of scanning");

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
            try { Directory.Delete(dir, true); } catch { /* best-effort */ }
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
    /// A hot tier whose sorted read is one long synchronous stretch between the second and third
    /// match. It watches the body from inside the stretch, so it can tell the send happened while
    /// the scan was still working, not after.
    /// </summary>
    private sealed class StretchedHotTier(RecordingBody body, string awaitedRow, TimeSpan limit) : IHotTierReader
    {
        public volatile bool ClientHadRowDuringStretch;
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
                for (int i = 0; i < 256; i++) yield return miss;
                MissesServed += 256;

                if (body.Contains(awaitedRow)) { ClientHadRowDuringStretch = true; break; }
                if (stretch.Elapsed > limit) break;
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
