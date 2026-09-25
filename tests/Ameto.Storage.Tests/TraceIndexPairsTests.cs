using Ameto.Tracing;
using Ameto.Tracing.Storage;

namespace Ameto.Storage.Tests;

/// <summary>
/// TS#7(c): THE WRITER'S TRACE INDEX IS ONE SORTED (trace, offset) PER SPAN, not a
/// <c>Dictionary&lt;TraceId, List&lt;uint&gt;&gt;</c>. Two things must not move with it:
///
/// <list type="bullet">
/// <item>THE v3 INDEX BLOCK'S ORDER — first-seen trace order, which is what the dictionary
/// enumerated in; sorting it by id would change every v3 <c>.trc</c> (the golden bytes in
/// <c>TraceFlushProbe</c> pin it too; this states it as a property of any corpus).</item>
/// <item>WHAT THE <c>.tix</c> ANSWERS — for every trace, exactly the offsets the segment's own v3
/// index holds, ascending.</item>
/// </list>
///
/// <para>The corpus interleaves traces across blocks and ties on start time, so first-seen order,
/// id order and offset order all differ. Mutation check (by hand): writing the v3 block in id order
/// fails the first fact and the v3 golden-bytes facts.</para>
/// </summary>
public sealed class TraceIndexPairsTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-tixpairs-" + Guid.NewGuid().ToString("N"));

    public TraceIndexPairsTests() => Directory.CreateDirectory(_dir);
    public void Dispose() { try { Directory.Delete(_dir, true); } catch { } }

    /// <summary>9 000 spans over 700 traces, ids unrelated to arrival order, every trace spread over the blocks.</summary>
    private static List<SpanRecord> Corpus()
    {
        var spans = new List<SpanRecord>(9_000);
        for (int i = 0; i < 9_000; i++)
        {
            int t = (int)((uint)(i * 7919) % 700);
            spans.Add(new SpanRecord
            {
                TraceId = new TraceId(unchecked((ulong)t * 0x9E3779B97F4A7C15UL), (ulong)(t % 3)),
                SpanId  = new SpanId((ulong)(i + 1)),
                StartTimeUnixNano = 1_785_000_000_000_000_000L + (i % 400) * 1_000L,   // ties
                DurationNanos = 1_000, Name = "op", ServiceName = "svc", Kind = SpanKind.Server,
            });
        }
        return spans;
    }

    [Fact]
    public void The_v3_index_block_keeps_first_seen_order_and_the_run_answers_what_it_holds()
    {
        TraceIndexPairs pairs = default;
        var info = SpanWriter.Write(_dir, Corpus(), onTraceIndex: p => pairs = p, version: 3);
        try
        {
            // First-seen order, from the file's own span order.
            var written   = SpanReader.ReadAll(info.FilePath);
            var firstSeen = new List<TraceId>();
            var seen      = new HashSet<TraceId>();
            foreach (var s in written) if (seen.Add(s.TraceId)) firstSeen.Add(s.TraceId);

            var v3 = SpanReader.ReadTraceIndexForTest(info.FilePath)!;
            Assert.Equal(firstSeen, v3.Keys.ToList());                      // the block's order
            Assert.Equal(700, pairs.Traces);
            Assert.Equal(9_000, pairs.Count);

            // The refs: sorted by id, then offset; each run is the v3 index's list for that trace.
            var refs = pairs.Refs.ToArray();
            for (int i = 1; i < refs.Length; i++)
                Assert.True(new ByTraceThenOffset().Compare(refs[i - 1], refs[i]) < 0, $"refs out of order at {i}");
            foreach (var (id, offsets) in v3)
                Assert.Equal(offsets, refs.Where(r => r.TraceId.Equals(id)).Select(r => r.Offset).ToList());

            // The .tix built from them answers every trace with exactly those offsets.
            var w = new TraceIndexWriter();
            w.AddSegment(in pairs, segmentId: 42);
            var run = w.Write(Path.Combine(_dir, "pairs.tix"), level: 1, coveredSegments: [42UL]);
            Assert.Equal(700, run.EntryCount);
            using var reader = TraceIndexReader.Open(run.FilePath)!;
            foreach (var (id, offsets) in v3)
            {
                var hits = new List<TraceIndexHit>();
                Assert.Equal(TraceIndexOutcome.Found, reader.Lookup(TraceIndexFile.KeyOf(id), hits));
                // A key is the id's high half: ids sharing it (t % 3 differs) come back together.
                Assert.Contains(hits, h => h.SegmentId == 42 && h.Offsets.SequenceEqual(offsets));
            }
        }
        finally { pairs.Release(); }
    }

    [Fact]
    public void A_v4_flush_with_no_index_caller_builds_no_map_and_one_with_a_caller_still_gets_it()
    {
        var v4 = SpanWriter.Write(_dir, Corpus(), version: 4);                        // nothing to hand over
        Assert.Equal(9_000, SpanReader.ReadAll(v4.FilePath).Count);

        TraceIndexPairs pairs = default;
        SpanWriter.Write(_dir, Corpus(), onTraceIndex: p => pairs = p, version: 4);
        try
        {
            Assert.Equal(700, pairs.Traces);
            Assert.Equal(9_000, pairs.Count);
        }
        finally { pairs.Release(); }
    }
}
