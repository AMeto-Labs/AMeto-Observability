using System.Text;
using MessagePack;
using Microsoft.Extensions.Logging.Abstractions;
using Ameto.Tracing;
using Ameto.Tracing.Ingestion;
using Ameto.Tracing.Storage;
using Xunit.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// TI#3: THE RAW SPAN RING — 72-byte headers in native slots, payloads in a slab arena — and the sink
/// and the drainer on either side of it. Every concurrency fact here is judged by a SEAM: a thread is
/// parked at an exact instant and the window is observed, never waited for.
/// </summary>
public sealed class SpanRingRawTests : IDisposable
{
    private static readonly TimeSpan HangGuard = TimeSpan.FromSeconds(30);
    private static readonly DateTimeOffset Base = new(2026, 8, 1, 12, 0, 0, TimeSpan.Zero);

    private readonly string            _root = Path.Combine(Path.GetTempPath(), "ameto-rawring-" + Guid.NewGuid().ToString("N"));
    private readonly ITestOutputHelper _out;
    private readonly long              _baseNano = Base.ToUnixTimeMilliseconds() * 1_000_000L;

    public SpanRingRawTests(ITestOutputHelper output)
    {
        _out = output;
        Directory.CreateDirectory(_root);
    }

    public void Dispose() { try { Directory.Delete(_root, true); } catch { } }

    private string Dir(string name)
    {
        string d = Path.Combine(_root, name);
        Directory.CreateDirectory(d);
        return d;
    }

    // ── the ring ────────────────────────────────────────────────────────────

    /// <summary>
    /// A PAYLOAD GOES IN AND COMES BACK BYTE FOR BYTE, in every shape the arena treats differently:
    /// nothing at all, an ordinary span, one exactly a chunk long, and one larger than a chunk (kept
    /// apart in a managed array). And the budget is whole again once the run is released.
    /// </summary>
    [Fact]
    public void A_payload_goes_through_the_ring_and_comes_back_byte_for_byte()
    {
        using var ring = new SpanRingBuffer(capacity: 64, maxBytes: 4 * 1024 * 1024);
        var shapes = new (string Name, string Service, byte[] Attrs)[]
        {
            ("", "", []),
            ("GET /orders/{id}", "billing", Blob("/orders/1", "GET")),
            ("x", "s", new byte[SpanRingBuffer.ChunkBytes - 2]),        // exactly one chunk of payload
            ("big", "billing", Filled(100_000)),                         // larger than a chunk: kept apart
            ("é ünïcode 名前", "サービス", Blob("/ü", "PUT")),
        };

        for (int i = 0; i < shapes.Length; i++)
        {
            var h = Fields(i);
            int idx = shapes[i].Service.Length == 0 ? -1 : ring.Pools.Services.Intern(Encoding.UTF8.GetBytes(shapes[i].Service));
            Assert.True(ring.TryEnqueueRaw(in h, Encoding.UTF8.GetBytes(shapes[i].Name), idx,
                                           Encoding.UTF8.GetBytes(shapes[i].Service), shapes[i].Attrs));
        }
        ring.EndBatch();

        var headers = new SpanHeader[16];
        var apart   = new byte[]?[16];
        int n = ring.TryDequeueMany(headers, apart);
        Assert.Equal(shapes.Length, n);

        var batch = ring.Drained(headers.AsSpan(0, n), apart.AsSpan(0, n), new ServiceIndexCache());
        for (int i = 0; i < n; i++)
        {
            Assert.Equal(Fields(i).SpanId, batch.Header(i).SpanId);
            Assert.Equal(shapes[i].Name,    Encoding.UTF8.GetString(batch.NameUtf8(i)));
            Assert.Equal(shapes[i].Service, Encoding.UTF8.GetString(batch.ServiceUtf8(i)));
            Assert.True(shapes[i].Attrs.AsSpan().SequenceEqual(batch.Attributes(i)), $"attributes of shape {i}");
        }
        Assert.NotNull(apart[3]);                                        // the big one never touched the arena
        Assert.True(headers[3].PayloadArenaOffset <= -2);                // parked apart, by place

        ring.Release(headers.AsSpan(0, n));
        Assert.Equal(0, ring.BytesInFlight);
    }

    /// <summary>
    /// THE OLD RING'S "PUBLISHED HEAD WITH A NULL SLOT", PRESERVED. A producer that has CLAIMED a slot
    /// and not yet written it is parked; a later producer publishes the slot after it. The consumer
    /// must take what is before the gap, STOP at the gap — not skip it, not read it half-written, not
    /// spin on it — and take both, in order, once the gap is written.
    /// </summary>
    [Fact]
    public async Task A_claimed_but_unwritten_slot_stops_the_consumer_and_is_taken_in_order()
    {
        using var ring = new SpanRingBuffer(capacity: 16, maxBytes: 1024 * 1024);
        var claimed = new TaskCompletionSource();
        var release = new TaskCompletionSource();
        using var _ = Seam.ReleasedOnExit(release);

        Enqueue(ring, 0);                                                // published: position 0
        ring.EndBatch();   // on THIS thread: a batch begins and ends on one thread, and this test awaits

        ring._afterSlotClaimedForTest = pos =>
        {
            if (pos != 1) return;
            claimed.TrySetResult();
            release.Task.GetAwaiter().GetResult();                       // claimed, NOT written
        };
        var gap = Task.Run(() => { Enqueue(ring, 1); ring.EndBatch(); });
        await claimed.Task.WaitAsync(HangGuard);
        ring._afterSlotClaimedForTest = null;

        await Task.Run(() => { Enqueue(ring, 2); ring.EndBatch(); });   // published behind the gap: position 2

        var headers = new SpanHeader[8];
        var apart   = new byte[]?[8];
        int first = ring.TryDequeueMany(headers, apart);
        Assert.Equal(1, first);                                          // took 0, stopped at the gap
        Assert.Equal(new SpanId(1), headers[0].SpanId);
        ring.Release(headers.AsSpan(0, first));

        Assert.Equal(0, ring.TryDequeueMany(headers, apart));            // still stopped, still not skipped

        release.SetResult();
        await gap.WaitAsync(HangGuard);

        int rest = ring.TryDequeueMany(headers, apart);
        Assert.Equal(2, rest);
        Assert.Equal(new SpanId(2), headers[0].SpanId);                  // the gap, first
        Assert.Equal(new SpanId(3), headers[1].SpanId);                  // then what was behind it
        ring.Release(headers.AsSpan(0, rest));
        ring.EndBatch();
    }

    /// <summary>
    /// F2 — A FAULT BETWEEN CLAIM AND PUBLISH MUST NOT WEDGE THE RING. The consumer stops at a
    /// claimed-but-unwritten slot and waits for it; a producer that throws in that window (the
    /// oversize path used to allocate a dictionary node there — an OutOfMemoryException on a
    /// 512 MB host) left the slot claimed forever, the consumer stopped at it forever, the ring
    /// filled and every later span was refused until a restart. The seam throws at exactly that
    /// point, for an ordinary span and for one kept apart from the arena: the spans after it must
    /// still come out, and the budget must be whole again.
    /// </summary>
    [Theory]
    [InlineData(100)]
    [InlineData(SpanRingBuffer.ChunkBytes + 1_000)]
    public void A_fault_between_claim_and_publish_does_not_wedge_the_ring(int blobBytes)
    {
        using var ring = new SpanRingBuffer(capacity: 16, maxBytes: 4 * 1024 * 1024);
        ring._afterSlotClaimedForTest = pos => { if (pos == 1) throw new OutOfMemoryException("injected at the claim"); };

        Enqueue(ring, 0);
        var h = Fields(1);
        Assert.Throws<OutOfMemoryException>(() => ring.TryEnqueueRaw(in h, "op"u8, -1, "svc"u8, Filled(blobBytes)));
        for (int i = 2; i < 5; i++) Enqueue(ring, i);
        ring.EndBatch();
        ring._afterSlotClaimedForTest = null;

        var headers = new SpanHeader[16];
        var apart   = new byte[]?[16];
        int n = ring.TryDequeueMany(headers, apart);
        Assert.Equal(4, n);                                              // 0, 2, 3, 4 — the faulted one is skipped, not waited for
        Assert.Equal(new SpanId(1), headers[0].SpanId);
        Assert.Equal(new SpanId(3), headers[1].SpanId);
        Assert.Equal(new SpanId(5), headers[3].SpanId);
        ring.Release(headers.AsSpan(0, n));
        Assert.Equal(0, ring.BytesInFlight);                            // the faulted span gave its bytes back

        for (int round = 0; round < 40; round++)                         // and the ring keeps flowing past the lap
        {
            Enqueue(ring, 100 + round);
            ring.EndBatch();
            Assert.Equal(1, ring.TryDequeueMany(headers, apart));
            ring.Release(headers.AsSpan(0, 1));
        }
    }

    // ── F3: the idle trim ────────────────────────────────────────────────────

    private const int ChunkSpan = 40_000;   // one span per chunk: two would not fit in 64 KB

    private int EnqueueChunkSpans(SpanRingBuffer ring, int first, int count)
    {
        for (int i = first; i < first + count; i++)
        {
            var h = Fields(i);
            Assert.True(ring.TryEnqueueRaw(in h, "op"u8, -1, "svc"u8, Stamp(i)));
        }
        return count;
    }

    /// <summary>A payload whose bytes say which span it is, so a chunk given back under a span shows.</summary>
    private static byte[] Stamp(int i)
    {
        var a = new byte[ChunkSpan];
        for (int k = 0; k < a.Length; k++) a[k] = (byte)(i * 7 + k);
        return a;
    }

    /// <summary>
    /// F3 — A BURST IS GIVEN BACK ONCE THE RING IS IDLE. Sixty-four chunks of backlog, drained: the
    /// high-water mark used to be the arena's residency for the life of the process. The trim takes
    /// it down to the low-water mark, and the ring works across the given-back range afterwards —
    /// the chunks are committed again as they are reached, and every byte comes back as it went in.
    /// </summary>
    [Fact]
    public void A_burst_is_given_back_once_the_ring_is_idle_and_the_ring_still_works()
    {
        using var ring = new SpanRingBuffer(capacity: 1_024, maxBytes: 8 * 1024 * 1024);
        var headers = new SpanHeader[128];
        var apart   = new byte[]?[128];

        EnqueueChunkSpans(ring, 0, 64);
        ring.EndBatch();
        int n = ring.TryDequeueMany(headers, apart);
        ring.Release(headers.AsSpan(0, n));
        long before = ring.ArenaHighWaterBytes;

        long given = ring.TrimIdleArena();
        _out.WriteLine($"burst reached {before:N0} B; trim gave back {given:N0} B; high water now {ring.ArenaHighWaterBytes:N0} B");
        Assert.Equal(64L * SpanRingBuffer.ChunkBytes, before);
        if (TrimGivesBack)
        {
            Assert.True(given >= (64 - SpanRingBuffer.LowWaterChunks) * (long)SpanRingBuffer.ChunkBytes - Environment.SystemPageSize);
            Assert.Equal((long)SpanRingBuffer.LowWaterChunks * SpanRingBuffer.ChunkBytes, ring.ArenaHighWaterBytes);
        }
        else Assert.Equal(0, given);

        // Across the given-back range again: committed afresh, byte for byte.
        EnqueueChunkSpans(ring, 1_000, 64);
        ring.EndBatch();
        n = ring.TryDequeueMany(headers, apart);
        Assert.Equal(64, n);
        var batch = ring.Drained(headers.AsSpan(0, n), apart.AsSpan(0, n), new ServiceIndexCache());
        for (int i = 0; i < n; i++)
            Assert.True(Stamp(1_000 + i).AsSpan().SequenceEqual(batch.Attributes(i)), $"span {i} after the trim");
        ring.Release(headers.AsSpan(0, n));
        Assert.Equal(0, ring.RefusedNoArena);
    }

    /// <summary>
    /// Whether this platform's trim gives memory back at all: VirtualFree on Windows, madvise on the
    /// plain Linux allocation. Anywhere else the arena keeps its pages and the trim answers 0.
    /// </summary>
    private static bool TrimGivesBack => OperatingSystem.IsWindows() || OperatingSystem.IsLinux();

    /// <summary>
    /// THE TRIM NEVER GIVES BACK A CHUNK SOMETHING STILL USES. Sixty-four chunks drained, but the
    /// spans in chunks 10, 39 and 40 not yet released (the drainer is still copying them): the trim
    /// may give back only what lies ABOVE chunk 40, and those spans' bytes must read back whole —
    /// chunk 40 sits right against the cut, so a page rounded the wrong way at the boundary shows
    /// here. Released, the next trim takes the rest down to the low-water mark.
    ///
    /// <para>PLATFORM-AWARE, because the bytes given back are counted differently: exactly the 23
    /// chunks on Windows (the committed range is decommitted), from the cut to the END of the arena
    /// on Linux (madvise covers never-touched pages too), and nothing elsewhere. The high-water mark
    /// and the live bytes are the same claim everywhere.</para>
    /// </summary>
    [Fact]
    public void The_trim_never_gives_back_a_chunk_a_span_still_uses()
    {
        using var ring = new SpanRingBuffer(capacity: 1_024, maxBytes: 8 * 1024 * 1024);
        var headers = new SpanHeader[128];
        var apart   = new byte[]?[128];

        EnqueueChunkSpans(ring, 0, 64);
        ring.EndBatch();
        int n = ring.TryDequeueMany(headers, apart);
        Assert.Equal(64, n);

        int[] heldChunks = [10, 39, 40];
        var   held = new List<int>();
        for (int i = 0; i < n; i++)
        {
            if (Array.IndexOf(heldChunks, headers[i].PayloadArenaOffset / SpanRingBuffer.ChunkBytes) >= 0) held.Add(i);
            else ring.Release(headers.AsSpan(i, 1));                      // everything else
        }
        Assert.Equal(3, held.Count);

        long given = ring.TrimIdleArena();
        _out.WriteLine($"trim with chunks 10, 39, 40 in use gave back {given:N0} B");
        if (OperatingSystem.IsWindows())    Assert.Equal((64 - 41) * (long)SpanRingBuffer.ChunkBytes, given);
        else if (OperatingSystem.IsLinux()) Assert.True(given >= (64 - 41) * (long)SpanRingBuffer.ChunkBytes - Environment.SystemPageSize);
        else                                Assert.Equal(0, given);
        if (TrimGivesBack) Assert.Equal(41L * SpanRingBuffer.ChunkBytes, ring.ArenaHighWaterBytes);

        foreach (int i in held)
        {
            var batch = ring.Drained(headers.AsSpan(i, 1), apart.AsSpan(i, 1), new ServiceIndexCache());
            Assert.True(Stamp(i).AsSpan().SequenceEqual(batch.Attributes(0)),
                $"the span in chunk {headers[i].PayloadArenaOffset / SpanRingBuffer.ChunkBytes}, still in use, did not read back whole");
            ring.Release(headers.AsSpan(i, 1));
        }

        long rest = ring.TrimIdleArena();
        if (TrimGivesBack)
        {
            Assert.True(rest > 0);
            Assert.Equal((long)SpanRingBuffer.LowWaterChunks * SpanRingBuffer.ChunkBytes, ring.ArenaHighWaterBytes);
        }
    }

    /// <summary>
    /// A PRODUCER THAT MEETS A TRIM WAITS FOR IT, IT DOES NOT REFUSE. The trim holds the whole free
    /// list; a producer that needs a chunk in that moment finds the list empty. Parked inside the
    /// trim (seam), a producer on another thread asks for a chunk: it must be seen WAITING, and once
    /// the trim is done its span must be taken — not counted as refused for want of arena.
    /// </summary>
    [Fact]
    public async Task A_producer_that_meets_a_trim_waits_for_it_instead_of_refusing()
    {
        using var ring = new SpanRingBuffer(capacity: 1_024, maxBytes: 8 * 1024 * 1024);
        var headers = new SpanHeader[128];
        var apart   = new byte[]?[128];
        EnqueueChunkSpans(ring, 0, 32);
        ring.EndBatch();
        ring.Release(headers.AsSpan(0, ring.TryDequeueMany(headers, apart)));

        var waiting = new TaskCompletionSource();
        ring._onWaitingForTrimForTest = () => waiting.TrySetResult();
        Task<bool>? producer = null;
        Task? first = null;
        ring._whileTrimmingForTest = () =>
        {
            producer = Task.Run(() =>
            {
                var h = Fields(500);
                bool ok = ring.TryEnqueueRaw(in h, "op"u8, -1, "svc"u8, Stamp(500));
                ring.EndBatch();
                return ok;
            });
            first = Task.WhenAny(waiting.Task, producer).WaitAsync(HangGuard).GetAwaiter().GetResult();
        };

        ring.TrimIdleArena();
        ring._whileTrimmingForTest = null;
        ring._onWaitingForTrimForTest = null;

        Assert.Same(waiting.Task, first);                                // it waited, rather than answering
        Assert.True(await producer!.WaitAsync(HangGuard), "the span was refused because a trim held the free list");
        Assert.Equal(0, ring.RefusedNoArena);
        Assert.Equal(1, ring.TryDequeueMany(headers, apart));
        ring.Release(headers.AsSpan(0, 1));
    }

    /// <summary>
    /// A CHUNK IS REUSED ONLY WHEN EVERY SPAN IN IT IS DRAINED, AND THEN FIRST. Three spans in one chunk,
    /// one drained: a new batch must NOT be packed into that chunk. All drained: the next batch gets it
    /// back (LIFO), so a drainer that keeps up works in one chunk forever — the arena's residency is
    /// the deepest the backlog has been, not how much has passed through.
    /// </summary>
    [Fact]
    public void A_chunk_comes_back_only_when_its_last_span_is_drained_and_is_reused_first()
    {
        using var ring = new SpanRingBuffer(capacity: 64, maxBytes: 8 * 1024 * 1024);
        var headers = new SpanHeader[8];
        var apart   = new byte[]?[8];

        for (int i = 0; i < 3; i++) Enqueue(ring, i);
        ring.EndBatch();
        int c0 = ChunkOf(Peek(ring, headers, apart, take: 1));           // drains ONE of the three

        for (int i = 3; i < 4; i++) Enqueue(ring, i);
        ring.EndBatch();
        int n = ring.TryDequeueMany(headers, apart);
        Assert.Equal(3, n);                                               // the two left in c0, and the new one
        Assert.NotEqual(c0, ChunkOf(headers[2]));                         // the new one went elsewhere
        ring.Release(headers.AsSpan(0, n));

        // Everything drained: c0 and the other are free, and the LAST freed comes back first.
        for (int round = 0; round < 1_000; round++)
        {
            Enqueue(ring, 10 + round);
            ring.EndBatch();
            Assert.Equal(1, ring.TryDequeueMany(headers, apart));
            ring.Release(headers.AsSpan(0, 1));
        }
        _out.WriteLine($"1 000 rounds of a drainer keeping up: arena reached {ring.ArenaHighWaterBytes:N0} B");
        Assert.True(ring.ArenaHighWaterBytes <= 2 * SpanRingBuffer.ChunkBytes,
            $"a drainer that keeps up walked the arena to {ring.ArenaHighWaterBytes:N0} B");
    }

    /// <summary>
    /// DISPOSE DOES NOT FREE THE ARENA UNDER A PRODUCER. A request thread inside its batch is parked;
    /// the ring is disposed on another thread; the memory must still be there while the producer is
    /// inside, and freed once it ends its batch.
    /// </summary>
    [Fact]
    public async Task Dispose_frees_the_arena_only_after_the_last_open_batch_ends()
    {
        var ring = new SpanRingBuffer(capacity: 16, maxBytes: 1024 * 1024) { _producerWaitBudget = HangGuard };
        var inside  = new TaskCompletionSource();
        var endIt   = new TaskCompletionSource();
        using var _ = Seam.ReleasedOnExit(endIt);

        var producer = Task.Run(() =>
        {
            Enqueue(ring, 0);                                             // the batch is open
            inside.SetResult();
            endIt.Task.GetAwaiter().GetResult();
            ring.EndBatch();
        });
        await inside.Task.WaitAsync(HangGuard);

        // The seam fires from INSIDE the wait, so a teardown that does not wait never reaches it:
        // the disposer then finishes first, and that is the failure — no clock decides it.
        var waiting = new TaskCompletionSource();
        ring._onWaitingForProducersForTest = () => waiting.TrySetResult();
        var disposer = Task.Run(ring.Dispose);
        var first    = await Task.WhenAny(waiting.Task, disposer).WaitAsync(HangGuard);

        Assert.Same(waiting.Task, first);
        Assert.False(ring.FreedForTest, "the arena was freed with a producer still inside its batch");
        Assert.False(disposer.IsCompleted, "Dispose returned with a producer still inside its batch");

        endIt.SetResult();
        await producer.WaitAsync(HangGuard);
        await disposer.WaitAsync(HangGuard);
        Assert.True(ring.FreedForTest);
    }

    // ── the sink ────────────────────────────────────────────────────────────

    /// <summary>
    /// A RAW BATCH LANDS AS A PREFIX, exactly as an item batch always has: once one span is refused,
    /// every later span of the batch is refused too — even one that would have fit — and the batch
    /// is one refused request, reported at its end.
    /// </summary>
    [Fact]
    public void The_sink_takes_a_batch_as_a_prefix_and_reports_it_once()
    {
        using var ring = new SpanRingBuffer(capacity: 64, maxBytes: 30_000);
        var sink = new SpanIngestionEndpoint(ring, NullLogger<SpanIngestionEndpoint>.Instance);

        var big   = Filled(10_000);
        var small = Filled(10);
        int taken = 0;
        for (int i = 0; i < 5; i++) if (Raw(sink, i, big)) taken++;      // 2 fit, the 3rd is refused
        bool smallAfter = Raw(sink, 9, small);                           // would fit — refused all the same
        sink.EndBatch();

        Assert.Equal(2, taken);
        Assert.False(smallAfter);
        Assert.Equal(1, sink.RefusedRequests);
        Assert.Equal(4, sink.RefusedSpans);

        // A new batch starts clean.
        Assert.True(Raw(sink, 10, small));
        sink.EndBatch();
        Assert.Equal(1, sink.RefusedRequests);
    }

    // ── the raw path into the engine ────────────────────────────────────────

    /// <summary>
    /// THROUGH THE RING, THE DRAINER AND THE ENGINE, A SPAN ARRIVES AS IT WAS SENT: the tier holds its
    /// name, service and attribute bytes, the log holds the same, and the names are one shared string
    /// per distinct value — interned from UTF-8 on the drainer, never built per span.
    /// </summary>
    [Fact]
    public void Spans_through_the_ring_reach_the_log_and_the_tier_as_they_arrived()
    {
        var pools = new SpanStringPools();
        using var ring   = new SpanRingBuffer(capacity: 256, maxBytes: 4 * 1024 * 1024, pools);
        using var engine = new TraceStorageEngine(Dir("raw"), NullLogger<TraceStorageEngine>.Instance,
                                                  false, true, null, pools);
        var sink    = new SpanIngestionEndpoint(ring, NullLogger<SpanIngestionEndpoint>.Instance);
        var drainer = new SpanDrainer(ring, engine, NullLogger<SpanDrainer>.Instance, startLoop: false);

        byte[] svc = Encoding.UTF8.GetBytes("Wallet.API");
        int svcIdx = sink.InternService(svc);
        for (int i = 0; i < 200; i++)
        {
            var name = Encoding.UTF8.GetBytes(i % 2 == 0 ? "GET /orders/{id}" : "SELECT orders");
            Assert.True(sink.TryIngestRaw(new TraceId(7, (ulong)(i / 10 + 1)), new SpanId((ulong)(i + 1)), default,
                _baseNano + i, 1_000, name, svcIdx, svc, SpanKind.Server, SpanStatusCode.Ok, 200,
                Blob($"/orders/{i}", "GET")));
        }
        sink.EndBatch();

        while (drainer.DrainOnce(out _) > 0) { }

        var hot = engine.HotSpansForTest;
        var wal = engine.WalForTest.ReadAll();
        Assert.Equal(200, hot.Count);
        Assert.Equal(200, wal.Count);
        for (int i = 0; i < 200; i++)
        {
            string name = i % 2 == 0 ? "GET /orders/{id}" : "SELECT orders";
            Assert.Equal(name, hot[i].Name);
            Assert.Equal("Wallet.API", hot[i].ServiceName);
            Assert.True(Blob($"/orders/{i}", "GET").AsSpan().SequenceEqual(hot[i].AttributesBytes.Span));
            Assert.Equal((short)200, hot[i].HttpStatusCode);

            Assert.Equal(name, wal[i].Name);
            Assert.Equal("Wallet.API", wal[i].ServiceName);
            Assert.True(Blob($"/orders/{i}", "GET").AsSpan().SequenceEqual(wal[i].AttributesBytes));
        }
        Assert.Same(hot[0].Name, hot[2].Name);
        Assert.Same(hot[0].ServiceName, hot[199].ServiceName);
        Assert.Equal(0, ring.BytesInFlight);
    }

    /// <summary>
    /// A SATURATED POOL ON THE RAW PATH NEVER DROPS A SPAN. Pools of two names and one service, fifty
    /// distinct names and five services through the ring: every span is in the tier under its own name
    /// and service — the service index a full pool could not give (-1) and the name it could not pool
    /// both fall back to the bytes the span carried.
    /// </summary>
    [Fact]
    public void A_saturated_pool_on_the_raw_path_keeps_every_span_under_its_own_name()
    {
        var pools = new SpanStringPools(maxNames: 2, maxServices: 1);
        using var ring   = new SpanRingBuffer(capacity: 256, maxBytes: 4 * 1024 * 1024, pools);
        using var engine = new TraceStorageEngine(Dir("rawsat"), NullLogger<TraceStorageEngine>.Instance,
                                                  false, true, null, pools);
        var sink    = new SpanIngestionEndpoint(ring, NullLogger<SpanIngestionEndpoint>.Instance);
        var drainer = new SpanDrainer(ring, engine, NullLogger<SpanDrainer>.Instance, startLoop: false);

        for (int i = 0; i < 100; i++)
        {
            byte[] svc = Encoding.UTF8.GetBytes($"svc-{i % 5}");
            Assert.True(sink.TryIngestRaw(new TraceId(9, (ulong)(i + 1)), new SpanId((ulong)(i + 1)), default,
                _baseNano + i, 1_000, Encoding.UTF8.GetBytes($"GET /api/user/{i % 50}"),
                sink.InternService(svc), svc, SpanKind.Server, SpanStatusCode.Unset, 0, []));
        }
        sink.EndBatch();
        while (drainer.DrainOnce(out _) > 0) { }

        var hot = engine.HotSpansForTest;
        Assert.Equal(100, hot.Count);
        for (int i = 0; i < 100; i++)
        {
            Assert.Equal($"GET /api/user/{i % 50}", hot[i].Name);
            Assert.Equal($"svc-{i % 5}", hot[i].ServiceName);
        }
        Assert.True(pools.UnpooledNames > 0 && pools.UnpooledServices > 0, "the pools never saturated — the fact tests nothing");
    }

    /// <summary>
    /// THE ARENA'S LIFETIME IS NOT THE TIER'S, AND NO LOCK-FREE READER CAN TELL. The trace list's pass
    /// walks the unflushed records it captured AFTER letting go of the read lock (5de1c8f) and reads
    /// each root span's attribute bytes as it goes (the row's HTTP method and path). Park that pass
    /// mid-walk; then push a second wave of spans through the ring — the drainer releases the first
    /// wave's chunks and the second wave is written INTO THEM, byte offset for byte offset (asserted);
    /// then release the pass. Every row must still carry the first wave's method and path.
    ///
    /// <para>It holds because nothing the tier keeps points into the arena: the drainer copies each
    /// blob out (<c>DrainedSpans.AttributesForTier</c>) and interns each name and service, all before it
    /// releases the run. A record that kept a VIEW of the arena would read the second wave here.</para>
    /// </summary>
    [Fact]
    public async Task A_lock_free_pass_parked_mid_walk_reads_intact_records_while_the_arena_is_reused()
    {
        const int Traces = 100;
        var pools = new SpanStringPools();
        using var ring   = new SpanRingBuffer(capacity: 1_024, maxBytes: 4 * 1024 * 1024, pools);
        using var engine = new TraceStorageEngine(Dir("arena"), NullLogger<TraceStorageEngine>.Instance,
                                                  false, true, null, pools);
        var sink = new SpanIngestionEndpoint(ring, NullLogger<SpanIngestionEndpoint>.Instance);

        // Wave A: a root per trace carrying (GET, /a/NNNN), and a child.
        var aOffsets = new List<int>();
        Wave(sink, ring, engine, first: 0, method: "GET", pathPrefix: "/a/", aOffsets);

        var parked  = new TaskCompletionSource();
        var release = new TaskCompletionSource();
        using var _ = Seam.ReleasedOnExit(release);
        engine._aggregatePassForTest = name =>
        {
            if (name != nameof(TraceStorageEngine.GetTraceListAsync)) return;
            parked.TrySetResult();
            release.Task.GetAwaiter().GetResult();
        };
        var page = Task.Run(() => engine.GetTraceListAsync(Base.AddMinutes(-1), Base.AddDays(1),
                                                           null, null, null, null, null, Traces * 2));
        await parked.Task.WaitAsync(HangGuard);
        engine._aggregatePassForTest = null;

        // Wave B, while the pass is parked: same shapes, other bytes (PUT, /b/NNNN) — into the chunks
        // wave A just gave back.
        var bOffsets = new List<int>();
        Wave(sink, ring, engine, first: 10_000, method: "PUT", pathPrefix: "/b/", bOffsets);
        Assert.True(aOffsets.Intersect(bOffsets).Count() >= aOffsets.Count / 2,
            "wave B did not land on wave A's arena offsets — the fact would prove nothing");

        release.SetResult();
        var rows = (await page.WaitAsync(HangGuard)).Rows;

        _out.WriteLine($"{rows.Count} rows; wave B reused {aOffsets.Intersect(bOffsets).Count()} of wave A's "
                     + $"{aOffsets.Count} arena offsets while the pass was parked");
        Assert.Equal(Traces, rows.Count);
        foreach (var r in rows)
        {
            Assert.Equal("GET", r.HttpMethod);
            Assert.StartsWith("/a/", r.HttpPath);
        }
    }

    /// <summary>One wave of <see cref="Traces"/> two-span traces through sink, ring and engine, recording the arena offset of every span.</summary>
    private void Wave(SpanIngestionEndpoint sink, SpanRingBuffer ring, TraceStorageEngine engine,
                      int first, string method, string pathPrefix, List<int> offsets)
    {
        byte[] svc = Encoding.UTF8.GetBytes("gateway");
        int idx = sink.InternService(svc);
        for (int t = 0; t < 100; t++)
        {
            var trace = new TraceId(0xA7E4, (ulong)(first + t + 1));
            var root  = new SpanId((ulong)(first + 2 * t + 1));
            Assert.True(sink.TryIngestRaw(trace, root, default, _baseNano + (first + t) * 1_000_000L, 5_000_000,
                "GET /x"u8, idx, svc, SpanKind.Server, SpanStatusCode.Unset, 200, Blob($"{pathPrefix}{t:D4}", method)));
            Assert.True(sink.TryIngestRaw(trace, new SpanId((ulong)(first + 2 * t + 2)), root, _baseNano + (first + t) * 1_000_000L + 1,
                1_000_000, "SELECT"u8, idx, svc, SpanKind.Client, SpanStatusCode.Unset, 0, Blob($"{pathPrefix}{t:D4}", method)));
        }
        sink.EndBatch();

        // What the drainer does, with the offsets written down on the way.
        var headers = new SpanHeader[512];
        var apart   = new byte[]?[512];
        var cache   = new ServiceIndexCache();
        int n;
        while ((n = ring.TryDequeueMany(headers, apart)) > 0)
        {
            for (int i = 0; i < n; i++) offsets.Add(headers[i].PayloadArenaOffset);
            var batch = ring.Drained(headers.AsSpan(0, n), apart.AsSpan(0, n), cache);
            Assert.Equal(n, engine.WriteRaw(ref batch));
            ring.Release(headers.AsSpan(0, n));
        }
    }

    // ── fixtures ────────────────────────────────────────────────────────────

    private const int Traces = 100;

    private static int ChunkOf(in SpanHeader h) => h.PayloadArenaOffset / SpanRingBuffer.ChunkBytes;

    private static SpanHeader Peek(SpanRingBuffer ring, SpanHeader[] headers, byte[]?[] apart, int take)
    {
        int n = ring.TryDequeueMany(headers.AsSpan(0, take), apart.AsSpan(0, take));
        Assert.Equal(take, n);
        var first = headers[0];
        ring.Release(headers.AsSpan(0, n));
        return first;
    }

    private SpanHeader Fields(int i) => new()
    {
        TraceId           = new TraceId(0x5EED, (ulong)(i + 1)),
        SpanId            = new SpanId((ulong)(i + 1)),
        StartTimeUnixNano = _baseNano + i,
        DurationNanos     = 1_000,
        Kind              = SpanKind.Server,
    };

    private void Enqueue(SpanRingBuffer ring, int i)
    {
        var h = Fields(i);
        Assert.True(ring.TryEnqueueRaw(in h, "op"u8, -1, "svc"u8, Blob($"/r/{i}", "GET")));
    }

    private bool Raw(SpanIngestionEndpoint sink, int i, byte[] attrs) =>
        sink.TryIngestRaw(new TraceId(3, (ulong)(i + 1)), new SpanId((ulong)(i + 1)), default,
                          _baseNano + i, 1, "op"u8, -1, "svc"u8, SpanKind.Server, SpanStatusCode.Unset, 0, attrs);

    private static byte[] Filled(int n)
    {
        var a = new byte[n];
        for (int i = 0; i < n; i++) a[i] = (byte)(i * 31 + 7);
        return a;
    }

    /// <summary>A root span's HTTP attributes as the OTLP mapper would write them.</summary>
    private static byte[] Blob(string path, string method)
    {
        var buf = new System.Buffers.ArrayBufferWriter<byte>(64);
        var w   = new MessagePackWriter(buf);
        w.WriteMapHeader(2);
        w.Write("http.request.method"); w.Write(method);
        w.Write("url.path");            w.Write(path);
        w.Flush();
        return buf.WrittenMemory.ToArray();
    }
}
