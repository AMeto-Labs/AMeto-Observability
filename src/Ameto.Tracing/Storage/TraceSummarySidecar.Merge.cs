using System.Buffers;
using System.Buffers.Binary;
using System.Text;
using K4os.Compression.LZ4;

namespace Ameto.Tracing.Storage;

internal static partial class TraceSummarySidecar
{
    /// <summary>
    /// THE <c>.tracesum</c> OF A STREAMING MERGE (<see cref="SpanWriter.WriteMerged"/>): the same file
    /// format as <see cref="WriteOrdered"/>, built from spans that arrive one at a time in start order,
    /// without ever holding the merge's whole span set.
    ///
    /// <para><b>WHY NOT ONE ACCUMULATOR PER TRACE, AS THE FLUSH HAS.</b> The flush holds its whole batch
    /// anyway, so a dictionary of every trace in it costs nothing extra. A streaming merge exists to stop
    /// holding the batch, and a per-trace map of a million-span output is ~130 B a trace on the heap —
    /// the very thing the merge was rewritten to remove. So a trace is accumulated while it is LIVE and
    /// written out as a row once the stream has moved <see cref="WindowNanos"/> past its newest span (or
    /// once more than <see cref="MaxActive"/> traces are live, oldest first). Spans of one trace sit
    /// within milliseconds of each other in the ordinary case, so almost every trace still becomes
    /// exactly one row.</para>
    ///
    /// <para><b>A TRACE THAT OUTLIVES THE WINDOW BECOMES TWO ROWS</b>, and that is the shape the readers
    /// already take: every cold walk (<c>MergeSummaryInto</c>) merges rows BY TRACE ID, because a trace
    /// that straddled two flushes has always been two rows in two segments. The span counts add, the
    /// error flags OR, the service sets union, and the first root wins — the same answer as one row. The
    /// one figure that sees two rows is the volume histogram, which counts such a trace in each — exactly
    /// as it did before the merge, when the two rows lived in two files.</para>
    ///
    /// <para><b>THE SERVICE POOL IS SEEDED FROM THE SOURCES' SERVICE INDEXES</b>, so the body can be
    /// written front to back into ONE buffer: pool first, rows behind it. Every span's service is in its
    /// own segment's service index, so a seeded pool never misses on a file this engine wrote; if it ever
    /// does, the name is appended and the body is reassembled once, at the end.</para>
    /// </summary>
    internal sealed class MergeBuilder : IDisposable
    {
        /// <summary>How far past a trace's newest span the stream may move before the trace becomes a row.</summary>
        public long WindowNanos { get; }

        /// <summary>The most traces held open at once; past it the least recently seen becomes a row early.</summary>
        public int MaxActive { get; }

        private readonly Dictionary<TraceId, Acc>      _active;
        private readonly PriorityQueue<(Acc Acc, int Gen), long> _byLastSeen = new();
        private readonly Stack<Acc>                    _free = new();

        private readonly Dictionary<string, int>       _pool = new(StringComparer.Ordinal);
        private readonly List<string>                  _poolArr = [];
        private readonly int                           _seededCount;

        private readonly Dictionary<long, VolCell>     _vol = new();
        private Body _body;
        private readonly int  _rowsStart;
        private uint _rowCount;
        private long _segMin = long.MaxValue, _segMax = long.MinValue;

        /// <param name="seedServices">Every service the merged spans can carry — the union of the sources' service indexes.</param>
        public MergeBuilder(IEnumerable<string> seedServices, long windowNanos, int maxActive, SpanWriteScratch? scratch)
        {
            WindowNanos = windowNanos;
            MaxActive   = Math.Max(1, maxActive);
            _active     = new Dictionary<TraceId, Acc>(Math.Min(MaxActive, 4_096));

            foreach (var s in seedServices) Intern(_pool, _poolArr, s);
            _seededCount = _poolArr.Count;

            _body = new Body(scratch);
            _body.UInt32((uint)_poolArr.Count);
            foreach (var name in _poolArr) _body.Utf8(name, prefixBytes: 2, maxBytes: int.MaxValue);
            _body.UInt32(0);                     // the row count, patched when the rows are done
            _rowsStart = _body.Length;
        }

        /// <summary>Rows written so far — the traces finished, not the spans seen.</summary>
        public uint Rows => _rowCount;

        /// <summary>Traces held open right now.</summary>
        public int Active => _active.Count;

        /// <summary>
        /// Takes one span. Spans must arrive in non-decreasing start order — the merge's own order —
        /// because "the stream has moved past this trace" is read off the span in hand.
        /// </summary>
        public void Add(SpanRecord s)
        {
            long ts = s.StartTimeUnixNano;
            if (ts < _segMin) _segMin = ts;
            if (ts > _segMax) _segMax = ts;

            if (!_active.TryGetValue(s.TraceId, out var a))
            {
                a = _free.Count > 0 ? _free.Pop() : new Acc();
                a.Reset(s.TraceId);
                _active[s.TraceId] = a;
                _byLastSeen.Enqueue((a, a.Gen), ts);
            }

            a.SpanCount++;
            a.LastSeen = ts;
            if (s.Status == SpanStatusCode.Error) a.HasError = true;
            a.AddService(s.ServiceName);
            if (ts < a.EarliestNano)
            {
                a.EarliestNano = ts;
                a.FirstService = s.ServiceName;
            }

            // First empty-parent span wins the root slot — the flush's rule, read off the merge's order.
            if (s.ParentSpanId.IsEmpty && !a.HasRoot)
            {
                a.HasRoot        = true;
                a.RootSpanId     = s.SpanId;
                a.RootStartNano  = ts;
                a.RootDurNanos   = s.DurationNanos;
                a.RootStatus     = s.Status;
                a.RootHttpStatus = s.HttpStatusCode;
                a.RootName       = s.Name;
                a.RootService    = s.ServiceName;
                HttpSemconvKeys.Resolve(s, out a.RootMethod, out a.RootPath);
            }

            Expire(ts);
        }

        /// <summary>Turns every trace the stream has moved past into a row.</summary>
        private void Expire(long now)
        {
            while (_byLastSeen.TryPeek(out var e, out long key))
            {
                var (acc, gen) = e;
                if (acc.Gen != gen) { _byLastSeen.Dequeue(); continue; }          // already a row
                if (acc.LastSeen != key)                                           // seen since: re-key it
                {
                    _byLastSeen.Dequeue();
                    _byLastSeen.Enqueue(e, acc.LastSeen);
                    continue;
                }
                bool due = now - key > WindowNanos || _active.Count > MaxActive;
                if (!due) break;
                _byLastSeen.Dequeue();
                Finish(acc);
            }
        }

        private void Finish(Acc a)
        {
            _active.Remove(a.TraceId);
            WriteRow(a);
            a.Gen++;
            a.Release();
            _free.Push(a);
        }

        private void WriteRow(Acc a)
        {
            Span<byte> tid = stackalloc byte[16];
            a.TraceId.WriteTo(tid);
            _body.Bytes(tid);
            _body.UInt64(a.RootSpanId.RawValue);
            _body.Int64(a.HasRoot ? a.RootStartNano : a.EarliestNano);
            _body.Int64(a.HasRoot ? a.RootDurNanos  : 0L);
            _body.UInt32(a.SpanCount);

            byte flags = 0;
            if (a.HasRoot)  flags |= 0b01;
            if (a.HasError) flags |= 0b10;
            _body.Byte(flags);
            _body.Byte((byte)a.RootStatus);
            _body.Int16(a.RootHttpStatus);
            _body.Int32(Intern(_pool, _poolArr, a.HasRoot ? a.RootService : a.FirstService));

            _body.Utf8(a.HasRoot ? a.RootName   : string.Empty, prefixBytes: 2, maxBytes: ushort.MaxValue);
            _body.Utf8(a.HasRoot ? a.RootMethod : string.Empty, prefixBytes: 1, maxBytes: byte.MaxValue);
            _body.Utf8(a.HasRoot ? a.RootPath   : string.Empty, prefixBytes: 2, maxBytes: ushort.MaxValue);

            int svcCount = a.ServiceCount;
            _body.UInt16((ushort)svcCount);
            for (int i = 0; i < svcCount; i++) _body.Int32(Intern(_pool, _poolArr, a.ServiceAt(i)));

            long grid = (a.HasRoot ? a.RootStartNano : a.EarliestNano) / GridNanos;
            _vol.TryGetValue(grid, out var cell);
            cell.Traces++;
            if (a.HasError) cell.Errors++;
            _vol[grid] = cell;

            _rowCount++;
        }

        /// <summary>
        /// Writes every trace still open as a row and the file to <paramref name="path"/>, fsynced.
        /// Writes nothing when no span was ever added — the flush's rule for an empty batch.
        /// </summary>
        public void Write(string path)
        {
            if (_rowCount == 0 && _active.Count == 0) return;

            // Oldest first, so the rows keep the stream's order.
            while (_byLastSeen.TryDequeue(out var e, out _))
                if (e.Acc.Gen == e.Gen) Finish(e.Acc);

            ReadOnlySpan<byte> body;
            byte[]? reassembled = null;
            try
            {
                if (_poolArr.Count == _seededCount)
                {
                    BinaryPrimitives.WriteUInt32LittleEndian(_body.At(_rowsStart - 4), _rowCount);
                    body = _body.Written;
                }
                else
                {
                    // A service the sources' indexes did not name — the pool grew behind the rows, so
                    // the body is put back together once: the whole pool, the count, the rows.
                    var tail = new Body(null);
                    tail.UInt32((uint)_poolArr.Count);
                    foreach (var name in _poolArr) tail.Utf8(name, prefixBytes: 2, maxBytes: int.MaxValue);
                    tail.UInt32(_rowCount);
                    tail.Bytes(_body.Written[_rowsStart..]);
                    reassembled = tail.Detach(out int len);
                    body = reassembled.AsSpan(0, len);
                }

                int    rawLength = body.Length;
                byte[] compBody  = LZ4Pickler.Pickle(body);

                using var fs = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None, 65536);
                using var w  = new BinaryWriter(fs);

                w.Write(Magic);
                w.Write(Version);
                w.Write(_segMin);
                w.Write(_segMax);

                w.Write((uint)_vol.Count);
                foreach (var (grid, cell) in _vol)
                {
                    w.Write(grid);
                    w.Write(cell.Traces);
                    w.Write(cell.Errors);
                }

                w.Write((uint)rawLength);
                w.Write((uint)compBody.Length);
                w.Write(compBody);

                w.Flush();
                fs.Flush(flushToDisk: true);
            }
            finally
            {
                if (reassembled is not null) ArrayPool<byte>.Shared.Return(reassembled);
            }
        }

        public void Dispose() => _body.Dispose();

        /// <summary>One live trace. Pooled: a merge reuses the few hundred it ever has open at once.</summary>
        private sealed class Acc
        {
            public int            Gen;
            public TraceId        TraceId;
            public uint           SpanCount;
            public bool           HasError;
            public long           EarliestNano;
            public long           LastSeen;
            public string         FirstService = string.Empty;

            public bool           HasRoot;
            public SpanId         RootSpanId;
            public long           RootStartNano;
            public long           RootDurNanos;
            public SpanStatusCode RootStatus;
            public short          RootHttpStatus;
            public string         RootName    = string.Empty;
            public string         RootService = string.Empty;
            public string         RootMethod  = string.Empty;
            public string         RootPath    = string.Empty;

            // The trace's services, distinct by ordinal, in the order first met — the order the
            // flush's HashSet enumerated them in (it is never removed from).
            private string?       _svc0, _svc1, _svc2;
            private List<string>? _more;

            public void Reset(TraceId id)
            {
                TraceId      = id;
                SpanCount    = 0;
                HasError     = false;
                EarliestNano = long.MaxValue;
                LastSeen     = long.MinValue;
                FirstService = string.Empty;
                HasRoot      = false;
                RootSpanId   = default;
                RootStartNano = RootDurNanos = 0;
                RootStatus   = default;
                RootHttpStatus = 0;
                RootName = RootService = RootMethod = RootPath = string.Empty;
                _svc0 = _svc1 = _svc2 = null;
                _more?.Clear();
            }

            /// <summary>Drops the strings a finished row no longer needs, so a pooled slot pins nothing.</summary>
            public void Release()
            {
                FirstService = RootName = RootService = RootMethod = RootPath = string.Empty;
                _svc0 = _svc1 = _svc2 = null;
                _more?.Clear();
            }

            public void AddService(string service)
            {
                if (_svc0 is null)                                          { _svc0 = service; return; }
                if (string.Equals(_svc0, service, StringComparison.Ordinal)) return;
                if (_svc1 is null)                                          { _svc1 = service; return; }
                if (string.Equals(_svc1, service, StringComparison.Ordinal)) return;
                if (_svc2 is null)                                          { _svc2 = service; return; }
                if (string.Equals(_svc2, service, StringComparison.Ordinal)) return;
                _more ??= [];
                foreach (var m in _more) if (string.Equals(m, service, StringComparison.Ordinal)) return;
                _more.Add(service);
            }

            public int ServiceCount =>
                (_svc0 is null ? 0 : _svc1 is null ? 1 : _svc2 is null ? 2 : 3) + (_more?.Count ?? 0);

            public string ServiceAt(int i) => i switch
            {
                0 => _svc0!,
                1 => _svc1!,
                2 => _svc2!,
                _ => _more![i - 3],
            };
        }

        /// <summary>
        /// The body under construction: one <see cref="ArrayPool{T}"/> rental, grown by doubling,
        /// little-endian throughout — <see cref="PooledBody"/>'s encoding, in a class, because this
        /// one lives across a whole merge rather than inside one call.
        /// </summary>
        private sealed class Body(SpanWriteScratch? scratch)
        {
            private byte[] _buf = scratch?.RentBody(64 * 1024) ?? ArrayPool<byte>.Shared.Rent(64 * 1024);
            private int    _len;

            public int                Length  => _len;
            public ReadOnlySpan<byte> Written => _buf.AsSpan(0, _len);
            public Span<byte>         At(int offset) => _buf.AsSpan(offset);

            public void Byte  (byte v)   => Take(1)[0] = v;
            public void UInt16(ushort v) => BinaryPrimitives.WriteUInt16LittleEndian(Take(2), v);
            public void Int16 (short v)  => BinaryPrimitives.WriteInt16LittleEndian (Take(2), v);
            public void UInt32(uint v)   => BinaryPrimitives.WriteUInt32LittleEndian(Take(4), v);
            public void Int32 (int v)    => BinaryPrimitives.WriteInt32LittleEndian (Take(4), v);
            public void UInt64(ulong v)  => BinaryPrimitives.WriteUInt64LittleEndian(Take(8), v);
            public void Int64 (long v)   => BinaryPrimitives.WriteInt64LittleEndian (Take(8), v);
            public void Bytes (scoped ReadOnlySpan<byte> v) => v.CopyTo(Take(v.Length));

            /// <summary>The flush's string rule exactly — see <see cref="PooledBody.Utf8"/>.</summary>
            public void Utf8(string s, int prefixBytes, int maxBytes)
            {
                var dst = Reserve(prefixBytes + Encoding.UTF8.GetMaxByteCount(s.Length));
                int n   = Encoding.UTF8.GetBytes(s, dst[prefixBytes..]);
                if (n > maxBytes) n = maxBytes;
                if (prefixBytes == 1) dst[0] = (byte)n;
                else                  BinaryPrimitives.WriteUInt16LittleEndian(dst, (ushort)n);
                _len += prefixBytes + n;
            }

            /// <summary>Hands the buffer over to the caller, who returns it to the shared pool.</summary>
            public byte[] Detach(out int length)
            {
                var buf = _buf;
                length = _len;
                _buf = [];
                _len = 0;
                return buf;
            }

            public void Dispose()
            {
                if (_buf.Length == 0) return;
                if (scratch is not null) scratch.ReturnBody(_buf);
                else                     ArrayPool<byte>.Shared.Return(_buf);
                _buf = [];
                _len = 0;
            }

            private Span<byte> Take(int n)
            {
                var dst = Reserve(n)[..n];
                _len += n;
                return dst;
            }

            private Span<byte> Reserve(int n)
            {
                if (_buf.Length - _len < n)
                {
                    var next = ArrayPool<byte>.Shared.Rent(Math.Max(_buf.Length * 2, _len + n));
                    _buf.AsSpan(0, _len).CopyTo(next);
                    if (_buf.Length > 0) ArrayPool<byte>.Shared.Return(_buf);
                    _buf = next;
                }
                return _buf.AsSpan(_len);
            }
        }
    }
}
