using System.Buffers;
using MessagePack;
using Ameto.Core;
using Ameto.Storage;

namespace Ameto.Indexing;

/// <summary>
/// Builds all three index structures (inverted, trigram, bloom) from a sealed HotTierSegment.
///
/// Called during segment flush (after Freeze()) before writing the .seg file. The property
/// walk reads each event's msgpack payload with a streaming <see cref="MessagePackReader"/> and
/// feeds the indexes directly — no per-event <c>Dictionary</c>, no boxing, no per-attribute
/// strings. This is the flush-path allocation hot spot (index build was ~16 KB/event); the
/// streaming walk is byte-parity with the old dictionary path (see <see cref="BuildReference"/>,
/// exercised by the parity test).
/// </summary>
public sealed class SegmentIndexBuilder
{
    private readonly SegmentInvertedIndex _inverted = new();
    private readonly SegmentTrigramIndex  _trigram  = new();
    private readonly SegmentBloomFilter   _bloom;

    private readonly int _maxFlattenDepth;

    // Per-build scratch (Build is single-threaded per flush). Grown on demand.
    private byte[] _mp  = new byte[512];   // payload copy for MessagePackReader (needs a sequence)
    private char[] _key = new char[256];   // accumulated flat (dot-notation) key
    private char[] _val = new char[128];   // formatted value (serialised form, prefix at [0..2])

    public SegmentIndexBuilder(int expectedEventCount, int maxFlattenDepth = 5)
    {
        _bloom            = SegmentBloomFilter.Create(expectedEventCount);
        _maxFlattenDepth  = maxFlattenDepth;
    }

    // ── Build ─────────────────────────────────────────────────────────────────

    /// <summary>Streaming (zero-alloc) build. Must be called while <paramref name="hot"/> is frozen.</summary>
    public void Build(HotTierSegment hot, StringInternPool pool)
    {
        for (int i = 0; i < hot.Count; i++)
        {
            uint offset = (uint)i;
            ref readonly var header = ref hot.GetHeader(i);
            IndexHeaderFields(in header, i, offset, hot, pool);
            IndexPropertiesStreaming(hot.GetPropertiesPayload(i), offset);
        }
    }

    /// <summary>
    /// Reference build via the old per-event <c>Dictionary</c> path. Kept only as the
    /// correctness oracle for the streaming-parity test; not used in production.
    /// </summary>
    public void BuildReference(HotTierSegment hot, StringInternPool pool)
    {
        for (int i = 0; i < hot.Count; i++)
        {
            uint offset = (uint)i;
            ref readonly var header = ref hot.GetHeader(i);
            IndexHeaderFields(in header, i, offset, hot, pool);

            var props = hot.ReadPropertiesPayload(i, pool);
            if (props is not null)
                FlattenProperties(string.Empty, props, offset, depth: 0);
        }
    }

    // ── Per-event header fields (shared by both paths) ─────────────────────────
    private void IndexHeaderFields(in LogEventHeader header, int i, uint offset, HotTierSegment hot, StringInternPool pool)
    {
        // Level — inverted + bloom
        string levelStr = header.Level.ToSeqString();
        _inverted.Add(offset, "@l", levelStr);
        _bloom.Add(levelStr);

        // Message template — trigram only.
        string template = hot.GetTemplate(i) ?? pool.Get(header.MessageTemplatePoolIndex);
        if (!string.IsNullOrEmpty(template))
        {
            _trigram.Add(offset, template);
            _bloom.Add(template);
        }

        // Exception (structured)
        var exception = hot.GetException(i);
        if (exception is not null)
        {
            _inverted.Add(offset, "@x.exists", "true");
            _bloom.Add("@x.exists");

            if (!string.IsNullOrEmpty(exception.Type))
            {
                _inverted.Add(offset, "@x.type", exception.Type);
                _bloom.Add(exception.Type);
                if (exception.Type.Length >= 3) _trigram.Add(offset, exception.Type);
            }
            if (!string.IsNullOrEmpty(exception.Message) && exception.Message.Length >= 3)
                _trigram.Add(offset, exception.Message);
            if (exception.Inner is { Type.Length: > 0 } inner)
            {
                _inverted.Add(offset, "@x.inner.type", inner.Type);
                _bloom.Add(inner.Type);
            }
        }

        // TraceId / SpanId
        if (header.HasTraceId)
        {
            string traceHex = TraceIdHelper.FormatTraceId(header.TraceIdHi, header.TraceIdLo)!;
            _inverted.Add(offset, ClefFields.TraceId, traceHex);
            _bloom.Add(traceHex);
        }
        if (header.HasSpanId)
        {
            string spanHex = TraceIdHelper.FormatSpanId(header.SpanId)!;
            _inverted.Add(offset, ClefFields.SpanId, spanHex);
            _bloom.Add(spanHex);
        }

        // ServiceName
        if (header.ServiceNamePoolIndex >= 0)
        {
            string svcName = pool.Get(header.ServiceNamePoolIndex);
            if (!string.IsNullOrEmpty(svcName))
            {
                _inverted.Add(offset, ClefFields.ServiceName, svcName);
                _bloom.Add(svcName);
            }
        }
    }

    // ── Streaming property walk (msgpack → indexes, no Dictionary/boxing) ───────
    private void IndexPropertiesStreaming(ReadOnlySpan<byte> payload, uint offset)
    {
        if (payload.IsEmpty) return;
        if (payload.Length > _mp.Length) _mp = new byte[Math.Max(payload.Length, _mp.Length * 2)];
        payload.CopyTo(_mp);
        var reader = new MessagePackReader(new ReadOnlySequence<byte>(_mp, 0, payload.Length));
        try { WalkMap(ref reader, 0, offset, 0); }
        catch { /* malformed payload — index what we could, mirror old try/catch tolerance */ }
    }

    private void WalkMap(ref MessagePackReader reader, int prefixLen, uint offset, int depth)
    {
        if (depth > _maxFlattenDepth) { reader.Skip(); return; }
        int count = reader.ReadMapHeader();
        for (int e = 0; e < count; e++)
        {
            ReadOnlySpan<byte> keyUtf8 = ReadStr(ref reader);
            int keyChars = System.Text.Encoding.UTF8.GetCharCount(keyUtf8);
            EnsureKey(prefixLen + keyChars + 1);
            System.Text.Encoding.UTF8.GetChars(keyUtf8, _key.AsSpan(prefixLen));
            WalkValue(ref reader, prefixLen + keyChars, offset, depth);
        }
    }

    private void WalkValue(ref MessagePackReader reader, int flatLen, uint offset, int depth)
    {
        switch (reader.NextMessagePackType)
        {
            case MessagePackType.Map:
                EnsureKey(flatLen + 1);
                _key[flatLen] = ClefFields.PropertyPathSeparator;
                WalkMap(ref reader, flatLen + 1, offset, depth + 1);
                break;

            case MessagePackType.Array:
                int n = reader.ReadArrayHeader();
                for (int i = 0; i < n; i++) WalkValue(ref reader, flatLen, offset, depth);
                break;

            default:
                AddScalar(ref reader, flatLen, offset);
                break;
        }
    }

    private void AddScalar(ref MessagePackReader reader, int flatLen, uint offset)
    {
        var flatKey = _key.AsSpan(0, flatLen);

        switch (reader.NextMessagePackType)
        {
            case MessagePackType.String:
            {
                ReadOnlySpan<byte> vUtf8 = ReadStr(ref reader);
                int vc = System.Text.Encoding.UTF8.GetCharCount(vUtf8);
                EnsureVal(vc);
                System.Text.Encoding.UTF8.GetChars(vUtf8, _val);
                var v = _val.AsSpan(0, vc);
                _inverted.AddSpan(offset, flatKey, v);   // serialised == plain for strings
                _bloom.Add(flatKey);
                _bloom.Add(v);
                if (vc >= 3) _trigram.Add(offset, v);
                break;
            }
            case MessagePackType.Integer:
            {
                if (reader.NextCode == MessagePackCode.UInt64)
                {
                    ulong u = reader.ReadUInt64();
                    if (u > (ulong)long.MaxValue) { WriteUnsigned(u, out var pl, out var sr); AddNumeric(offset, flatKey, pl, sr); break; }
                    AddLong((long)u, offset, flatKey); break;
                }
                AddLong(reader.ReadInt64(), offset, flatKey);
                break;
            }
            case MessagePackType.Float:
            {
                double d = reader.ReadDouble();
                // serialised = "\0d" + R-format; plain = same digits (default ToString == R in modern .NET).
                _val[0] = '\0'; _val[1] = 'd';
                EnsureVal(2 + 40);
                d.TryFormat(_val.AsSpan(2), out int w, "R", System.Globalization.CultureInfo.CurrentCulture);
                AddNumeric(offset, flatKey, _val.AsSpan(2, w), _val.AsSpan(0, 2 + w));
                break;
            }
            case MessagePackType.Boolean:
            {
                bool b = reader.ReadBoolean();
                if (b) { _bloom.Add(flatKey); _bloom.Add("True");  _inverted.AddSpan(offset, flatKey, "\0true");  }
                else   { _bloom.Add(flatKey); _bloom.Add("False"); _inverted.AddSpan(offset, flatKey, "\0false"); }
                break;
            }
            case MessagePackType.Nil:
            {
                reader.ReadNil();
                _bloom.Add(flatKey);
                _bloom.Add(ReadOnlySpan<char>.Empty);          // v?.ToString() ?? "" → ""
                _inverted.AddSpan(offset, flatKey, "\0null");
                break;
            }
            default:
                reader.Skip();
                break;
        }
    }

    private void AddLong(long l, uint offset, ReadOnlySpan<char> flatKey)
    {
        _val[0] = '\0'; _val[1] = 'l';
        EnsureVal(2 + 24);
        l.TryFormat(_val.AsSpan(2), out int w, default, System.Globalization.CultureInfo.CurrentCulture);
        AddNumeric(offset, flatKey, _val.AsSpan(2, w), _val.AsSpan(0, 2 + w));
    }

    private void WriteUnsigned(ulong u, out ReadOnlySpan<char> plain, out ReadOnlySpan<char> serialised)
    {
        // ulong > long.Max: SerialiseValue default → plain ToString(), no prefix.
        EnsureVal(24);
        u.TryFormat(_val, out int w, default, System.Globalization.CultureInfo.CurrentCulture);
        plain = serialised = _val.AsSpan(0, w);
    }

    private void AddNumeric(uint offset, ReadOnlySpan<char> flatKey, ReadOnlySpan<char> plain, ReadOnlySpan<char> serialised)
    {
        _inverted.AddSpan(offset, flatKey, serialised);
        _bloom.Add(flatKey);
        _bloom.Add(plain);
    }

    private static ReadOnlySpan<byte> ReadStr(ref MessagePackReader reader)
        => reader.TryReadStringSpan(out ReadOnlySpan<byte> span) ? span : ReadStrSlow(ref reader);

    private static byte[] _empty = System.Array.Empty<byte>();
    private static ReadOnlySpan<byte> ReadStrSlow(ref MessagePackReader reader)
    {
        // Rare: string spans buffer segments. Our payload is one array, so this is unreachable,
        // but keep it correct — materialise once.
        var seq = reader.ReadStringSequence();
        return seq.HasValue ? seq.Value.ToArray() : _empty;
    }

    private void EnsureKey(int len) { if (len > _key.Length) System.Array.Resize(ref _key, Math.Max(len, _key.Length * 2)); }
    private void EnsureVal(int len) { if (len > _val.Length) System.Array.Resize(ref _val, Math.Max(len, _val.Length * 2)); }

    // ── Reference recursive flatten (used only by BuildReference) ──────────────
    private void FlattenProperties(string prefix, Dictionary<string, object?> dict, uint offset, int depth)
    {
        if (depth > _maxFlattenDepth) return;
        foreach (var (k, v) in dict)
        {
            string flatKey = prefix.Length == 0 ? k : string.Concat(prefix, ClefFields.PropertyPathSeparator, k);
            FlattenValue(flatKey, v, offset, depth);
        }
    }

    private void FlattenValue(string flatKey, object? v, uint offset, int depth)
    {
        switch (v)
        {
            case Dictionary<string, object?> nested:
                FlattenProperties(flatKey, nested, offset, depth + 1);
                break;
            case object[] arr:
                foreach (var item in arr) FlattenValue(flatKey, item, offset, depth);
                break;
            default:
                _inverted.Add(offset, flatKey, v);
                _bloom.Add(flatKey);
                string valStr = v?.ToString() ?? string.Empty;
                _bloom.Add(valStr);
                if (v is string strVal && strVal.Length >= 3) _trigram.Add(offset, strVal);
                break;
        }
    }

    // ── Serialise ─────────────────────────────────────────────────────────────

    public byte[] SerialisedInvertedIndex  => _inverted.Serialise();
    public byte[] SerialisedTrigramIndex   => _trigram.Serialise();
    public byte[] SerialisedBloomFilter    => _bloom.Serialise();
}
