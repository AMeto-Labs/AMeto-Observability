using Rd.Log.Core;
using Rd.Log.Storage;

namespace Rd.Log.Indexing;

/// <summary>
/// Builds all three index structures (inverted, trigram, bloom) from a sealed HotTierSegment.
///
/// Called during segment flush (after Freeze()) before writing the .seg file.
/// Returns the three byte arrays ready to pass to SegmentWriter.
/// </summary>
public sealed class SegmentIndexBuilder
{
    private readonly SegmentInvertedIndex _inverted = new();
    private readonly SegmentTrigramIndex  _trigram  = new();
    private readonly SegmentBloomFilter   _bloom;

    private readonly int _maxFlattenDepth;

    public SegmentIndexBuilder(int expectedEventCount, int maxFlattenDepth = 5)
    {
        _bloom            = SegmentBloomFilter.Create(expectedEventCount);
        _maxFlattenDepth  = maxFlattenDepth;
    }

    // ── Build ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// Iterates over all events in the hot tier and builds the three indexes.
    /// Must be called while <paramref name="hot"/> is frozen.
    /// </summary>
    public void Build(HotTierSegment hot, StringInternPool pool)
    {
        for (int i = 0; i < hot.Count; i++)
        {
            uint localOffset = (uint)i;
            ref readonly var header = ref hot.GetHeader(i);

            // Level — inverted + bloom
            string levelStr = header.Level.ToSeqString();
            _inverted.Add(localOffset, "@l", levelStr);
            _bloom.Add(levelStr);

            // Message template — trigram only.
            // Prefer the string captured at ingest time over the intern pool, so
            // the trigram index agrees with the @mt persisted by SegmentWriter
            // even if the pool entry was lost.
            string template = hot.GetTemplate(i) ?? pool.Get(header.MessageTemplatePoolIndex);
            if (!string.IsNullOrEmpty(template))
            {
                _trigram.Add(localOffset, template);
                _bloom.Add(template);
            }

            // Exception (structured) — inverted on @x.exists/@x.type/@x.inner.type
            // and trigram on @x.message for substring search.
            var exception = hot.GetException(i);
            if (exception is not null)
            {
                _inverted.Add(localOffset, "@x.exists", "true");
                _bloom.Add("@x.exists");

                if (!string.IsNullOrEmpty(exception.Type))
                {
                    _inverted.Add(localOffset, "@x.type", exception.Type);
                    _bloom.Add(exception.Type);
                    if (exception.Type.Length >= 3) _trigram.Add(localOffset, exception.Type);
                }
                if (!string.IsNullOrEmpty(exception.Message))
                {
                    if (exception.Message.Length >= 3) _trigram.Add(localOffset, exception.Message);
                }
                if (exception.Inner is { Type.Length: > 0 } inner)
                {
                    _inverted.Add(localOffset, "@x.inner.type", inner.Type);
                    _bloom.Add(inner.Type);
                }
            }

            // Properties — recursive flatten → inverted + bloom + trigram
            var props = hot.ReadPropertiesPayload(i, pool);
            if (props is not null)
                FlattenProperties(string.Empty, props, localOffset, depth: 0);
        }
    }

    // ── Recursive property flattening ─────────────────────────────────────────

    /// <summary>
    /// Recursively walks a property dictionary and adds flat dot-notation keys to all indexes.
    /// Arrays are expanded element-by-element; nested maps are walked with depth guard.
    /// Example: { User: { Name: "Alice", Roles: ["Admin"] } }
    ///   → ("User.Name", "Alice"), ("User.Roles", "Admin")
    /// </summary>
    private void FlattenProperties(
        string prefix,
        Dictionary<string, object?> dict,
        uint offset,
        int depth)
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
                // Recurse into nested map — do NOT add the map itself as a value
                FlattenProperties(flatKey, nested, offset, depth + 1);
                break;

            case object[] arr:
                // Expand array elements individually
                foreach (var item in arr)
                    FlattenValue(flatKey, item, offset, depth);
                break;

            default:
                // Scalar — add to all indexes
                _inverted.Add(offset, flatKey, v);
                _bloom.Add(flatKey);
                string valStr = v?.ToString() ?? string.Empty;
                _bloom.Add(valStr);
                if (v is string strVal && strVal.Length >= 3)
                    _trigram.Add(offset, strVal);
                break;
        }
    }

    // ── Serialise ─────────────────────────────────────────────────────────────

    public byte[] SerialisedInvertedIndex  => _inverted.Serialise();
    public byte[] SerialisedTrigramIndex   => _trigram.Serialise();
    public byte[] SerialisedBloomFilter    => _bloom.Serialise();
}
