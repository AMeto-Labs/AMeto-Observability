using System.Collections.Concurrent;
using System.Text.Json.Serialization;

namespace Ameto.Server;

// ── Wire DTOs (source-generated JSON) ──────────────────────────────────────────

/// <summary>One time series in the counts response: a service or a level plus its per-bucket points.</summary>
internal sealed class CountSeriesDto
{
    /// <summary>Service name — omitted for level series.</summary>
    public string? Service { get; init; }
    /// <summary>Level name — omitted for service series.</summary>
    public string? Level   { get; init; }
    public long    Count   { get; init; }
    /// <summary>One value per bucket, aligned with <see cref="EventCountsResponse.Buckets"/>.</summary>
    public long[]  Points  { get; init; } = [];
}

/// <summary>
/// Response body for <c>GET /api/events/counts</c>. Backward compatible with the previous shape;
/// <see cref="Levels"/> is the new per-level breakdown.
/// </summary>
internal sealed class EventCountsResponse
{
    public string From          { get; init; } = "";
    public string To            { get; init; } = "";
    public int    BucketSeconds { get; init; }
    public long   Total         { get; init; }
    public long   Sampled       { get; init; }
    public bool   Truncated     { get; init; }
    /// <summary>Bucket start timestamps (unix milliseconds).</summary>
    public long[] Buckets       { get; init; } = [];
    public CountSeriesDto[] Services { get; init; } = [];
    public CountSeriesDto[] Levels   { get; init; } = [];
}

/// <summary>Reflection-free serialization metadata for the counts endpoint.</summary>
[JsonSourceGenerationOptions(
    PropertyNamingPolicy   = JsonKnownNamingPolicy.CamelCase,
    DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull)]
[JsonSerializable(typeof(EventCountsResponse))]
internal partial class EventCountsJsonContext : JsonSerializerContext;

// ── Short-TTL response cache ────────────────────────────────────────────────────

/// <summary>Cache key for a counts query. The window is quantised to the bucket grid so repeated
/// "last N hours" requests (whose absolute <c>to</c>=now drifts every second) still hit while the
/// bucketed answer is effectively unchanged.</summary>
internal readonly record struct CountsCacheKey(long MinBucket, long MaxBucket, int BucketSeconds, string? Service);

/// <summary>
/// Tiny in-memory cache for <c>GET /api/events/counts</c> responses, keyed by
/// <see cref="CountsCacheKey"/> with a short TTL. Absorbs the burst of identical requests that a
/// range-tab toggle or periodic client refresh produces, so the header scan runs at most once per
/// window per TTL window. Thread-safe.
/// </summary>
public sealed class LogVolumeCountsCache
{
    private static readonly TimeSpan Ttl        = TimeSpan.FromSeconds(20);
    private const           int      MaxEntries = 256;

    private readonly record struct Entry(long ExpiresTicks, EventCountsResponse Payload);

    private readonly ConcurrentDictionary<CountsCacheKey, Entry> _entries = new();

    internal bool TryGet(in CountsCacheKey key, out EventCountsResponse payload)
    {
        if (_entries.TryGetValue(key, out var e) && e.ExpiresTicks > DateTime.UtcNow.Ticks)
        {
            payload = e.Payload;
            return true;
        }
        payload = null!;
        return false;
    }

    internal void Set(in CountsCacheKey key, EventCountsResponse payload)
    {
        if (_entries.Count >= MaxEntries) PruneExpired();
        _entries[key] = new Entry(DateTime.UtcNow.Add(Ttl).Ticks, payload);
    }

    private void PruneExpired()
    {
        long now = DateTime.UtcNow.Ticks;
        foreach (var kv in _entries)
            if (kv.Value.ExpiresTicks <= now)
                _entries.TryRemove(kv.Key, out _);

        // Still oversized ⇒ many live entries; drop everything rather than grow unbounded.
        if (_entries.Count >= MaxEntries) _entries.Clear();
    }
}
