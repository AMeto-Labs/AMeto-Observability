namespace Ameto.Indexing;

/// <summary>
/// What an inverted-index add created. Ordered so that a "newer" outcome compares greater:
/// a new property is also a new value in it.
/// </summary>
public enum IndexAddOutcome : byte
{
    /// <summary>The (property, value) bucket already existed.</summary>
    Existing = 0,
    /// <summary>The property existed; this value is its first sighting.</summary>
    NewValue = 1,
    /// <summary>First sighting of the property (and therefore of the value).</summary>
    NewProperty = 2,
}
