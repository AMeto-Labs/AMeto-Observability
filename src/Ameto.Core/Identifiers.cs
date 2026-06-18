using System.ComponentModel;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

namespace Ameto.Core;

/// <summary>
/// Globally unique, time-sortable 64-bit event identifier (Snowflake layout):
/// <code>
///   [ 42 bits: ms since EventIdGenerator.Epoch ] [ 10 bits: NodeId ] [ 12 bits: sequence ]
/// </code>
/// Sorting by <see cref="RawValue"/> ascending equals sorting by ingest time ascending.
/// Per-node monotonic — pagination by <c>Id &gt;= cursor</c> / <c>Id &lt;= cursor</c> works
/// without consulting the timestamp.
///
/// Capacity per node: 4096 events / ms / node ≈ 4M events/sec/node before borrowing future ms.
/// Time range: ~139 years from <see cref="EventIdGenerator.Epoch"/> (UTC 2024-01-01).
/// </summary>
[StructLayout(LayoutKind.Sequential, Size = 8)]
public readonly struct EventId : IEquatable<EventId>, IComparable<EventId>
{
    private readonly ulong _value;

    /// <summary>
    /// Legacy two-arg ctor kept for callers that pass (nodeId, sequence). Used only by
    /// tests and code paths that want a deterministic id without time semantics.
    /// Encodes as if <c>timestampMs = 0</c>.
    /// </summary>
    public EventId(uint nodeId, uint sequence)
        : this(0UL, nodeId, sequence) { }

    public EventId(ulong timestampMsSinceEpoch, uint nodeId, uint sequence)
    {
        _value =
            ((timestampMsSinceEpoch & EventIdGenerator.TimeMask) << EventIdGenerator.TimeShift) |
            (((ulong)nodeId        & EventIdGenerator.NodeMask) << EventIdGenerator.NodeShift) |
            ( (ulong)sequence      & EventIdGenerator.SeqMask);
    }

    public EventId(ulong raw) => _value = raw;

    /// <summary>Milliseconds since <see cref="EventIdGenerator.Epoch"/>.</summary>
    public ulong TimestampMs => (_value >> EventIdGenerator.TimeShift) & EventIdGenerator.TimeMask;

    /// <summary>10-bit node id encoded in the middle of the value.</summary>
    public uint NodeId       => (uint)((_value >> EventIdGenerator.NodeShift) & EventIdGenerator.NodeMask);

    /// <summary>12-bit per-ms sequence.</summary>
    public uint Sequence     => (uint)(_value & EventIdGenerator.SeqMask);

    public ulong RawValue    => _value;

    public bool Equals(EventId other)           => _value == other._value;
    public int  CompareTo(EventId other)        => _value.CompareTo(other._value);
    public override bool Equals(object? obj)    => obj is EventId e && Equals(e);
    public override int  GetHashCode()          => _value.GetHashCode();
    public override string ToString()           => $"{TimestampMs}:{NodeId}:{Sequence}";

    public static bool operator ==(EventId a, EventId b) => a._value == b._value;
    public static bool operator !=(EventId a, EventId b) => a._value != b._value;
    public static bool operator  <(EventId a, EventId b) => a._value <  b._value;
    public static bool operator  >(EventId a, EventId b) => a._value >  b._value;
    public static bool operator <=(EventId a, EventId b) => a._value <= b._value;
    public static bool operator >=(EventId a, EventId b) => a._value >= b._value;

    public static readonly EventId Empty = new(0UL);
}

/// <summary>
/// Generates time-sortable, monotonic <see cref="EventId"/> values for a single node.
///
/// Hot path is one CAS on a packed long. The value is a 64-bit pack of
/// <c>(timestampMs &lt;&lt; 12) | sequence</c>; node id is constant and OR-ed in at
/// the very end so it never participates in CAS contention.
///
/// Monotonicity guarantees:
///   - Strictly increasing per node, even if the wall clock goes backwards.
///   - If &gt; 4096 events arrive in the same millisecond, the generator "borrows"
///     time from the future (advances <c>ms</c> by 1) instead of blocking.
/// </summary>
public sealed class EventIdGenerator
{
    /// <summary>Custom epoch: 2024-01-01T00:00:00Z (in DateTime.Ticks).</summary>
    public const long Epoch = 638_396_640_000_000_000L; // new DateTime(2024,1,1,0,0,0,DateTimeKind.Utc).Ticks

    public const int  SeqBits  = 12;
    public const int  NodeBits = 10;
    public const int  TimeBits = 42;

    public const int  NodeShift = SeqBits;             // 12
    public const int  TimeShift = SeqBits + NodeBits;  // 22

    public const ulong SeqMask  = (1UL << SeqBits)  - 1; // 0xFFF
    public const ulong NodeMask = (1UL << NodeBits) - 1; // 0x3FF
    public const ulong TimeMask = (1UL << TimeBits) - 1;

    private readonly ulong _nodeShifted; // (nodeId & NodeMask) << NodeShift, OR-ed at the end

    // Packed state: (ms << SeqBits) | seq. Single CAS target.
    private long _state;

    public EventIdGenerator(NodeId nodeId)
    {
        _nodeShifted = ((ulong)nodeId.Value & NodeMask) << NodeShift;
    }

    /// <summary>Generates the next id using <see cref="DateTime.UtcNow"/> for the time component.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ulong Next() => Next(DateTime.UtcNow.Ticks);

    /// <summary>
    /// Generates the next id from explicit ticks. Intended for the ingest path so the
    /// timestamp embedded in the id matches <see cref="DateTime.UtcNow"/> sampled once
    /// at slot acquisition (avoids two reads of the system clock).
    /// </summary>
    public ulong Next(long utcTicks)
    {
        long nowMs = (utcTicks - Epoch) / TimeSpan.TicksPerMillisecond;
        if (nowMs < 0) nowMs = 0; // before epoch — clamp

        SpinWait sw = default;
        while (true)
        {
            long prev    = Volatile.Read(ref _state);
            long prevMs  = prev >> SeqBits;
            long prevSeq = prev & (long)SeqMask;

            long ms, seq;
            if (nowMs > prevMs)
            {
                ms  = nowMs;
                seq = 0;
            }
            else if (prevSeq < (long)SeqMask)
            {
                ms  = prevMs;
                seq = prevSeq + 1;
            }
            else
            {
                // Saturated current ms — borrow next ms.
                ms  = prevMs + 1;
                seq = 0;
            }

            long next = (ms << SeqBits) | seq;
            if (Interlocked.CompareExchange(ref _state, next, prev) == prev)
                return ((ulong)ms << TimeShift) | _nodeShifted | (ulong)seq;

            sw.SpinOnce();
        }
    }
}

/// <summary>
/// Segment identifier — monotonically increasing per node.
/// </summary>
[StructLayout(LayoutKind.Sequential, Size = 8)]
public readonly struct SegmentId : IEquatable<SegmentId>
{
    public readonly ulong Value;
    public SegmentId(ulong value) => Value = value;
    public bool Equals(SegmentId other) => Value == other.Value;
    public override bool Equals(object? obj) => obj is SegmentId s && Equals(s);
    public override int  GetHashCode() => Value.GetHashCode();
    public override string ToString()  => Value.ToString();
    public static bool operator ==(SegmentId a, SegmentId b) => a.Value == b.Value;
    public static bool operator !=(SegmentId a, SegmentId b) => a.Value != b.Value;
}

/// <summary>
/// Node identifier in the cluster. Zero = standalone/single-node.
/// </summary>
[StructLayout(LayoutKind.Sequential, Size = 4)]
[TypeConverter(typeof(NodeIdTypeConverter))]
public readonly struct NodeId : IEquatable<NodeId>
{
    public readonly uint Value;
    public NodeId(uint value) => Value = value;
    public bool Equals(NodeId other) => Value == other.Value;
    public override bool Equals(object? obj) => obj is NodeId n && Equals(n);
    public override int  GetHashCode() => Value.GetHashCode();
    public override string ToString()  => Value.ToString();
    public static readonly NodeId Local = new(0);
}

/// <summary>Allows <see cref="Microsoft.Extensions.Configuration"/> to bind a YAML integer to <see cref="NodeId"/>.</summary>
internal sealed class NodeIdTypeConverter : TypeConverter
{
    public override bool CanConvertFrom(ITypeDescriptorContext? _, Type sourceType)
        => sourceType == typeof(string);

    public override object ConvertFrom(ITypeDescriptorContext? _, CultureInfo? __, object value)
        => new NodeId(uint.Parse((string)value, CultureInfo.InvariantCulture));
}
