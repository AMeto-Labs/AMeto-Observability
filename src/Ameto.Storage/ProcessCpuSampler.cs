using System.Diagnostics;

namespace Ameto.Storage;

/// <summary>
/// Rolling CPU-usage sampler for this process.
///
/// <para>Process CPU time is a monotonic total, so a <em>percentage</em> only exists between
/// two reads of it. Keeping that previous read in one shared place is the whole point of this
/// type: the 30-second attribution line and <c>/api/diagnostics</c> then report the same
/// number over the same window, instead of each holding its own clock — and a dashboard poll
/// re-sampling on its own would otherwise shorten the interval the log line is measuring and
/// make both readings jitter.</para>
///
/// <para>Reads <see cref="Environment.CpuUsage"/> rather than
/// <c>Process.GetCurrentProcess().TotalProcessorTime</c>: the former is a struct read of the
/// same OS counters, where the latter allocates a <see cref="Process"/> whose finalizable
/// handle then has to be collected — needless on a path that runs every 30 seconds forever.</para>
/// </summary>
public sealed class ProcessCpuSampler
{
    private readonly int  _cores = Environment.ProcessorCount;
    private readonly Lock _gate  = new();

    private TimeSpan _prevCpu       = Environment.CpuUsage.TotalTime;
    private long     _prevTimestamp = Stopwatch.GetTimestamp();
    private double   _lastPercent   = -1;

    /// <summary>Logical processors the percentage is normalised against.</summary>
    public int Cores => _cores;

    /// <summary>
    /// CPU used since the previous <see cref="Sample"/>, as a percentage of ALL cores — the
    /// figure Task Manager shows, not the per-core one where 8 saturated cores read 800 %.
    /// Returns the value from the last completed interval; -1 until the first one closes.
    /// </summary>
    public double LastPercent { get { lock (_gate) return _lastPercent; } }

    /// <summary>Total CPU time consumed by this process since start. Monotonic.</summary>
    public static TimeSpan TotalProcessorTime => Environment.CpuUsage.TotalTime;

    /// <summary>
    /// Closes the current interval and opens the next. Call from ONE place on a steady
    /// cadence — readers use <see cref="LastPercent"/>.
    /// </summary>
    public double Sample()
    {
        lock (_gate)
        {
            var  cpu       = Environment.CpuUsage.TotalTime;
            long timestamp = Stopwatch.GetTimestamp();

            double elapsedMs = (timestamp - _prevTimestamp) * 1000.0 / Stopwatch.Frequency;
            double cpuMs     = (cpu - _prevCpu).TotalMilliseconds;

            _prevCpu       = cpu;
            _prevTimestamp = timestamp;

            // A zero-length interval (two samples inside a timer tick) would divide by zero;
            // report the previous figure rather than an infinity.
            if (elapsedMs <= 0) return _lastPercent;

            _lastPercent = Math.Clamp(cpuMs / elapsedMs / _cores * 100.0, 0, 100);
            return _lastPercent;
        }
    }
}
