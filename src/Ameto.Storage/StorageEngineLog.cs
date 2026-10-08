using Microsoft.Extensions.Logging;

namespace Ameto.Storage;

/// <summary>
/// <see cref="StorageEngine"/>'s source-generated log lines. A class of their own because the
/// engine is not declared <c>partial</c>, and the generator needs a partial method on a partial type.
/// </summary>
internal static partial class StorageEngineLog
{
    /// <summary>
    /// A segment the catalog scan could not open, through its retries, for a reason that is not its
    /// bytes (#119 review): left under its name — not set aside as <c>.seg.corrupt</c> — out of this
    /// run's catalog, and the store <see cref="Ameto.Core.QueryAvailability.Degraded"/> until the
    /// restart that reads it. One Error per such segment, naming it.
    /// </summary>
    [LoggerMessage(Level = LogLevel.Error,
        Message = "Segment {File} could not be read by the catalog scan, through its retries, for a reason that is not "
                + "its contents: it is left where it is — not set aside as .corrupt — and is read at the next start. Until "
                + "then its events are not served: the log store reports itself Degraded, and log ALERT RULES are not "
                + "evaluated on the partial data (unless Ameto:Alerts:EvaluateOnDegradedStore is set). Restart once "
                + "whatever holds the file has let go of it; if a restart names the same file again, the failure is not "
                + "transient — check the file's permissions and its volume, or move it aside")]
    internal static partial void CatalogSegmentUnreachable(ILogger logger, Exception exception, string file);

    /// <summary>
    /// A segment in a newer format than this build reads (#119 review F2): kept under its name, out of
    /// the catalog — not set aside, not Degraded. One Error per such segment at every start, naming it
    /// and its version, because its events are not served until a build that reads it runs.
    /// </summary>
    [LoggerMessage(Level = LogLevel.Error,
        Message = "Segment {File} was written in segment format v{Version}, newer than this build reads: it is left on "
                + "disk untouched, under its name — setting it aside would hide events a build that knows the format can "
                + "read — and its events are not served until this node runs such a build")]
    internal static partial void CatalogSegmentNewerFormat(ILogger logger, Exception exception, string file, int version);

    /// <summary>
    /// The catalog scan failed AS A WHOLE (#94): the directory could not be listed, or the recovery of
    /// interrupted merges threw — a file that fails on its own is handled inside the scan (set aside
    /// for its bytes, otherwise left unread with an Error of its own, see above). The
    /// segments on disk are then not in the catalog, nothing scans again before a restart, and every
    /// reader answers from the hot tier and what has been flushed since. The boot scan's task faults,
    /// which is what makes the store report itself <see cref="Ameto.Core.QueryAvailability.Degraded"/>
    /// and the alert evaluator skip it rather than read a missing window as a quiet one. Said once per
    /// failed scan, as an Error naming both.
    /// </summary>
    [LoggerMessage(Level = LogLevel.Error,
        Message = "The log segment catalog scan of {SegmentDirectory} failed: the segments on disk it had not registered "
                + "are not served until a restart, and log queries answer from the hot tier and what has been flushed "
                + "since. The log store reports itself Degraded until then, and log ALERT RULES are not evaluated on "
                + "that partial data (unless Ameto:Alerts:EvaluateOnDegradedStore is set): a missing window would read "
                + "as a quiet one. Restart once the cause is fixed")]
    internal static partial void CatalogScanFailed(ILogger logger, Exception exception, string segmentDirectory);
}
