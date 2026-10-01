using Microsoft.Extensions.Logging;

namespace Ameto.Storage;

/// <summary>
/// <see cref="StorageEngine"/>'s source-generated log lines. A class of their own because the
/// engine is not declared <c>partial</c>, and the generator needs a partial method on a partial type.
/// </summary>
internal static partial class StorageEngineLog
{
    /// <summary>
    /// The catalog scan failed AS A WHOLE (#94): the directory could not be listed, or the recovery of
    /// interrupted merges threw — a file that fails on its own is quarantined inside the scan. The
    /// segments on disk are then not in the catalog, nothing scans again before a restart, and every
    /// reader answers from the hot tier and what has been flushed since: the alert evaluator included,
    /// which reads a missing window as a quiet one. Said once per failed scan, as an Error naming that
    /// consequence — a store that reports itself degraded, and an evaluator that skips it, is the design
    /// filed on #94; this line is what an operator has until then.
    /// </summary>
    [LoggerMessage(Level = LogLevel.Error,
        Message = "The log segment catalog scan of {SegmentDirectory} failed: the segments on disk it had not registered "
                + "are not served until a restart, and log queries answer from the hot tier and what has been flushed "
                + "since. Log ALERT RULES keep being evaluated on that partial data: a missing window reads as a quiet "
                + "one, so a \"<\" rule can fire and a \">\" rule can resolve on events that exist but were not loaded. "
                + "Restart once the cause is fixed")]
    internal static partial void CatalogScanFailed(ILogger logger, Exception exception, string segmentDirectory);
}
