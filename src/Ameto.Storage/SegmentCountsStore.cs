using System.Buffers;
using System.Data;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging;
using Ameto.Core;

namespace Ameto.Storage;

/// <summary>
/// <c>counts.db</c>: every cold segment's event counts per (slot, service, level), on disk, at
/// three resolutions — 15 minutes, an hour and a day — so <c>GET /api/events/counts</c> answers a
/// segment wholly inside its window with one indexed read, at the coarsest resolution its grid
/// allows, after a restart as much as before one.
///
/// <para><b>Derived data, never the truth.</b> A row is the summary of one segment FILE, keyed by
/// the segment and pinned by a fingerprint of that file (size, event count, first and last
/// event). A row whose fingerprint does not match the catalog's entry is not used, so nothing
/// that deletes, merges, quarantines or replaces a segment has to tell the store: the row stops
/// matching, and <see cref="DeleteStale"/> clears it on the backfill's next pass. Lose the file
/// and it is rebuilt — from the sections new segments carry, and by one header scan of each older
/// one. A file that will not open is set aside and started afresh; if even that fails the store
/// is off and every segment is counted as before it existed.</para>
///
/// <para><b>One connection, one lock.</b> Readers take it for one prepared lookup per segment and
/// writers for one upsert, so neither waits long; WAL journalling and <c>synchronous=NORMAL</c>
/// make an upsert cheap, and a power cut can cost at most the last few rows, which the backfill
/// writes again. The connection is not pooled, so disposing the store closes the file.</para>
/// </summary>
internal sealed class SegmentCountsStore : IDisposable
{
    public const string FileName = "counts.db";

    /// <summary>What a lookup found for a segment.</summary>
    public enum Lookup
    {
        /// <summary>No row for this file: the caller counts it another way.</summary>
        Missing,
        /// <summary>The row's counts were added.</summary>
        Added,
        /// <summary>The segment is known to have no summary (its writer or scan gave up): scan it, do not try to build one.</summary>
        Unsummarisable,
    }

    /// <summary>A row's fingerprint, as the backfill reads it to decide what is missing.</summary>
    public readonly record struct Fingerprint(long FileBytes, long Events, long MinTicks, long MaxTicks)
    {
        public bool Matches(SegmentInfo info) =>
            FileBytes == info.CompressedBytes && Events == info.EventCount
            && MinTicks == info.MinTimestampTicks && MaxTicks == info.MaxTimestampTicks;
    }

    private readonly SqliteConnection _conn;
    private readonly SqliteCommand    _select15, _select1h, _select1d, _upsert, _delete;
    private readonly Lock             _lock = new();
    private readonly ILogger          _logger;
    private bool _disposed;
    private int  _errorsLogged;

    private SegmentCountsStore(string path, ILogger logger)
    {
        _logger = logger;
        var csb = new SqliteConnectionStringBuilder
        {
            DataSource = path,
            Mode       = SqliteOpenMode.ReadWriteCreate,
            Pooling    = false,
        };
        _conn = new SqliteConnection(csb.ToString());
        try
        {
            _conn.Open();
            Exec("PRAGMA journal_mode=WAL;");
            Exec("PRAGMA synchronous=NORMAL;");
            Exec("""
                CREATE TABLE IF NOT EXISTS segment_counts (
                    node       INTEGER NOT NULL,
                    id         INTEGER NOT NULL,
                    file_bytes INTEGER NOT NULL,
                    events     INTEGER NOT NULL,
                    min_ticks  INTEGER NOT NULL,
                    max_ticks  INTEGER NOT NULL,
                    q15        BLOB    NOT NULL,
                    h1         BLOB    NOT NULL,
                    d1         BLOB    NOT NULL,
                    PRIMARY KEY (node, id)
                ) WITHOUT ROWID;
                """);
            // A file that is not a database, or a damaged one, fails here rather than mid-request.
            Exec("SELECT count(*) FROM segment_counts WHERE node = -1;");

            _select15 = SelectCommand("q15");
            _select1h = SelectCommand("h1");
            _select1d = SelectCommand("d1");
            _upsert   = Command("""
                INSERT INTO segment_counts (node, id, file_bytes, events, min_ticks, max_ticks, q15, h1, d1)
                VALUES ($node, $id, $bytes, $events, $min, $max, $q15, $h1, $d1)
                ON CONFLICT (node, id) DO UPDATE SET
                    file_bytes = excluded.file_bytes, events = excluded.events,
                    min_ticks = excluded.min_ticks, max_ticks = excluded.max_ticks,
                    q15 = excluded.q15, h1 = excluded.h1, d1 = excluded.d1;
                """, "$node", "$id", "$bytes", "$events", "$min", "$max", "$q15", "$h1", "$d1");
            _delete   = Command("DELETE FROM segment_counts WHERE node = $node AND id = $id;", "$node", "$id");
        }
        catch
        {
            _conn.Dispose();
            throw;
        }
    }

    /// <summary>
    /// Opens (or creates) <c>counts.db</c> in <paramref name="dataDirectory"/>. A file that will not
    /// open as the store is set aside as <c>counts.db.corrupt</c> and the store starts empty; null
    /// when even that fails — the engine then counts every segment as it did before the store.
    /// </summary>
    public static SegmentCountsStore? Open(string dataDirectory, ILogger logger)
    {
        string path = Path.Combine(dataDirectory, FileName);
        try
        {
            Directory.CreateDirectory(dataDirectory);
            return new SegmentCountsStore(path, logger);
        }
        catch (Exception first)
        {
            try
            {
                SqliteConnection.ClearAllPools();
                foreach (var suffix in new[] { "", "-wal", "-shm" })
                {
                    string f = path + suffix;
                    if (File.Exists(f)) File.Move(f, f + ".corrupt", overwrite: true);
                }
                var store = new SegmentCountsStore(path, logger);
                Say(logger, first, "The counts summaries store {Path} did not open and was set aside as .corrupt; it is rebuilt from the segments.", path);
                return store;
            }
            catch (Exception second)
            {
                Say(logger, second, "The counts summaries store {Path} cannot be opened; wide counts windows decode their segments as before.", path);
                return null;
            }
        }
    }

    /// <summary>
    /// Adds the stored counts of <paramref name="info"/> at <paramref name="resolutionSeconds"/> to
    /// <paramref name="agg"/>, when a row for exactly this file exists and checks out. Never throws:
    /// a store that fails is a store that has no row.
    /// </summary>
    public Lookup TryAddTo(SegmentInfo info, int resolutionSeconds, LogVolumeAggregator agg, int bucketSeconds)
    {
        lock (_lock)
        {
            if (_disposed) return Lookup.Missing;
            try
            {
                var cmd = resolutionSeconds switch
                {
                    SegmentCountsSummary.DaySeconds  => _select1d,
                    SegmentCountsSummary.HourSeconds => _select1h,
                    _                                => _select15,
                };
                cmd.Parameters[0].Value = (long)info.NodeId.Value;
                cmd.Parameters[1].Value = unchecked((long)info.Id.Value);
                using var r = cmd.ExecuteReader(CommandBehavior.SingleRow);
                if (!r.Read()) return Lookup.Missing;
                if (!new Fingerprint(r.GetInt64(0), r.GetInt64(1), r.GetInt64(2), r.GetInt64(3)).Matches(info))
                    return Lookup.Missing;

                long length = r.GetBytes(4, 0, null, 0, 0);
                if (length == 0) return Lookup.Unsummarisable;
                if (length > SegmentCountsSummary.SectionHeaderBytes + SegmentCountsSummary.MaxPayloadBytes + 4)
                    return Lookup.Missing;
                byte[] buf = ArrayPool<byte>.Shared.Rent((int)length);
                try
                {
                    if (r.GetBytes(4, 0, buf, 0, (int)length) != length) return Lookup.Missing;
                    return SegmentCountsSummary.TryAddFrom(buf.AsSpan(0, (int)length), info, agg, bucketSeconds)
                        ? Lookup.Added : Lookup.Missing;
                }
                finally { ArrayPool<byte>.Shared.Return(buf); }
            }
            catch (Exception ex)
            {
                SayOnce(ex, "Reading the counts summaries store failed; the segment is counted without it.");
                return Lookup.Missing;
            }
        }
    }

    /// <summary>
    /// Stores <paramref name="summary"/> (built at 15 minutes) for the file <paramref name="info"/>
    /// describes, with its hour and day forms; null stores that the segment has none, so the
    /// backfill does not scan it again. False when the write failed (it is retried by a later pass).
    /// </summary>
    public bool TryUpsert(SegmentInfo info, SegmentCountsSummary? summary)
    {
        byte[] q15 = [], h1 = [], d1 = [];
        if (summary is not null)
        {
            if (summary.ResolutionSeconds != SegmentCountsSummary.SlotSeconds)
                throw new ArgumentException("The store is fed 15-minute summaries.", nameof(summary));
            q15 = summary.ToStoreBytes();
            h1  = summary.Coarsen(SegmentCountsSummary.HourSeconds).ToStoreBytes();
            d1  = summary.Coarsen(SegmentCountsSummary.DaySeconds).ToStoreBytes();
        }
        lock (_lock)
        {
            if (_disposed) return false;
            try
            {
                var p = _upsert.Parameters;
                p[0].Value = (long)info.NodeId.Value;
                p[1].Value = unchecked((long)info.Id.Value);
                p[2].Value = info.CompressedBytes;
                p[3].Value = (long)info.EventCount;
                p[4].Value = info.MinTimestampTicks;
                p[5].Value = info.MaxTimestampTicks;
                p[6].Value = q15;
                p[7].Value = h1;
                p[8].Value = d1;
                _upsert.ExecuteNonQuery();
                return true;
            }
            catch (Exception ex)
            {
                SayOnce(ex, "Writing the counts summaries store failed; the segment's summary is written again by a later pass.");
                return false;
            }
        }
    }

    /// <summary>Every row's key and fingerprint: what the backfill compares the catalog against. Empty on failure.</summary>
    public Dictionary<SegmentKey, Fingerprint> ReadFingerprints()
    {
        var rows = new Dictionary<SegmentKey, Fingerprint>();
        lock (_lock)
        {
            if (_disposed) return rows;
            try
            {
                using var cmd = _conn.CreateCommand();
                cmd.CommandText = "SELECT node, id, file_bytes, events, min_ticks, max_ticks FROM segment_counts;";
                using var r = cmd.ExecuteReader();
                while (r.Read())
                    rows[new SegmentKey(new NodeId((uint)r.GetInt64(0)), new SegmentId(unchecked((ulong)r.GetInt64(1))))] =
                        new Fingerprint(r.GetInt64(2), r.GetInt64(3), r.GetInt64(4), r.GetInt64(5));
            }
            catch (Exception ex) { SayOnce(ex, "Reading the counts summaries store failed."); }
        }
        return rows;
    }

    /// <summary>
    /// Deletes every row whose segment the catalog no longer serves as that file — merged away,
    /// deleted by retention, set aside, or replaced. Returns how many went.
    /// </summary>
    /// <param name="current">The catalog's entry under a key, or null when it has none.</param>
    public int DeleteStale(Func<SegmentKey, SegmentInfo?> current)
    {
        var stale = new List<SegmentKey>();
        foreach (var (key, fp) in ReadFingerprints())
            if (current(key) is not { } info || !fp.Matches(info)) stale.Add(key);
        if (stale.Count == 0) return 0;

        lock (_lock)
        {
            if (_disposed) return 0;
            try
            {
                using var tx = _conn.BeginTransaction();
                _delete.Transaction = tx;
                foreach (var key in stale)
                {
                    _delete.Parameters[0].Value = (long)key.Node.Value;
                    _delete.Parameters[1].Value = unchecked((long)key.Id.Value);
                    _delete.ExecuteNonQuery();
                }
                tx.Commit();
                return stale.Count;
            }
            catch (Exception ex)
            {
                SayOnce(ex, "Clearing stale rows from the counts summaries store failed; a later pass tries again.");
                return 0;
            }
            finally { _delete.Transaction = null; }
        }
    }

    public void Dispose()
    {
        lock (_lock)
        {
            if (_disposed) return;
            _disposed = true;
            _select15.Dispose(); _select1h.Dispose(); _select1d.Dispose(); _upsert.Dispose(); _delete.Dispose();
            _conn.Dispose();
        }
    }

    private SqliteCommand SelectCommand(string column) =>
        Command($"SELECT file_bytes, events, min_ticks, max_ticks, {column} FROM segment_counts WHERE node = $node AND id = $id;",
                "$node", "$id");

    private SqliteCommand Command(string sql, params string[] parameters)
    {
        var cmd = _conn.CreateCommand();
        cmd.CommandText = sql;
        foreach (var name in parameters) cmd.Parameters.Add(new SqliteParameter(name, null));
        cmd.Prepare();
        return cmd;
    }

    private void Exec(string sql)
    {
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    /// <summary>The first few failures are said; a store failing on every request must not flood the log.</summary>
    private void SayOnce(Exception ex, string message)
    {
        if (Interlocked.Increment(ref _errorsLogged) > 5) return;
        Say(_logger, ex, message + " {Path}", FileName);
    }

    private static void Say(ILogger logger, Exception ex, string message, string path)
    {
        try { logger.LogWarning(ex, message, path); }
        catch (Exception) { /* the line is lost; the store is not */ }
    }
}

/// <summary>What the last counts backfill pass found: catalog segments, and how many have their row.</summary>
public sealed record CountsSummaryProgress(int Segments, int Summarised, DateTimeOffset? AsOf)
{
    /// <summary>No pass has finished yet in this process.</summary>
    public static readonly CountsSummaryProgress NotYet = new(0, 0, null);
}

/// <summary>One backfill pass, by what it did with each segment.</summary>
internal readonly record struct CountsBackfillResult(
    int Segments, int AlreadyStored, int FromFileSections, int Scanned, int Unsummarisable, int Failed, int Swept)
{
    public int Written => FromFileSections + Scanned + Unsummarisable;
}
