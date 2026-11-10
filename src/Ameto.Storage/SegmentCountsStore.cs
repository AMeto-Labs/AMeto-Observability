using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging;
using SQLitePCL;
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
/// one. A file that is not a database, or a damaged one, is set aside and started afresh; one that
/// cannot be opened for another reason (locked, unreadable, the disk) is left alone, and this
/// process counts without the store.</para>
///
/// <para><b>Layout.</b> A rowid table: the key's own index has small entries, so a lookup walks
/// no row it does not want (a <c>WITHOUT ROWID</c> table compares whole rows of hundreds of
/// kilobytes on the way down — measured at ~0.9 ms a lookup against ~8 µs). The blobs are stored
/// day, hour, quarter, so reading the small ones walks no overflow chain of the big one.
/// <c>PRAGMA user_version</c> names the layout; another one is dropped and rebuilt.</para>
///
/// <para><b>One connection, one lock.</b> Readers take it for one lookup per segment and writers
/// for one upsert. A lookup is a persistent SQLite statement driven through SQLitePCLRaw: the blob
/// is read in place, in SQLite's own memory, and added from there, so it allocates nothing. WAL
/// journalling and <c>synchronous=NORMAL</c> make an upsert cheap, and a power cut can cost at most
/// the last few rows, which the backfill writes again. The connection is not pooled, so disposing
/// the store closes the file.</para>
/// </summary>
internal sealed class SegmentCountsStore : IDisposable
{
    public const string FileName = "counts.db";

    /// <summary>The table's layout; a file with another is dropped and rebuilt.</summary>
    internal const int SchemaVersion = 1;

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
    private readonly sqlite3_stmt     _lookup15 = null!, _lookup1h = null!, _lookup1d = null!;
    private readonly SqliteCommand    _upsert = null!, _delete = null!;
    private readonly Lock             _lock = new();
    private readonly ILogger          _logger;
    private bool _disposed;
    private int  _errorsLogged;
    private volatile bool _writable = true;

    /// <summary>Test seam: every write fails, as on a full disk.</summary>
    internal bool FailWritesForTest;

    private SegmentCountsStore(string path, ILogger logger)
    {
        _logger = logger;
        var csb = new SqliteConnectionStringBuilder
        {
            DataSource     = path,
            Mode           = SqliteOpenMode.ReadWriteCreate,
            Pooling        = false,
            // A lock held by another process is not worth the engine's constructor waiting the
            // default 30 s for; the store is optional.
            DefaultTimeout = 2,
        };
        _conn = new SqliteConnection(csb.ToString());
        try
        {
            _conn.Open();
            Exec("PRAGMA journal_mode=WAL;");
            Exec("PRAGMA synchronous=NORMAL;");
            if (Scalar("PRAGMA user_version;") != SchemaVersion)
            {
                Exec("DROP TABLE IF EXISTS segment_counts;");
                Exec($"PRAGMA user_version = {SchemaVersion};");
            }
            Exec("""
                CREATE TABLE IF NOT EXISTS segment_counts (
                    node       INTEGER NOT NULL,
                    id         INTEGER NOT NULL,
                    file_bytes INTEGER NOT NULL,
                    events     INTEGER NOT NULL,
                    min_ticks  INTEGER NOT NULL,
                    max_ticks  INTEGER NOT NULL,
                    d1         BLOB    NOT NULL,
                    h1         BLOB    NOT NULL,
                    q15        BLOB    NOT NULL,
                    PRIMARY KEY (node, id)
                );
                """);
            // A file that is not a database, or a damaged header or schema, fails here rather
            // than mid-request. A damaged page elsewhere fails the lookup or write that meets it.
            Exec("SELECT count(*) FROM segment_counts WHERE node = -1;");

            var db = _conn.Handle!;
            _lookup15 = Persistent(db, "q15");
            _lookup1h = Persistent(db, "h1");
            _lookup1d = Persistent(db, "d1");
            _upsert   = Command("""
                INSERT INTO segment_counts (node, id, file_bytes, events, min_ticks, max_ticks, d1, h1, q15)
                VALUES ($node, $id, $bytes, $events, $min, $max, $d1, $h1, $q15)
                ON CONFLICT (node, id) DO UPDATE SET
                    file_bytes = excluded.file_bytes, events = excluded.events,
                    min_ticks = excluded.min_ticks, max_ticks = excluded.max_ticks,
                    d1 = excluded.d1, h1 = excluded.h1, q15 = excluded.q15;
                """, "$node", "$id", "$bytes", "$events", "$min", "$max", "$d1", "$h1", "$q15");
            _delete   = Command("DELETE FROM segment_counts WHERE node = $node AND id = $id;", "$node", "$id");
        }
        catch
        {
            Finalise(_lookup15); Finalise(_lookup1h); Finalise(_lookup1d);
            _upsert?.Dispose(); _delete?.Dispose();
            _conn.Dispose();
            throw;
        }
    }

    /// <summary>
    /// Opens (or creates) <c>counts.db</c> in <paramref name="dataDirectory"/>. A file that is not a
    /// database, or a damaged one, is set aside as <c>counts.db.corrupt</c> and the store starts
    /// empty. Any other failure — a file another process holds, one that cannot be read, a full
    /// disk — leaves the file as it is and returns null: this process then counts every segment as
    /// it did before the store, and the next start tries again.
    /// </summary>
    public static SegmentCountsStore? Open(string dataDirectory, ILogger logger)
    {
        string path = Path.Combine(dataDirectory, FileName);
        try
        {
            Directory.CreateDirectory(dataDirectory);
            return new SegmentCountsStore(path, logger);
        }
        catch (SqliteException first) when (first.SqliteErrorCode is raw.SQLITE_NOTADB or raw.SQLITE_CORRUPT)
        {
            try
            {
                foreach (var suffix in new[] { "", "-wal", "-shm" })
                {
                    string f = path + suffix;
                    if (File.Exists(f)) File.Move(f, f + ".corrupt", overwrite: true);
                }
                var store = new SegmentCountsStore(path, logger);
                Say(logger, first, "The counts summaries store {Path} was damaged and is set aside as .corrupt; it is rebuilt from the segments.", path);
                return store;
            }
            catch (Exception second)
            {
                Say(logger, second, "The counts summaries store {Path} cannot be opened; wide counts windows decode their segments as before.", path);
                return null;
            }
        }
        catch (Exception ex)
        {
            Say(logger, ex, "The counts summaries store {Path} cannot be opened now; wide counts windows decode their segments as before, and the next start tries again.", path);
            return null;
        }
    }

    /// <summary>
    /// False once a write has failed, until one succeeds again (the backfill tries one each pass).
    /// A caller that would build a summary only to store it builds none while this is false.
    /// </summary>
    public bool Writable => _writable && !Volatile.Read(ref _disposed);

    /// <summary>
    /// Adds the stored counts of <paramref name="info"/> at <paramref name="resolutionSeconds"/> to
    /// <paramref name="agg"/>, when a row for exactly this file exists and checks out. Allocates
    /// nothing; never throws: a store that fails is a store that has no row.
    /// </summary>
    public Lookup TryAddTo(SegmentInfo info, int resolutionSeconds, LogVolumeAggregator agg, int bucketSeconds)
    {
        lock (_lock)
        {
            if (_disposed) return Lookup.Missing;
            var stmt = resolutionSeconds switch
            {
                SegmentCountsSummary.DaySeconds  => _lookup1d,
                SegmentCountsSummary.HourSeconds => _lookup1h,
                _                                => _lookup15,
            };
            try
            {
                raw.sqlite3_bind_int64(stmt, 1, info.NodeId.Value);
                raw.sqlite3_bind_int64(stmt, 2, unchecked((long)info.Id.Value));
                int rc = raw.sqlite3_step(stmt);
                if (rc == raw.SQLITE_DONE) return Lookup.Missing;
                if (rc != raw.SQLITE_ROW) { SayOnce(rc, "Reading the counts summaries store failed; the segment is counted without it."); return Lookup.Missing; }

                if (!new Fingerprint(raw.sqlite3_column_int64(stmt, 0), raw.sqlite3_column_int64(stmt, 1),
                                     raw.sqlite3_column_int64(stmt, 2), raw.sqlite3_column_int64(stmt, 3)).Matches(info))
                    return Lookup.Missing;

                // In place: the span is SQLite's own buffer for this row, valid until the reset below.
                ReadOnlySpan<byte> blob = raw.sqlite3_column_blob(stmt, 4);
                if (blob.IsEmpty) return Lookup.Unsummarisable;
                return SegmentCountsSummary.TryAddFrom(blob, info, agg, bucketSeconds) ? Lookup.Added : Lookup.Missing;
            }
            catch (Exception ex)
            {
                SayOnce(ex, "Reading the counts summaries store failed; the segment is counted without it.");
                return Lookup.Missing;
            }
            finally { raw.sqlite3_reset(stmt); }
        }
    }

    /// <summary>
    /// Stores <paramref name="summary"/> (built at 15 minutes) for the file <paramref name="info"/>
    /// describes, with its hour and day forms; null stores that the segment has none, so the
    /// backfill does not scan it again. False when the write failed (and <see cref="Writable"/> goes
    /// false until one succeeds).
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
                if (FailWritesForTest) throw new SqliteException("disk I/O error (test)", raw.SQLITE_IOERR);
                var p = _upsert.Parameters;
                p[0].Value = (long)info.NodeId.Value;
                p[1].Value = unchecked((long)info.Id.Value);
                p[2].Value = info.CompressedBytes;
                p[3].Value = (long)info.EventCount;
                p[4].Value = info.MinTimestampTicks;
                p[5].Value = info.MaxTimestampTicks;
                p[6].Value = d1;
                p[7].Value = h1;
                p[8].Value = q15;
                _upsert.ExecuteNonQuery();
                _writable = true;
                return true;
            }
            catch (Exception ex)
            {
                _writable = false;
                SayOnce(ex, "Writing the counts summaries store failed; no summary is built until a write succeeds again, which the backfill tries once a pass.");
                return false;
            }
        }
    }

    /// <summary>
    /// Every row's key and fingerprint: what the backfill compares the catalog against. False, with
    /// nothing, when the store cannot be read — the backfill then has nothing to go on and stops.
    /// </summary>
    public bool TryReadFingerprints(out Dictionary<SegmentKey, Fingerprint> rows)
    {
        rows = new Dictionary<SegmentKey, Fingerprint>();
        lock (_lock)
        {
            if (_disposed) return false;
            try
            {
                using var cmd = _conn.CreateCommand();
                cmd.CommandText = "SELECT node, id, file_bytes, events, min_ticks, max_ticks FROM segment_counts;";
                using var r = cmd.ExecuteReader();
                while (r.Read())
                    rows[new SegmentKey(new NodeId((uint)r.GetInt64(0)), new SegmentId(unchecked((ulong)r.GetInt64(1))))] =
                        new Fingerprint(r.GetInt64(2), r.GetInt64(3), r.GetInt64(4), r.GetInt64(5));
                return true;
            }
            catch (Exception ex)
            {
                SayOnce(ex, "Reading the counts summaries store failed.");
                rows.Clear();
                return false;
            }
        }
    }

    /// <summary>Every row's key and fingerprint, or none when the store cannot be read.</summary>
    public Dictionary<SegmentKey, Fingerprint> ReadFingerprints() =>
        TryReadFingerprints(out var rows) ? rows : new Dictionary<SegmentKey, Fingerprint>();

    /// <summary>
    /// Deletes every row in <paramref name="rows"/> whose segment the catalog no longer serves as
    /// that file — merged away, deleted by retention, set aside, or replaced. Returns how many went.
    /// The caller passes a COMPLETE catalog: against one still loading, every row it has not reached
    /// yet would look stale.
    /// </summary>
    /// <param name="rows">The fingerprints the caller read at the start of its pass.</param>
    /// <param name="current">The catalog's entry under a key, or null when it has none.</param>
    public int DeleteStale(IReadOnlyDictionary<SegmentKey, Fingerprint> rows, Func<SegmentKey, SegmentInfo?> current)
    {
        var stale = new List<SegmentKey>();
        foreach (var (key, fp) in rows)
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
            Volatile.Write(ref _disposed, true);
            // The statements before the connection, or closing it leaves the file open until they go.
            Finalise(_lookup15); Finalise(_lookup1h); Finalise(_lookup1d);
            _upsert.Dispose(); _delete.Dispose();
            _conn.Dispose();
        }
    }

    private static sqlite3_stmt Persistent(sqlite3 db, string column)
    {
        int rc = raw.sqlite3_prepare_v3(
            db, $"SELECT file_bytes, events, min_ticks, max_ticks, {column} FROM segment_counts WHERE node = ?1 AND id = ?2;",
            raw.SQLITE_PREPARE_PERSISTENT, out sqlite3_stmt stmt);
        if (rc != raw.SQLITE_OK)
        {
            Finalise(stmt);
            throw new SqliteException(raw.sqlite3_errmsg(db).utf8_to_string(), rc);
        }
        return stmt;
    }

    private static void Finalise(sqlite3_stmt? stmt)
    {
        if (stmt is not null && !stmt.IsInvalid) stmt.Dispose();
    }

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

    private long Scalar(string sql)
    {
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = sql;
        return Convert.ToInt64(cmd.ExecuteScalar());
    }

    /// <summary>The first few failures are said; a store failing on every request must not flood the log.</summary>
    private void SayOnce(Exception ex, string message)
    {
        if (Interlocked.Increment(ref _errorsLogged) > 5) return;
        Say(_logger, ex, message + " {Path}", FileName);
    }

    private void SayOnce(int sqliteCode, string message) =>
        SayOnce(new SqliteException(raw.sqlite3_errstr(sqliteCode).utf8_to_string(), sqliteCode), message);

    private static void Say(ILogger logger, Exception ex, string message, string path)
    {
        try { logger.LogWarning(ex, message, path); }
        catch (Exception) { /* the line is lost; the store is not */ }
    }
}

/// <summary>What the last counts backfill pass found: catalog segments, and how many have their row.</summary>
public sealed record CountsSummaryProgress(int Segments, int Summarised, DateTimeOffset? AsOf)
{
    /// <summary>No pass has finished over a whole catalog yet in this process.</summary>
    public static readonly CountsSummaryProgress NotYet = new(0, 0, null);
}

/// <summary>One backfill pass, by what it did with each segment.</summary>
internal readonly record struct CountsBackfillResult(
    int Segments, int AlreadyStored, int FromFileSections, int Scanned, int Unsummarisable, int Failed, int Swept,
    bool StoppedOnStoreFailure = false)
{
    public int Written => FromFileSections + Scanned + Unsummarisable;
}
