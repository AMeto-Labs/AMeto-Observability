using Ameto.Core;
using Ameto.Storage;

namespace Ameto.Storage.Tests;

/// <summary>
/// WAL append → recovery round-trip, focused on the exception path: exception bytes are
/// serialised through a thread-reused scratch buffer, so consecutive appends with
/// different exceptions must not bleed into each other, and exception-free entries in
/// between must stay exception-free.
/// </summary>
public sealed class WriteAheadLogTests
{
    [Fact]
    public void Append_WithExceptions_RoundTripsThroughRecovery()
    {
        string path = Path.Combine(Path.GetTempPath(), $"wal-{Guid.NewGuid():N}.wal");
        var payloadA = new byte[] { 1, 2, 3 };
        var payloadB = new byte[] { 9, 8 };

        var excA = new ExceptionInfo
        {
            Type       = "System.InvalidOperationException",
            Message    = "first failure",
            StackTrace = "at A.B()",
            Inner      = new ExceptionInfo { Type = "System.IO.IOException", Message = "disk" },
        };
        var excB = new ExceptionInfo { Type = "System.TimeoutException", Message = "second — different and longer than the first one" };

        try
        {
            using (var wal = WriteAheadLog.Open(path, new NodeId(0), new SegmentId(1UL), initialCapacity: 1024 * 1024))
            {
                wal.Append(100, Ameto.Core.LogLevel.Error,       0, "tmpl-a", payloadA, excA);
                wal.Append(200, Ameto.Core.LogLevel.Information, 1, "tmpl-b", payloadB, exception: null);
                wal.Append(300, Ameto.Core.LogLevel.Fatal,       2, "tmpl-c", ReadOnlySpan<byte>.Empty, excB);
            }

            var (segId, entries) = WriteAheadLog.ReadForRecovery(path);

            Assert.Equal(1UL, segId);
            Assert.Equal(3, entries.Count);

            Assert.Equal(100, entries[0].TimestampTicks);
            Assert.Equal(payloadA, entries[0].Payload);
            Assert.NotNull(entries[0].Exception);
            Assert.Equal(excA.Type,          entries[0].Exception!.Type);
            Assert.Equal(excA.Message,       entries[0].Exception!.Message);
            Assert.Equal(excA.StackTrace,    entries[0].Exception!.StackTrace);
            Assert.Equal(excA.Inner!.Type,   entries[0].Exception!.Inner?.Type);

            Assert.Equal(payloadB, entries[1].Payload);
            Assert.Null(entries[1].Exception);          // scratch reuse must not leak excA here

            Assert.Empty(entries[2].Payload);
            Assert.Equal(excB.Type,    entries[2].Exception?.Type);
            Assert.Equal(excB.Message, entries[2].Exception?.Message);
        }
        finally
        {
            File.Delete(path);
            File.Delete(path + ".pool");
        }
    }

    // ── v4 corruption handling ────────────────────────────────────────────────
    //
    // Offsets used below: file header = 32 bytes, entry header = 24 bytes, so with
    // checksummed v4 entries of payload sizes 3 / 2 / 4 the entries sit at file offsets
    // 32, 59 and 85, and the header's WriteOffset field (byte 24) reads 32+81 = 113.

    private static string NewWalPath() => Path.Combine(Path.GetTempPath(), $"wal-{Guid.NewGuid():N}.wal");

    private static void WriteThreeEntries(string path)
    {
        using var wal = WriteAheadLog.Open(path, new NodeId(0), new SegmentId(1UL), initialCapacity: 1024 * 1024);
        wal.Append(100, Ameto.Core.LogLevel.Error,       0, "tmpl-a", new byte[] { 1, 2, 3 });
        wal.Append(200, Ameto.Core.LogLevel.Information, 1, "tmpl-b", new byte[] { 9, 8 });
        wal.Append(300, Ameto.Core.LogLevel.Warning,     2, "tmpl-c", new byte[] { 7, 6, 5, 4 });
    }

    [Fact]
    public void Recovery_StopsAtEntryWhoseChecksumFails()
    {
        string path = NewWalPath();
        try
        {
            WriteThreeEntries(path);

            // Flip one payload byte of entry 2 — a torn page in the middle of the log.
            using (var fs = new FileStream(path, FileMode.Open, FileAccess.ReadWrite))
            {
                fs.Seek(59 + 24 + 1, SeekOrigin.Begin);
                int b = fs.ReadByte();
                fs.Seek(-1, SeekOrigin.Current);
                fs.WriteByte((byte)(b ^ 0xFF));
            }

            var (_, entries) = WriteAheadLog.ReadForRecovery(path);
            Assert.Single(entries);                 // entry 1 survives, replay stops at the tear
            Assert.Equal(100, entries[0].TimestampTicks);
        }
        finally { File.Delete(path); File.Delete(path + ".pool"); }
    }

    [Fact]
    public void Recovery_StopsAtZeroedTail()
    {
        string path = NewWalPath();
        try
        {
            WriteThreeEntries(path);

            // Zero everything after entry 1 while WriteOffset still covers it: the shape a
            // crash leaves when the header page reached disk but the payload pages did not
            // (the file is created sparse). Pre-checksum, a zero region parsed as an endless
            // run of "valid" zero-length entries at year 0001.
            using (var fs = new FileStream(path, FileMode.Open, FileAccess.ReadWrite))
            {
                fs.Seek(59, SeekOrigin.Begin);
                fs.Write(new byte[113 - 59]);
            }

            var (_, entries) = WriteAheadLog.ReadForRecovery(path);
            Assert.Single(entries);
            Assert.Equal(100, entries[0].TimestampTicks);
        }
        finally { File.Delete(path); File.Delete(path + ".pool"); }
    }

    [Fact]
    public void Recovery_ClampsGarbageWriteOffset()
    {
        string path = NewWalPath();
        try
        {
            using (var wal = WriteAheadLog.Open(path, new NodeId(0), new SegmentId(1UL), initialCapacity: 1024 * 1024))
                wal.Append(100, Ameto.Core.LogLevel.Error, 0, "tmpl-a", new byte[] { 1, 2, 3 });

            using (var fs = new FileStream(path, FileMode.Open, FileAccess.ReadWrite))
            {
                // Sanity-check the field position before poisoning it.
                fs.Seek(24, SeekOrigin.Begin);
                Span<byte> cur = stackalloc byte[8];
                fs.ReadExactly(cur);
                Assert.Equal(32L + 24 + 3, BitConverter.ToInt64(cur));

                fs.Seek(24, SeekOrigin.Begin);
                fs.Write(BitConverter.GetBytes(long.MaxValue - 100));
            }

            // Unclamped, the entry walk runs off the mapped view: an uncatchable
            // AccessViolation in the engine constructor, i.e. a crash-looping node.
            var (_, entries) = WriteAheadLog.ReadForRecovery(path);
            Assert.Single(entries);
            Assert.Equal(100, entries[0].TimestampTicks);
        }
        finally { File.Delete(path); File.Delete(path + ".pool"); }
    }

    [Fact]
    public void Reopen_WithGarbageWriteOffset_ResetsAndAppends()
    {
        string path = NewWalPath();
        try
        {
            using (var wal = WriteAheadLog.Open(path, new NodeId(0), new SegmentId(1UL), initialCapacity: 1024 * 1024))
                wal.Append(100, Ameto.Core.LogLevel.Error, 0, "tmpl-a", new byte[] { 1, 2, 3 });

            using (var fs = new FileStream(path, FileMode.Open, FileAccess.ReadWrite))
            {
                fs.Seek(24, SeekOrigin.Begin);
                fs.Write(BitConverter.GetBytes(long.MaxValue - 100));
            }

            // The live open path shares the clamp: a garbage offset resets the log instead
            // of letting the first Append write past the mapping.
            using (var wal = WriteAheadLog.Open(path, new NodeId(0), new SegmentId(1UL), initialCapacity: 1024 * 1024))
                wal.Append(500, Ameto.Core.LogLevel.Information, 3, "tmpl-x", new byte[] { 42 });

            var (_, entries) = WriteAheadLog.ReadForRecovery(path);
            Assert.Single(entries);
            Assert.Equal(500, entries[0].TimestampTicks);
        }
        finally { File.Delete(path); File.Delete(path + ".pool"); }
    }

    [Fact]
    public void Flush_BetweenAppends_KeepsAllEntriesRecoverable()
    {
        string path = NewWalPath();
        try
        {
            using (var wal = WriteAheadLog.Open(path, new NodeId(0), new SegmentId(1UL), initialCapacity: 1024 * 1024))
            {
                wal.Append(100, Ameto.Core.LogLevel.Error, 0, "tmpl-a", new byte[] { 1 });
                wal.Flush(); // the periodic msync the engine drives
                wal.Append(200, Ameto.Core.LogLevel.Error, 0, "tmpl-a", new byte[] { 2 });
            }

            var (_, entries) = WriteAheadLog.ReadForRecovery(path);
            Assert.Equal(2, entries.Count);
        }
        finally { File.Delete(path); File.Delete(path + ".pool"); }
    }
}
