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
}
