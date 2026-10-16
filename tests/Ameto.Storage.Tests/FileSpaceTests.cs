using System.Diagnostics;
using Ameto.Core;

namespace Ameto.Storage.Tests;

/// <summary>
/// <see cref="FileSpace.Reserve"/> on the file systems a log can live on (round-6 review R5-M13).
///
/// <para>It called glibc's <c>posix_fallocate</c>, which on a file system that cannot reserve — 9p (WSL's
/// <c>/mnt/c</c>), most FUSE file systems (Docker Desktop's bind mounts) — falls back to writing a byte
/// into every block itself: a 64 MB WAL's open took ~3.3 s on 9p, under the log engine's swap lock, where
/// the decision was that such a file stays sparse. It calls <c>fallocate(2)</c> now, which answers
/// EOPNOTSUPP there at once and writes nothing.</para>
///
/// <para>The fact needs such a file system, which no CI runner has: <c>AMETO_TEST_NO_FALLOCATE_DIR</c> names
/// a directory on one (the round-6 WSL runs point it at a <c>/mnt/c</c> directory). Without it, and off
/// Linux, it returns early.</para>
/// </summary>
public sealed class FileSpaceTests
{
    /// <summary>
    /// A reservation on a file system that cannot reserve leaves the file untouched: not a byte written —
    /// the emulation's writes moved the file's modification time — and no time spent writing them.
    /// </summary>
    [Fact]
    public void On_a_file_system_that_cannot_reserve_a_reservation_writes_nothing()
    {
        string? dir = Environment.GetEnvironmentVariable("AMETO_TEST_NO_FALLOCATE_DIR");
        if (!OperatingSystem.IsLinux() || string.IsNullOrEmpty(dir)) return;

        string path = Path.Combine(dir, $"reserve-{Guid.NewGuid():N}.bin");
        try
        {
            const long size = 8L * 1024 * 1024;
            var long_ago = new DateTime(2001, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            using (var fs = new FileStream(path, FileMode.CreateNew, FileAccess.ReadWrite))
            {
                fs.SetLength(size);
                fs.Flush(flushToDisk: true);
                File.SetLastWriteTimeUtc(path, long_ago);

                var took = Stopwatch.StartNew();
                FileSpace.Reserve(fs.SafeFileHandle, 0, size);
                took.Stop();

                DateTime modified = File.GetLastWriteTimeUtc(path);
                Assert.True(modified == long_ago,
                    $"the reservation wrote into a file its file system cannot reserve: modified {modified:O}, " +
                    $"{took.Elapsed.TotalMilliseconds:F0} ms for {size:N0} bytes");
            }
        }
        finally { try { File.Delete(path); } catch { } }
    }
}
