using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;

namespace Ameto.Core;

/// <summary>
/// RESERVES THE DISK BLOCKS OF A FILE A LOG HAS JUST SIZED, where the platform leaves them unallocated
/// (round-5 review H-F).
///
/// <para>The three write-ahead logs — the logs' WAL and its spill files, the span WAL (round-6 review
/// R5-H1) and the metric WAL — are written through a memory mapping over a file sized with
/// <see cref="FileStream.SetLength"/> (or a mapping's own capacity). On Linux that is <c>ftruncate</c>:
/// the file is sparse, and a block is taken only when a write through the mapping first touches its
/// page. On a volume that has filled meanwhile — segments, merges and other files take it — that write
/// is SIGBUS, and the process dies: it never reaches the IOException that a failed append, a failed grow
/// and the spill's free-space floor answer 503 with, and the writers had been answered 200 right up to
/// it. NTFS allocates on SetLength, so there the same full volume fails the open or the grow, cleanly.
/// This makes Linux do the same: the blocks are reserved where the size is set, and a volume that cannot
/// hold them is an <see cref="IOException"/> there. Each log reserves its WHOLE file at every open, not
/// only what the open adds (round-6 review R5-H2): a file an earlier build left sparse, or that a failed
/// open left sized, has its holes filled before anything is written through its mapping.</para>
///
/// <para>What it costs: the space is taken at once, as NTFS takes it — a 64 MB WAL holds 64 MB of the
/// volume from its open, not from its writes. It is <c>fallocate(2)</c> itself, mode 0, NOT
/// <c>posix_fallocate</c> (round-6 review R5-M13): where the file system cannot reserve — 9p (WSL's
/// <c>/mnt/c</c>), most FUSE file systems (Docker Desktop's bind mounts) — glibc's
/// <c>posix_fallocate</c> falls back to writing a byte into every block itself, which took 2.4–3.3 s for
/// a 64 MB WAL on 9p, under the log engine's swap lock. The system call answers EOPNOTSUPP there at once,
/// and the file is left as it was, which is what a file system that cannot reserve means (9p's files are
/// NTFS's, which allocates on SetLength anyway). On ext4 the call costs next to nothing (unwritten extents;
/// measured 0.0 ms for 64 MB), on tmpfs what the pages cost (3.4 ms).</para>
/// </summary>
public static partial class FileSpace
{
    /// <summary>
    /// Reserves bytes [<paramref name="offset"/>, <paramref name="offset"/> + <paramref name="length"/>) of
    /// <paramref name="file"/>, which its size already covers. Throws <see cref="IOException"/> when the volume
    /// cannot hold them (or the reservation fails outright), naming <paramref name="path"/> and the bytes its
    /// volume has free (round-6 review: a start that refused said neither); returns having done nothing on
    /// Windows (SetLength allocated), on platforms other than Linux, and on a filesystem that cannot reserve.
    /// Blocks a range already has are kept as they are: reserving an allocated range is cheap and changes
    /// nothing in it.
    /// </summary>
    public static void Reserve(SafeFileHandle file, long offset, long length, string path)
    {
        if (length <= 0 || offset < 0) return;
        if (Volatile.Read(ref s_reserveForTest)?.Value is { } seam && seam(file, offset, length) is { } refused)
            throw refused;
        Func<int>? answers = Volatile.Read(ref s_fallocateForTest)?.Value;
        if (answers is null && !OperatingSystem.IsLinux()) return;

        bool added = false;
        try
        {
            int error;
            if (answers is not null)
            {
                do error = answers(); while (error == EINTR);
            }
            else
            {
                file.DangerousAddRef(ref added);
                int fd = (int)file.DangerousGetHandle();
                do { error = Native.Fallocate(fd, 0, offset, length) == 0 ? 0 : Marshal.GetLastPInvokeError(); }
                while (error == EINTR);
            }

            if (error == 0 || error is EOPNOTSUPP or EINVAL or ENODEV or ESPIPE or ENOSYS) return;
            throw Refusal(error, offset, length, path);
        }
        catch (EntryPointNotFoundException) { /* no fallocate in this libc: left as it was */ }
        catch (DllNotFoundException)        { /* no libc to call: left as it was */ }
        finally
        {
            if (added) file.DangerousRelease();
        }
    }

    /// <summary>What a reservation the volume refused says: the file, the range, the cause, and the bytes the volume has free.</summary>
    private static IOException Refusal(int error, long offset, long length, string path)
    {
        string what = error switch
        {
            ENOSPC or EDQUOT => "the volume is full",
            EFBIG            => "the file would be too large",
            _                => $"fallocate failed with errno {error}",
        };
        long free = FreeBytesBeside(path);
        string room = free >= 0 ? $"{free:N0} bytes free on its volume" : "its volume's free space could not be read";
        return new IOException($"Could not reserve {length:N0} bytes at {offset:N0} of the log file {path} ({room}): {what}");
    }

    /// <summary>The bytes free to this process on the volume that holds <paramref name="path"/>, by its directory (statvfs on Linux); -1 when it cannot be read.</summary>
    private static long FreeBytesBeside(string path)
    {
        try { return new DriveInfo(Path.GetDirectoryName(Path.GetFullPath(path)) ?? path).AvailableFreeSpace; }
        catch (Exception) { return -1; }
    }

    /// <summary>
    /// Test seam: asked by every <see cref="Reserve"/> with the range, on every platform, before anything is
    /// reserved — an exception it returns is thrown there, as a volume that cannot hold the range would be
    /// on Linux; null goes on to the reservation. An <see cref="AsyncLocal{T}"/>, so it reaches only the
    /// flows of the test that sets it, created by the first test that asks for it: no static initializer
    /// (round-5 review M-A), and production reads a null field.
    /// </summary>
    internal static AsyncLocal<Func<SafeFileHandle, long, long, Exception?>?> ReserveForTest =>
        Volatile.Read(ref s_reserveForTest)
        ?? Interlocked.CompareExchange(ref s_reserveForTest, new AsyncLocal<Func<SafeFileHandle, long, long, Exception?>?>(), null)
        ?? s_reserveForTest!;

    /// <summary>What <see cref="ReserveForTest"/> made; null until a test asks for it.</summary>
    private static AsyncLocal<Func<SafeFileHandle, long, long, Exception?>?>? s_reserveForTest;

    /// <summary>
    /// Test seam: the system call's answer — 0, or the errno it set — in place of the call itself, on every
    /// platform, so the classification of its answers runs where no file system refuses (round-6 review: it had
    /// no CI coverage). Called again for EINTR, as the call is. Made as <see cref="ReserveForTest"/> is.
    /// </summary>
    internal static AsyncLocal<Func<int>?> FallocateForTest =>
        Volatile.Read(ref s_fallocateForTest)
        ?? Interlocked.CompareExchange(ref s_fallocateForTest, new AsyncLocal<Func<int>?>(), null)
        ?? s_fallocateForTest!;

    /// <summary>What <see cref="FallocateForTest"/> made; null until a test asks for it.</summary>
    private static AsyncLocal<Func<int>?>? s_fallocateForTest;

    internal const int ErrnoInterrupted = EINTR, ErrnoNoSpace = ENOSPC, ErrnoTooLarge = EFBIG, ErrnoNotSupported = EOPNOTSUPP,
                       ErrnoIo = 5;

    private const int EINTR = 4, ENODEV = 19, EINVAL = 22, EFBIG = 27, ENOSPC = 28, ESPIPE = 29, ENOSYS = 38,
                      EOPNOTSUPP = 95, EDQUOT = 122;

    private static partial class Native
    {
        /// <summary><c>int fallocate(int fd, int mode, off_t offset, off_t len)</c>: 0, or -1 with errno.</summary>
        [LibraryImport("libc", EntryPoint = "fallocate", SetLastError = true)]
        internal static partial int Fallocate(int fd, int mode, long offset, long length);
    }
}
