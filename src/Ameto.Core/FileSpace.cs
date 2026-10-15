using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;

namespace Ameto.Core;

/// <summary>
/// RESERVES THE DISK BLOCKS OF A FILE A LOG HAS JUST SIZED, where the platform leaves them unallocated
/// (round-5 review H-F).
///
/// <para>The logs' write-ahead logs and spill files, and the metric WAL, are written through a memory
/// mapping over a file sized with <see cref="FileStream.SetLength"/> (or a mapping's own capacity). On
/// Linux that is <c>ftruncate</c>: the file is sparse, and a block is taken only when a write through the
/// mapping first touches its page. On a volume that has filled meanwhile — segments, merges and other
/// files take it — that write is SIGBUS, and the process dies: it never reaches the IOException that a
/// failed append, a failed grow and the spill's free-space floor answer 503 with, and the writers had
/// been answered 200 right up to it. NTFS allocates on SetLength, so there the same full volume fails
/// the open or the grow, cleanly. This makes Linux do the same: the blocks are reserved where the size
/// is set, and a volume that cannot hold them is an <see cref="IOException"/> there.</para>
///
/// <para>What it costs: the space is taken at once, as NTFS takes it — a 64 MB WAL holds 64 MB of the
/// volume from its open, not from its writes. <c>posix_fallocate</c>; on a filesystem that cannot
/// reserve (glibc then writes a byte into each block itself; musl reports it unsupported) the file is
/// left as it was, sparse, as before.</para>
/// </summary>
public static partial class FileSpace
{
    /// <summary>
    /// Reserves bytes [<paramref name="offset"/>, <paramref name="offset"/> + <paramref name="length"/>) of
    /// <paramref name="file"/>, which its size already covers. Throws <see cref="IOException"/> when the volume
    /// cannot hold them (or the reservation fails outright); returns having done nothing on Windows (SetLength
    /// allocated), on platforms other than Linux, and on a filesystem that cannot reserve.
    /// </summary>
    public static void Reserve(SafeFileHandle file, long offset, long length)
    {
        if (length <= 0 || offset < 0 || !OperatingSystem.IsLinux()) return;

        bool added = false;
        try
        {
            file.DangerousAddRef(ref added);
            int fd = (int)file.DangerousGetHandle();
            int error;
            do { error = Native.PosixFallocate(fd, offset, length); }
            while (error == EINTR);

            if (error == 0 || error is EOPNOTSUPP or EINVAL or ENODEV or ESPIPE or ENOSYS) return;
            string what = error switch
            {
                ENOSPC or EDQUOT => "the volume is full",
                EFBIG            => "the file would be too large",
                _                => $"posix_fallocate failed with errno {error}",
            };
            throw new IOException($"Could not reserve {length:N0} bytes at {offset:N0} of a log file: {what}");
        }
        catch (EntryPointNotFoundException) { /* no posix_fallocate in this libc: left as it was */ }
        catch (DllNotFoundException)        { /* no libc to call: left as it was */ }
        finally
        {
            if (added) file.DangerousRelease();
        }
    }

    private const int EINTR = 4, ENODEV = 19, EINVAL = 22, EFBIG = 27, ENOSPC = 28, ESPIPE = 29, ENOSYS = 38,
                      EOPNOTSUPP = 95, EDQUOT = 122;

    private static partial class Native
    {
        /// <summary><c>int posix_fallocate(int fd, off_t offset, off_t len)</c>: returns an error number, not -1 and errno.</summary>
        [LibraryImport("libc", EntryPoint = "posix_fallocate")]
        internal static partial int PosixFallocate(int fd, long offset, long length);
    }
}
