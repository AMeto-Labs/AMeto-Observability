using System.Runtime.InteropServices;

namespace Ameto.Core;

/// <summary>
/// THE COMMIT POINT OF A WRITE-AHEAD LOG'S FORMAT UPGRADE: a rename that survives a power loss. Both
/// logs upgrade the same way — the old file rewritten in the new layout beside it, fsynced, and moved
/// over it — and the move is what makes the new file the log. An atomic rename the disk has not
/// committed can come undone in a power loss and bring the old file back under the name: for the
/// metric WAL with the watermark it had at the upgrade (every flush since replays as duplicates,
/// every point logged since is gone), for the span WAL with the generation it had (every span
/// logged since is gone).
///
/// <para>IN Ameto.Core BECAUSE IT HAS TWO CALLERS. It was written for the metric WAL's v1 → v2 upgrade
/// (44797aa, bedba1b) and lived inside that class; the span WAL's upgrade (#103) needs exactly the
/// same guarantee over exactly the same operation, and a second copy is the shape
/// <see cref="FileBounds"/> and <see cref="Crc32c"/> were moved here to end — the last copy diverged
/// within a single review round.</para>
/// </summary>
public static partial class DurableFile
{
    /// <summary>
    /// <paramref name="from"/> replaces <paramref name="to"/> atomically and, on Windows, durably —
    /// <c>MoveFileEx</c> with <c>MOVEFILE_WRITE_THROUGH</c> does not return until the move is on the
    /// disk, which <see cref="File.Move(string, string, bool)"/> cannot ask for. Elsewhere it is
    /// <see cref="File.Move(string, string, bool)"/> (rename(2), atomic) and the caller fsyncs the
    /// directory (<see cref="SyncDirectory"/>). Fails the way File.Move does —
    /// <see cref="UnauthorizedAccessException"/> for access denied, <see cref="IOException"/>
    /// otherwise — because the upgrades' retry around it filters on exactly those two, and a raw
    /// Win32Exception would escape that retry and fail the engine's start.
    ///
    /// <para>The paths go to <c>MoveFileExW</c> in their <c>\\?\</c> form, because without it the call
    /// is limited to MAX_PATH where File.Move is not — a data directory deep enough would have failed
    /// the upgrade on every start. <c>MOVEFILE_COPY_ALLOWED</c> is not passed, deliberately: the copy
    /// sits beside the log, so this is always a rename, and a copy-and-delete (the one case where
    /// write-through covers less than the data) cannot happen — which is why no FlushFileBuffers of
    /// the result follows it.</para>
    /// </summary>
    public static void Replace(string from, string to)
    {
        if (!OperatingSystem.IsWindows())
        {
            File.Move(from, to, overwrite: true);
            return;
        }

        const uint MoveFileReplaceExisting = 0x1, MoveFileWriteThrough = 0x8;
        if (Native.MoveFileEx(ExtendedPath(from), ExtendedPath(to), MoveFileReplaceExisting | MoveFileWriteThrough))
            return;

        int error = Marshal.GetLastPInvokeError();
        string message = $"Could not move '{from}' over '{to}': {Marshal.GetPInvokeErrorMessage(error)}";
        if (error == 5) throw new UnauthorizedAccessException(message);        // ERROR_ACCESS_DENIED
        throw new IOException(message, unchecked((int)0x80070000) | error);     // HRESULT_FROM_WIN32
    }

    /// <summary>The Win32 extended-length (<c>\\?\</c>, or <c>\\?\UNC\</c>) form of a path, which the W APIs take past MAX_PATH.</summary>
    private static string ExtendedPath(string path)
    {
        string full = Path.GetFullPath(path);
        if (full.StartsWith(@"\\?\", StringComparison.Ordinal) || full.StartsWith(@"\\.\", StringComparison.Ordinal)) return full;
        return full.StartsWith(@"\\", StringComparison.Ordinal) ? @"\\?\UNC\" + full[2..] : @"\\?\" + full;
    }

    /// <summary>
    /// fsyncs the directory <paramref name="directory"/>, so a rename inside it survives a power loss.
    /// True where that happened or where the platform has nothing to do (Windows, whose move was
    /// write-through); false when it could not be done — a failed call, or no libc to call at all.
    /// It runs AFTER the rename has committed, so it must not throw: an exception here would fail
    /// the engine's start over a log that is already upgraded. macOS gets fsync, not F_FULLFSYNC —
    /// the residual the logs already accept there.
    /// </summary>
    public static bool SyncDirectory(string directory)
    {
        if (OperatingSystem.IsWindows()) return true;
        if (!OperatingSystem.IsLinux() && !OperatingSystem.IsMacOS()) return false;

        try
        {
            int fd = Native.Open(directory, 0 /* O_RDONLY */);
            if (fd < 0) return false;
            try { return Native.Fsync(fd) == 0; }
            finally { Native.Close(fd); }
        }
        catch (Exception) { return false; }      // DllNotFoundException, EntryPointNotFoundException, …
    }

    private static partial class Native
    {
        [LibraryImport("kernel32.dll", EntryPoint = "MoveFileExW", StringMarshalling = StringMarshalling.Utf16, SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        internal static partial bool MoveFileEx(string existingFileName, string newFileName, uint flags);

        [LibraryImport("libc", EntryPoint = "open", StringMarshalling = StringMarshalling.Utf8, SetLastError = true)]
        internal static partial int Open(string path, int flags);

        [LibraryImport("libc", EntryPoint = "fsync", SetLastError = true)]
        internal static partial int Fsync(int fd);

        [LibraryImport("libc", EntryPoint = "close", SetLastError = true)]
        internal static partial int Close(int fd);
    }
}
