namespace Ameto.Core;

/// <summary>
/// TEST SEAM FOR WHAT THE FILE SYSTEM ANSWERS ABOUT A PATH (round-6 review R5-M6).
///
/// <para>A log is gone only when the file system says so — FileNotFoundException or
/// DirectoryNotFoundException — and a look or an open that fails any other way (a share that did not
/// answer, an access check, a sharing violation) is a read that failed, after which recovery keeps the
/// log and tries again. <c>File.Exists</c> answers false for both, and the recovery reads, the
/// completion-marker checks and the logs' opens took that false for absence: an unreadable log read as
/// empty and was deleted, a completion marker that could not be looked at let a flushed block be
/// written a second time, and an open re-initialised an existing log as a new one.</para>
///
/// <para>Round 5's seam (<c>StorageEngine.LookFailsForTest</c>) reached only the look; this one reaches the
/// real opens as well. A test answers a <see cref="Access.Look"/> or an <see cref="Access.Open"/> of a path
/// with the exception the file system would throw, on every platform. An <see cref="AsyncLocal{T}"/>,
/// so it reaches only the flows of the test that sets it, created by the first test that asks for it
/// (no static initializer, round-5 M-A): production reads a null field.</para>
/// </summary>
internal static class FileRefusal
{
    /// <summary>What is asked of the file system.</summary>
    internal enum Access
    {
        /// <summary>Its attributes: whether the path is there.</summary>
        Look,
        /// <summary>An open of the file, for reading or writing.</summary>
        Open,
    }

    /// <summary>The seam; null in production. See the class remarks.</summary>
    internal static AsyncLocal<Func<string, Access, Exception?>?> ForTest =>
        Volatile.Read(ref s_forTest)
        ?? Interlocked.CompareExchange(ref s_forTest, new AsyncLocal<Func<string, Access, Exception?>?>(), null)
        ?? s_forTest!;

    /// <summary>What <see cref="ForTest"/> made; null until a test asks for it.</summary>
    private static AsyncLocal<Func<string, Access, Exception?>?>? s_forTest;

    /// <summary>Throws what the seam says the file system answers to this <paramref name="access"/> of <paramref name="path"/>.</summary>
    internal static void ThrowIfRefused(string path, Access access)
    {
        if (Volatile.Read(ref s_forTest)?.Value is { } refuse && refuse(path, access) is { } answer)
            throw answer;
    }
}
