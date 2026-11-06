namespace Ameto.Core;

/// <summary>What a look at a path found (<see cref="FileLook.Of"/>).</summary>
public enum FilePresence
{
    /// <summary>The file system answered: the file is there.</summary>
    There,
    /// <summary>The file system answered: the file, or its directory, is not there.</summary>
    Gone,
    /// <summary>The file system could not be asked: a share that did not answer, an access check, a sharing violation.</summary>
    Unknown,
}

/// <summary>
/// Whether a path is there, as the file system answers it — THREE ways, not two. <see cref="File.Exists"/>
/// answers false for a file it could not look at as well as for one that is gone, and every check that
/// decided from that false that a file was gone did the wrong thing on a share that did not answer, an
/// antivirus hold or an access check: a secret key regenerated over the one that existed (every stored
/// secret undecryptable), a live trace segment dropped from the catalog with its index runs, a merge
/// written without a source's edges, a manifest that could not be read taken for no manifest, in silence
/// (#138 item 7).
/// The log engine kept its own copy of this since round 5 (<c>StorageEngine.Look</c>, which now delegates here).
/// </summary>
public static class FileLook
{
    /// <summary><see cref="FilePresence.Gone"/> only when the file system says so; <see cref="FilePresence.Unknown"/> when it could not be asked.</summary>
    public static FilePresence Of(string path)
    {
        try
        {
            FileRefusal.ThrowIfRefused(path, FileRefusal.Access.Look);
            File.GetAttributes(path);
            return FilePresence.There;
        }
        catch (FileNotFoundException)      { return FilePresence.Gone; }
        catch (DirectoryNotFoundException) { return FilePresence.Gone; }
        catch (Exception)                  { return FilePresence.Unknown; }
    }
}
