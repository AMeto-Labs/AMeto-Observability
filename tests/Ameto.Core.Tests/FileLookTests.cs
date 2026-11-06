using Ameto.Core;

namespace Ameto.Core.Tests;

/// <summary>
/// #138 item 7: <c>File.Exists</c> answers false for a file it could not look at as well as for one that is
/// gone, and the checks that decided from that false that a file was gone overwrote a secret key, dropped a
/// live segment or wrote a merge without a source's edges. <see cref="FileLook.Of"/> answers three ways, and
/// the secret-key factories read first and generate only on "not found".
/// </summary>
public sealed class FileLookTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-look-" + Guid.NewGuid().ToString("N"));

    public FileLookTests() => Directory.CreateDirectory(_dir);

    public void Dispose()
    {
        FileRefusal.ForTest.Value = null;
        try { Directory.Delete(_dir, recursive: true); } catch { }
    }

    [Fact]
    public void There_gone_and_a_gone_directory()
    {
        string file = Path.Combine(_dir, "a.bin");
        File.WriteAllBytes(file, [1, 2, 3]);

        Assert.Equal(FilePresence.There, FileLook.Of(file));
        Assert.Equal(FilePresence.Gone,  FileLook.Of(Path.Combine(_dir, "missing.bin")));
        Assert.Equal(FilePresence.Gone,  FileLook.Of(Path.Combine(_dir, "no-such-dir", "x.bin")));
    }

    [Fact]
    public void A_look_the_file_system_cannot_answer_is_Unknown_not_Gone()
    {
        string file = Path.Combine(_dir, "held.bin");
        File.WriteAllBytes(file, [1]);
        FileRefusal.ForTest.Value = (path, access) =>
            path == file && access == FileRefusal.Access.Look ? new IOException("the share did not answer (test seam)") : null;

        Assert.Equal(FilePresence.Unknown, FileLook.Of(file));
    }

    // ── The secret key ───────────────────────────────────────────────────────

    [Fact]
    public void An_existing_secret_key_is_read_not_regenerated()
    {
        var first  = SecretProtectorFactory.Create(_dir, configuredKey: null);
        string sealedText = first.Protect("hunter2");
        string keyOnDisk  = File.ReadAllText(Path.Combine(_dir, "secret.key"));

        var second = SecretProtectorFactory.Create(_dir, configuredKey: null);

        Assert.Equal("hunter2", second.Unprotect(sealedText));
        Assert.Equal(keyOnDisk, File.ReadAllText(Path.Combine(_dir, "secret.key")));
    }

    /// <summary>
    /// The key file is there but cannot be read (a share that did not answer): before, <c>File.Exists</c>
    /// answered false, a new key was generated and written over it, and every stored secret was lost. Now the
    /// start fails — recoverable — and the file is untouched. This pins the new contract: main never consulted
    /// the seam (its <c>Create</c> returned normally), so on main this fact is red on the null exception, not
    /// on the harmful branch, which no seam reaches.
    /// </summary>
    [Fact]
    public void A_secret_key_that_cannot_be_read_is_not_overwritten()
    {
        SecretProtectorFactory.Create(_dir, configuredKey: null);
        string path      = Path.Combine(_dir, "secret.key");
        string keyOnDisk = File.ReadAllText(path);
        int    generated = 0;
        FileRefusal.ForTest.Value = (p, access) =>
            p == path && access == FileRefusal.Access.Open ? new IOException("the share did not answer (test seam)") : null;

        var ex = Record.Exception(() => SecretProtectorFactory.Create(_dir, configuredKey: null, onGeneratedKey: _ => generated++));

        Assert.IsType<IOException>(ex);
        Assert.Equal(0, generated);
        FileRefusal.ForTest.Value = null;
        Assert.Equal(keyOnDisk, File.ReadAllText(path));
    }

    [Fact]
    public void A_missing_secret_key_is_generated_once()
    {
        int generated = 0;
        SecretProtectorFactory.Create(_dir, configuredKey: null, onGeneratedKey: _ => generated++);
        SecretProtectorFactory.Create(_dir, configuredKey: null, onGeneratedKey: _ => generated++);

        Assert.Equal(1, generated);
    }

    /// <summary>
    /// The key file's write: two writers on one path, the second finds the first's key in place and fails
    /// (never over one that exists), the first's bytes are on disk, and neither leaves a temp behind.
    /// </summary>
    [Fact]
    public void A_key_file_is_written_once_and_a_second_writer_fails_without_a_temp_left_behind()
    {
        string path = Path.Combine(_dir, "k.key");

        KeyFile.WriteNew(path, "first"u8);
        Assert.Throws<IOException>(() => KeyFile.WriteNew(path, "second"u8));

        Assert.Equal("first", File.ReadAllText(path));
        Assert.Empty(Directory.GetFiles(_dir, "*.tmp"));
    }
}
