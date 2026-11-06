using Ameto.Core;
using Ameto.Tracing.Storage;

namespace Ameto.Storage.Tests;

/// <summary>
/// #138 item 7: a merge asks <c>ServiceGraphSidecar.TryReadEdges</c> for a source's edges and deletes the
/// source afterwards. "No sidecar" is no edges, truthfully — but <c>File.Exists</c> answered false for a
/// sidecar it could not LOOK at too, and the merge then wrote its output without that source's edges, for
/// good. A look the file system cannot answer now reads as an unreadable sidecar: false, so the merge that
/// rebuilds edges from spans takes the source.
/// </summary>
public sealed class ServiceGraphSidecarLookTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-svcgraph-look-" + Guid.NewGuid().ToString("N"));

    public ServiceGraphSidecarLookTests() => Directory.CreateDirectory(_dir);

    public void Dispose()
    {
        FileRefusal.ForTest.Value = null;
        try { Directory.Delete(_dir, recursive: true); } catch { }
    }

    [Fact]
    public void No_sidecar_is_no_edges()
    {
        Assert.True(ServiceGraphSidecar.TryReadEdges(Path.Combine(_dir, "seg.trc"), out var edges));
        Assert.Empty(edges);
    }

    [Fact]
    public void A_sidecar_that_cannot_be_looked_at_is_not_no_edges()
    {
        string trc     = Path.Combine(_dir, "seg.trc");
        string sidecar = Path.ChangeExtension(trc, ".svcgraph");
        File.WriteAllBytes(sidecar, [0, 0, 0, 0]);
        FileRefusal.ForTest.Value = (path, access) =>
            path == sidecar && access == FileRefusal.Access.Look ? new IOException("the share did not answer (test seam)") : null;

        Assert.False(ServiceGraphSidecar.TryReadEdges(trc, out _));
    }

    [Fact]
    public void A_sidecar_that_is_there_but_will_not_read_is_still_false()
    {
        string trc = Path.Combine(_dir, "seg.trc");
        File.WriteAllBytes(Path.ChangeExtension(trc, ".svcgraph"), [0xDE, 0xAD, 0xBE, 0xEF, 0, 0]);   // wrong magic

        Assert.False(ServiceGraphSidecar.TryReadEdges(trc, out _));
    }
}
