using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Options;
using Ameto.Replication;
using Ameto.Storage;

namespace Ameto.Server;

public static class ReplicationEndpointMapper
{
    /// <summary>
    /// Maps replication endpoints (only registered when Replication.Enabled = true):
    ///   GET  /api/replication/nodes                          — list known peers + health
    ///   POST /api/replication/ping                           — peer probe (bidirectional presence)
    ///   POST /api/replication/segments/{nodeId}/{segmentId}  — receive a replicated .seg file
    /// </summary>
    public static void MapReplicationEndpoints(this WebApplication app)
    {
        var opts = app.Services.GetService<IOptions<ReplicationOptions>>()?.Value;
        if (opts is null || !opts.Enabled) return;

        // ── Node list (authenticated — management use) ────────────────────────
        app.MapGet("/api/replication/nodes", (NodeRegistry registry) =>
            Results.Ok(registry.GetAll().Select(n => new
            {
                id          = n.Id.Value,
                address     = n.BaseAddress,
                lastSeenUtc = n.LastSeen,
                healthy     = n.IsHealthy,
            }))).RequireAuthorization();

        // ── Peer probe — unauthenticated peer-to-peer exchange ─────────────────
        // Caller announces itself; this node replies with its own identity.
        // Both sides upsert the received payload into their NodeRegistry.
        app.MapPost("/api/replication/ping",
            (PeerPayload payload,
             NodeRegistry registry,
             IOptions<ReplicationOptions> replicationOpts,
             IOptions<Ameto.Core.ServerOptions> serverOpts) =>
            {
                registry.Upsert(payload);
                return Results.Ok(new PeerPayload
                {
                    NodeId    = serverOpts.Value.NodeId.Value,
                    Address   = replicationOpts.Value.LocalAddress,
                    Timestamp = DateTimeOffset.UtcNow,
                });
            });

        // ── Segment receive — unauthenticated peer-to-peer transfer ───────────
        // Write-then-rename ensures the segment file is never seen half-written.
        app.MapPost("/api/replication/segments/{nodeId}/{segmentId}",
            async (uint         nodeId,
                   ulong        segmentId,
                   HttpRequest  request,
                   StorageEngine storage,
                   IOptions<Ameto.Core.ServerOptions> serverOpts) =>
            {
                var segDir   = Path.Combine(serverOpts.Value.DataDirectory, "segments");
                var fileName = $"{nodeId}-{segmentId}.seg";
                var filePath = Path.Combine(segDir, fileName);

                Directory.CreateDirectory(segDir);

                var tmpPath = filePath + ".tmp";
                try
                {
                    await using var file = File.Create(tmpPath);
                    await request.Body.CopyToAsync(file);
                }
                catch
                {
                    try { File.Delete(tmpPath); } catch { /* ignore */ }
                    return Results.Problem("Failed to write segment file.");
                }

                File.Move(tmpPath, filePath, overwrite: true);
                storage.ImportSegment(filePath);

                return Results.NoContent();
            });
    }
}
