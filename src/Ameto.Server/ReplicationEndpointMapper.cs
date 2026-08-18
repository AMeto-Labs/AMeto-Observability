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

        // ── Peer probe — authenticated by the replication shared secret ───────
        // Caller announces itself; this node replies with its own identity.
        // Both sides upsert the received payload into their NodeRegistry.
        app.MapPost("/api/replication/ping",
            (PeerPayload payload,
             HttpRequest request,
             NodeRegistry registry,
             IOptions<ReplicationOptions> replicationOpts,
             IOptions<Ameto.Core.ServerOptions> serverOpts) =>
            {
                if (!PeerAuthorised(request, replicationOpts.Value)) return Results.Unauthorized();
                registry.Upsert(payload);
                return Results.Ok(new PeerPayload
                {
                    NodeId    = serverOpts.Value.NodeId.Value,
                    Address   = replicationOpts.Value.LocalAddress,
                    Timestamp = DateTimeOffset.UtcNow,
                });
            });

        // ── Segment receive — authenticated by the replication shared secret ──
        // Write-then-rename ensures the segment file is never seen half-written.
        app.MapPost("/api/replication/segments/{nodeId}/{segmentId}",
            async (uint         nodeId,
                   ulong        segmentId,
                   HttpRequest  request,
                   StorageEngine storage,
                   IOptions<ReplicationOptions> replicationOpts,
                   IOptions<Ameto.Core.ServerOptions> serverOpts) =>
            {
                if (!PeerAuthorised(request, replicationOpts.Value)) return Results.Unauthorized();
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

                // Idempotent by construction: the name comes from the route, so a re-push of a
                // segment this node already holds lands on its own path and refreshes its own
                // catalog entry. That is normal traffic and stays a 204.
                File.Move(tmpPath, filePath, overwrite: true);

                var outcome = storage.ImportSegment(filePath);
                if (outcome == SegmentImportOutcome.Registered) return Results.NoContent();

                // Nothing in the engine points at what we just wrote. Unlinking it is not
                // tidiness: the catalog is rebuilt from this directory on every start, and in
                // enumeration order, so a file left behind here would let the next boot pick the
                // winner by chance and undo the refusal.
                try { File.Delete(filePath); } catch { /* the boot scan skips what it cannot key */ }

                return outcome == SegmentImportOutcome.Conflict
                    // 409 rather than a log line nobody reads: a DIFFERENT file already holds
                    // this (nodeId, segmentId), which means the sender and this node are both
                    // configured as NodeId {nodeId}. Registering it would have dropped whatever
                    // is being served under that key out of queries, retention and the merge
                    // planner at once — the file staying on disk the whole time, so nothing
                    // anywhere would look wrong. The sender is the only party positioned to tell
                    // that from a healthy push, so the sender is told.
                    ? Results.Problem(
                        $"Segment {nodeId}-{segmentId} is already held by a different file on this node; " +
                        $"two nodes appear to be configured with NodeId {nodeId}.",
                        statusCode: StatusCodes.Status409Conflict)
                    : Results.Problem(
                        $"Segment {nodeId}-{segmentId} could not be read as a segment file.",
                        statusCode: StatusCodes.Status400BadRequest);
            });
    }

    /// <summary>
    /// Constant-time check of the <c>X-Ameto-Replication</c> header against the
    /// configured secret. A blank secret fails closed — an enabled-but-unconfigured
    /// node never accepts peer writes, so an exposed port can't inject segments.
    /// </summary>
    private static bool PeerAuthorised(HttpRequest request, ReplicationOptions opts)
    {
        if (string.IsNullOrEmpty(opts.Secret)) return false;
        var presented = request.Headers["X-Ameto-Replication"].ToString();
        if (string.IsNullOrEmpty(presented)) return false;
        return System.Security.Cryptography.CryptographicOperations.FixedTimeEquals(
            System.Text.Encoding.UTF8.GetBytes(presented),
            System.Text.Encoding.UTF8.GetBytes(opts.Secret));
    }
}
