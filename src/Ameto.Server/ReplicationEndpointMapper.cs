using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features;
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

                // The body limit, decided before a byte of the body is read. Kestrel's default is
                // 30 MB, and what arrives here is a peer's hot-tier level segment: one flush
                // starts from a 64 MB budget (HotTier.MaxSizeBytes, and configurable upward),
                // writes one segment per level, and LZ4 is the only thing between that budget and
                // this endpoint. A flush concentrated on one level, or carrying properties that
                // do not compress, clears 30 MB without anything unusual happening.
                //
                // Over the limit the body read THROWS, and the catch below folded that into a
                // 500. What that cost is not what the comment here used to say. There is no
                // retry loop anywhere in Ameto.Replication: SegmentReplicator.OnSegmentFlushed
                // fires once per flushed segment and does one fire-and-forget PushAsync per
                // healthy peer, a non-success status is a LogWarning and the push is dropped.
                // So the fault was the quieter one — a single push, one warning naming a status
                // and no reason, and that segment never offered to that peer again. Permanent,
                // silent non-replication of the largest segments this node produces, which the
                // sender's contract told it to read as transient.
                //
                // Set per REQUEST rather than on the host, because this is the only endpoint
                // that receives a file and MaxRequestBodySize on ConfigureKestrel would hand
                // the same ceiling to OTLP ingest, the CLEF endpoint and the UI. Below the
                // secret check, so an unauthenticated caller still meets the 30 MB default —
                // the raised ceiling is something a peer authenticates for. The feature is
                // absent under TestServer, which enforces no limit at all, hence the match.
                if (request.HttpContext.Features.Get<IHttpMaxRequestBodySizeFeature>()
                        is { IsReadOnly: false } bodyLimit)
                    bodyLimit.MaxRequestBodySize = replicationOpts.Value.MaxSegmentBytes;

                var segDir   = Path.Combine(serverOpts.Value.DataDirectory, "segments");
                var fileName = $"{nodeId}-{segmentId}.seg";
                var filePath = Path.Combine(segDir, fileName);

                Directory.CreateDirectory(segDir);

                // The final path comes from the route, because that is what identifies the
                // segment. The STAGING path must not: two concurrent pushes to one
                // {nodeId}/{segmentId} — a peer retrying, a peer re-pushing after a
                // re-compression, or two peers misconfigured with one NodeId — computed the same
                // temp name and wrote into the same file.
                //
                // That was survivable only while the endpoint renamed BEFORE handing the file
                // over: the import read its metadata from the already-final path, so the catalog
                // entry always described the bytes sitting under it. Once the rename moved into
                // the engine, the two steps came apart. Request A finishes its body and closes
                // the handle; A's import reads EventCount, the timestamp span and MinLevel out of
                // the staged file and closes it; request B does File.Create on the same name —
                // FileMode.Create, which TRUNCATES — and starts writing its own body; A then
                // renames that file into place and registers what it read. The catalog describes
                // A's segment and the bytes under the final path are B's partial body, and
                // nothing looks wrong anywhere. On Windows the FileShare.None on File.Create
                // turns the same overlap into a sharing violation instead — a 500 on a healthy
                // push — so the failure is not even the same one on the two platforms we run on.
                //
                // The nonce is what MetricWriter uses for the same reason and the error handler
                // below is the other half of it: a failing request deleted the temp name it
                // computed, which under a shared name is another live push's staged body.
                var tmpPath = StagingPath(segDir, nodeId, segmentId);
                try
                {
                    await using var file = File.Create(tmpPath);
                    await request.Body.CopyToAsync(file);
                }
                catch (BadHttpRequestException tooLarge)
                    when (tooLarge.StatusCode == StatusCodes.Status413PayloadTooLarge)
                {
                    // Separated from the disk error below because the two ANSWERS differ, and the
                    // answer is the whole content of a status code here. A disk error is
                    // transient and the sender should come back; a body over the ceiling will be
                    // over it every time, so the sender must stop and somebody must raise
                    // MaxSegmentBytes on this node. Folded together they were one 500 marked
                    // "retry: yes", which is a published instruction to do the one thing that
                    // cannot work — and this codebase's own sender does not retry at all, so what
                    // it actually did with that instruction was log a status, drop the segment
                    // and never mention it again.
                    try { File.Delete(tmpPath); } catch { /* ignore */ }
                    return Results.Problem(
                        $"Segment {nodeId}-{segmentId} is larger than this node accepts " +
                        $"({replicationOpts.Value.MaxSegmentBytes} bytes). Retrying will not help; " +
                        "raise Ameto:Replication:MaxSegmentBytes on the receiver.",
                        statusCode: StatusCodes.Status413PayloadTooLarge);
                }
                catch
                {
                    // Everything else the body read can fail on IS worth another attempt: a
                    // full or failing disk, and a body that stopped arriving — Kestrel reports
                    // the dropped connection here too, and the same bytes over a live
                    // connection would land.
                    try { File.Delete(tmpPath); } catch { /* ignore */ }
                    return Results.Problem("Failed to write segment file.");
                }

                // The body stays at the temp name until the engine has decided what it is, and
                // the engine performs the rename itself. The name comes from the ROUTE, so it is
                // not this node's to say whether two pushes are the same segment: two peers
                // misconfigured with one NodeId send different segments to one path. Moving here
                // and asking afterwards destroyed the first peer's file before anything looked at
                // it, and what the import then compared was a path with itself — a re-push, 204,
                // nothing logged on either side.
                SegmentImportOutcome outcome;
                try { outcome = storage.ImportSegment(tmpPath, filePath); }
                catch (Exception ex)
                {
                    try { File.Delete(tmpPath); } catch { /* ignore */ }
                    return Results.Problem($"Failed to place segment file: {ex.Message}");
                }

                if (outcome == SegmentImportOutcome.Registered) return Results.NoContent();

                // Refused, so the body never left the temp name and the file under filePath — if
                // there is one — belongs to somebody else. Unlinking what we wrote is not
                // tidiness: the catalog is rebuilt from this directory on every start, and a
                // *.seg left behind here would be a second file under an occupied key, which the
                // boot scan can only refuse in its turn.
                try { File.Delete(tmpPath); } catch { /* swept as *.seg.tmp by the next boot scan */ }

                // 409 rather than a log line nobody reads: both conflict outcomes mean the
                // sender and some other node are configured as the same NodeId, but they rest
                // on different evidence, and the sender's log quotes this body -- so the body
                // names the case, or the sender confidently reports the wrong one of two.
                // The cause travels in a HEADER, not only in the body: the sender reads the
                // response with ResponseHeadersRead and frames its log line before it has any
                // body -- and body text is prose for humans, which the sender must not parse.
                // One switch decides the status AND the header, with no discard arm in either
                // column: the first version set the header in a separate guarded switch whose
                // discard silently labelled any future conflict "unreadable-incumbent" -- the
                // sender would then log a Warning about a local file for a cause nobody had
                // classified, which is the exact substitution the header was added to end.
                // No discard arm, and CS8509 is promoted to an ERROR in this project's csproj:
                // a new SegmentImportOutcome member without a wire mapping now fails the BUILD,
                // which is what "a compile-time question" has to mean -- as a bare warning it
                // scrolled past, and the runtime answer was a SwitchExpressionException out of
                // the endpoint delegate: a bodyless 500, strictly worse than the 400 the old
                // discard arm gave. CS8524 (an unnamed value cast into the enum) is disabled
                // for the expression: outcome comes from ImportSegment, which returns members.
#pragma warning disable CS8524
                var (cause, result) = outcome switch
                {
                    SegmentImportOutcome.ConflictDifferentSegment => ("different-segment", Results.Problem(
                        $"Segment {nodeId}-{segmentId} is already held by a different file on this node; " +
                        $"two nodes appear to be configured with NodeId {nodeId}.",
                        statusCode: StatusCodes.Status409Conflict)),
                    SegmentImportOutcome.ConflictAllocatedLocally => ("allocated-locally", Results.Problem(
                        $"Segment id {segmentId} falls inside the span this node's own allocator has " +
                        $"already handed out under NodeId {nodeId}: a local flush, merge or WAL replay " +
                        $"holds it or is about to publish under it. Two nodes appear to be configured " +
                        $"with NodeId {nodeId}.",
                        statusCode: StatusCodes.Status409Conflict)),
                    SegmentImportOutcome.ConflictUnreadableIncumbent => ("unreadable-incumbent", Results.Problem(
                        $"The path for segment {nodeId}-{segmentId} is occupied by a file this node " +
                        $"could not read; it was kept and nothing was registered. This says nothing " +
                        $"about NodeId configuration — remove or repair the file on the receiver and " +
                        $"the push can succeed.",
                        statusCode: StatusCodes.Status409Conflict)),
                    SegmentImportOutcome.Unreadable => ((string?)null, Results.Problem(
                        $"Segment {nodeId}-{segmentId} could not be read as a segment file.",
                        statusCode: StatusCodes.Status400BadRequest)),
                    SegmentImportOutcome.Registered => throw new System.Diagnostics.UnreachableException(
                        "Registered returns 204 above."),
                };
                if (cause is not null)
                    request.HttpContext.Response.Headers["X-Ameto-Conflict"] = cause;
                return result;
#pragma warning restore CS8524
            });
    }

    /// <summary>
    /// Where one request stages its body: <c>{nodeId}-{segmentId}.{nonce}.seg.tmp</c>. The route
    /// part is kept for a human reading the directory after a crash; the nonce is what makes the
    /// name the REQUEST's rather than the route's, so concurrent pushes to one segment cannot
    /// truncate, rename or delete each other's body.
    ///
    /// <para>Still ending in <c>.seg.tmp</c>, and deliberately: that is the pattern the boot
    /// scan's sweep uses to collect bodies left behind by a process that died mid-push, and it is
    /// also what keeps these files out of the <c>*.seg</c> enumeration that rebuilds the catalog.
    /// Eight hex digits, as in <c>MetricWriter</c> — the names only have to be distinct among the
    /// handful of pushes staged in one directory at one moment, and a shorter name is one a human
    /// can still read.</para>
    /// </summary>
    private static string StagingPath(string segDir, uint nodeId, ulong segmentId)
    {
        // The nonce is formatted into the stack rather than taken through Guid.ToString("N") and
        // a Substring, which would allocate two throwaway strings on the way to the two this
        // builds (the name, then the combined path).
        Span<char> nonce = stackalloc char[32];
        Guid.NewGuid().TryFormat(nonce, out _, "N");
        return Path.Combine(segDir, $"{nodeId}-{segmentId}.{nonce[..8]}.seg.tmp");
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
