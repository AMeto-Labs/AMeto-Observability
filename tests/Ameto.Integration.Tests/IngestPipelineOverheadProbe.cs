using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.IdentityModel.Tokens;
using System.Diagnostics;
using System.Net.Http.Headers;
using Xunit.Abstractions;

namespace Ameto.Integration.Tests;

/// <summary>
/// Per-request cost of the middleware stack on the ingest hot path, measured through the real
/// pipeline (TestServer, in-process, so the number is pipeline work and not sockets).
///
/// <para>The body is an empty CLEF batch, so the measurement is dominated by everything that
/// happens AROUND the parse — which is the point: this is the probe for the auth-bypass change,
/// and it is what a Serilog sink pays 100 times a second at 100k events/s in 1000-event
/// batches.</para>
/// </summary>
public sealed class IngestPipelineOverheadProbe : IClassFixture<AmetoWebAppFactory>
{
    private readonly AmetoWebAppFactory _factory;
    private readonly ITestOutputHelper  _out;

    public IngestPipelineOverheadProbe(AmetoWebAppFactory factory, ITestOutputHelper o)
    {
        _factory = factory;
        _out     = o;
    }

    [Fact]
    public async Task IngestPostRoundTrip()
    {
        var client = _factory.CreateClient();

        const int warmup = 200;
        const int iters  = 2000;
        const int rounds = 3;

        for (int i = 0; i < warmup; i++)
            (await client.PostAsync("/api/events", Batch())).Dispose();

        double best = double.MaxValue;
        var sw = new Stopwatch();
        for (int r = 0; r < rounds; r++)
        {
            sw.Restart();
            for (int i = 0; i < iters; i++)
                (await client.PostAsync("/api/events", Batch())).Dispose();
            sw.Stop();
            best = Math.Min(best, sw.Elapsed.TotalMicroseconds / iters);
        }

        _out.WriteLine($"POST /api/events (empty batch, through the whole pipeline): {best:F2} µs/request");
    }

    /// <summary>
    /// What the bypass actually removes from an ingest request: one full pass of the JwtBearer
    /// handler that ends in NoResult. Measured in isolation, because it cannot be seen through
    /// the TestServer round trip above — the harness swaps the default scheme for a trivial
    /// test handler, so in-harness there is nothing to save, and the round trip's own overhead
    /// (~130 µs) would swamp it anyway.
    /// </summary>
    [Fact]
    public async Task JwtBearerAuthenticate_CostPerRequest()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddAuthentication(JwtBearerDefaults.AuthenticationScheme)
                .AddJwtBearer(static o =>
                {
                    o.RequireHttpsMetadata = false;
                    o.TokenValidationParameters = new TokenValidationParameters
                    {
                        ValidateIssuer           = false,
                        ValidateAudience         = false,
                        ValidateLifetime         = false,
                        ValidateIssuerSigningKey = false,
                    };
                });
        await using var sp = services.BuildServiceProvider();

        // An ingest request: no Authorization header, so the handler runs and returns NoResult.
        static DefaultHttpContext NewCtx(IServiceProvider sp)
        {
            var ctx = new DefaultHttpContext { RequestServices = sp };
            ctx.Request.Method = "POST";
            ctx.Request.Path   = "/api/events";
            return ctx;
        }

        for (int i = 0; i < 500; i++)
            await NewCtx(sp).AuthenticateAsync(JwtBearerDefaults.AuthenticationScheme);

        const int iters  = 20_000;
        const int rounds = 3;

        long b0 = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < iters; i++)
            await NewCtx(sp).AuthenticateAsync(JwtBearerDefaults.AuthenticationScheme);
        long bytes = (GC.GetAllocatedBytesForCurrentThread() - b0) / iters;

        // Subtract the cost of the bare HttpContext the loop builds, so the figure is the
        // handler's own work.
        long c0 = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < iters; i++) { var c = NewCtx(sp); GC.KeepAlive(c); }
        long ctxBytes = (GC.GetAllocatedBytesForCurrentThread() - c0) / iters;

        double best = double.MaxValue, ctxNs = double.MaxValue;
        var sw = new Stopwatch();
        for (int r = 0; r < rounds; r++)
        {
            sw.Restart();
            for (int i = 0; i < iters; i++)
                await NewCtx(sp).AuthenticateAsync(JwtBearerDefaults.AuthenticationScheme);
            sw.Stop();
            best = Math.Min(best, sw.Elapsed.TotalNanoseconds / iters);

            sw.Restart();
            for (int i = 0; i < iters; i++) { var c = NewCtx(sp); GC.KeepAlive(c); }
            sw.Stop();
            ctxNs = Math.Min(ctxNs, sw.Elapsed.TotalNanoseconds / iters);
        }

        _out.WriteLine(
            $"JwtBearer AuthenticateAsync on a keyless ingest request (the work the bypass removes):\n" +
            $"  with handler: {best,8:F0} ns   {bytes,6:N0} B\n" +
            $"  bare context: {ctxNs,8:F0} ns   {ctxBytes,6:N0} B\n" +
            $"  handler cost: {best - ctxNs,8:F0} ns   {bytes - ctxBytes,6:N0} B per request");
    }

    /// <summary>
    /// The two ways of producing the <c>{"ingested":N,"dropped":M}</c> reply, per request: the
    /// interpolated string the endpoint used to hand to WriteAsync(string), and the
    /// Utf8Formatter writes it now makes straight into the response buffer. The production
    /// writer is private, so the shapes are reproduced here — the point is the technique, and
    /// IngestResponseBodyTests pins that the real endpoint emits the same bytes.
    /// </summary>
    [Fact]
    public void IngestReplyFormatting_CostPerRequest()
    {
        Span<byte> dest = stackalloc byte[64];

        for (int i = 0; i < 1000; i++) { ViaString(dest, i, 0); ViaFormatter(dest, i, 0); }

        const int iters  = 200_000;
        const int rounds = 3;

        long b0 = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < iters; i++) ViaString(dest, i, 0);
        double strBytes = (GC.GetAllocatedBytesForCurrentThread() - b0) / (double)iters;

        long b1 = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < iters; i++) ViaFormatter(dest, i, 0);
        double fmtBytes = (GC.GetAllocatedBytesForCurrentThread() - b1) / (double)iters;

        double strNs = double.MaxValue, fmtNs = double.MaxValue;
        var sw = new Stopwatch();
        for (int r = 0; r < rounds; r++)
        {
            sw.Restart();
            for (int i = 0; i < iters; i++) ViaString(dest, i, 0);
            sw.Stop();
            strNs = Math.Min(strNs, sw.Elapsed.TotalNanoseconds / iters);

            sw.Restart();
            for (int i = 0; i < iters; i++) ViaFormatter(dest, i, 0);
            sw.Stop();
            fmtNs = Math.Min(fmtNs, sw.Elapsed.TotalNanoseconds / iters);
        }

        _out.WriteLine(
            $"/api/events reply, one per request:\n" +
            $"  interpolated string + transcode (before): {strBytes,5:F1} B  {strNs,5:F0} ns\n" +
            $"  Utf8Formatter into the buffer   (after) : {fmtBytes,5:F1} B  {fmtNs,5:F0} ns");

        Assert.Equal(0, fmtBytes);
        Assert.True(strBytes > 0);
    }

    private static int ViaString(Span<byte> dest, int ingested, int dropped)
    {
        string s = $"{{\"ingested\":{ingested},\"dropped\":{dropped}}}";
        return System.Text.Encoding.UTF8.GetBytes(s, dest);
    }

    private static int ViaFormatter(Span<byte> dest, int ingested, int dropped)
    {
        int pos = 0;
        "{\"ingested\":"u8.CopyTo(dest);
        pos += 12;
        System.Buffers.Text.Utf8Formatter.TryFormat(ingested, dest[pos..], out int written);
        pos += written;
        ",\"dropped\":"u8.CopyTo(dest[pos..]);
        pos += 11;
        System.Buffers.Text.Utf8Formatter.TryFormat(dropped, dest[pos..], out written);
        pos += written;
        dest[pos++] = (byte)'}';
        return pos;
    }

    private static ByteArrayContent Batch()
    {
        var content = new ByteArrayContent([0x90]); // msgpack: empty array
        content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
        return content;
    }
}
