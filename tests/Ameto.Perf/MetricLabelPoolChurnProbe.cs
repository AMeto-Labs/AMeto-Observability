using System.Diagnostics;
using System.Globalization;
using Ameto.Metrics;
using Ameto.Otel;
using Google.Protobuf;
using Xunit;
using Xunit.Abstractions;

namespace Ameto.Perf;

/// <summary>
/// Issue #88, MEASURED: how fast a cluster whose pods churn fills the metric label pool, and what
/// ingest costs per point once it is full.
///
/// <para>The workload is a Kubernetes cluster as the OTel operator's resource detectors describe it:
/// <see cref="Services"/> deployments of <see cref="Replicas"/> pods, every pod exporting
/// <see cref="Instruments"/> instruments x <see cref="SeriesEach"/> series of HTTP-shaped points
/// under a resource that carries the pod's own identity — <c>k8s.pod.name</c>, <c>k8s.pod.uid</c>,
/// <c>container.id</c>, <c>k8s.replicaset.name</c> — beside the stable service, namespace and node
/// labels (and the <c>service.instance.id</c> / <c>telemetry.*</c> the parser drops). A rollout
/// replaces one deployment's pods; the probe rolls deployments round-robin until the pool is full.</para>
///
/// <para>Through the protobuf parser and a private interner of the production size — the same class
/// and bounds as <see cref="MetricLabelInterner.Shared"/>, which a probe must not fill for the rest
/// of the assembly.</para>
/// </summary>
public sealed class MetricLabelPoolChurnProbe
{
    private const int Services    = 50;
    private const int Replicas    = 3;
    private const int Instruments = 10;
    private const int SeriesEach  = 5;
    private const int PointsPerBatch = Instruments * SeriesEach;

    private readonly ITestOutputHelper _out;
    public MetricLabelPoolChurnProbe(ITestOutputHelper o) => _out = o;

    [Fact]
    public void Probe_a_churning_cluster_fills_the_pool_and_what_a_point_costs_after()
    {
        var clock    = new Ameto.Testing.ManualTimeProvider();
        var interner = new MetricLabelInterner(MetricLabelInterner.DefaultMaxStrings,
                                               MetricLabelInterner.DefaultLabelSetSlots, clock);
        int cap = interner.Strings.MaxPoolSize;

        // The cluster at rest: every pod of generation 0 exports once.
        for (int s = 0; s < Services; s++)
            for (int r = 0; r < Replicas; r++)
                OtlpMetricProtoParser.Parse(PodBatch(s, 0, r), interner);
        int atRest = interner.Strings.ClaimedCount;

        // Rollouts, round-robin over the deployments, until the pool stops pooling.
        int rollouts = 0, lastGeneration = 0;
        while (interner.Strings.ClaimedCount < cap)
        {
            int s = rollouts % Services;
            lastGeneration = 1 + rollouts / Services;
            for (int r = 0; r < Replicas; r++)
                OtlpMetricProtoParser.Parse(PodBatch(s, lastGeneration, r), interner);
            rollouts++;
        }
        double perPod = (double)(interner.Strings.ClaimedCount - atRest) / (rollouts * Replicas);

        // What a point costs: a pod the pool knows (generation 0 of the last deployment), and a pod born
        // after the pool filled — every one of its identity strings, every export, is a fresh string, and
        // its label sets can no longer be cached.
        byte[] pooledPod = PodBatch(Services - 1, 0, 0);
        byte[] bornLate  = PodBatch(0, lastGeneration + 1, 0);
        var pooled = Best(() => OtlpMetricProtoParser.Parse(pooledPod, interner));
        var late   = Best(() => OtlpMetricProtoParser.Parse(bornLate,  interner));
        Assert.Equal(cap, interner.Strings.ClaimedCount);
        Assert.Equal(0, interner.Resets);                              // all of the above inside one interval

        // The reset: an hour on, the next miss on the full pool empties it, and live traffic re-interns.
        clock.Advance(MetricLabelInterner.ResetInterval);
        OtlpMetricProtoParser.Parse(bornLate, interner);
        int resets = interner.Resets;
        var lateAfterReset   = Best(() => OtlpMetricProtoParser.Parse(bornLate,  interner));
        var pooledAfterReset = Best(() => OtlpMetricProtoParser.Parse(pooledPod, interner));

        _out.WriteLine($"CLUSTER  {Services} deployments x {Replicas} pods, {PointsPerBatch} points per pod export; pool cap {cap:N0} strings");
        _out.WriteLine($"  at rest (generation 0)        : {atRest,6:N0} strings pooled");
        _out.WriteLine($"  per replaced pod              : {perPod,6:N1} new strings");
        _out.WriteLine($"  full after                    : {rollouts,6:N0} rollouts = {rollouts * Replicas:N0} replaced pods "
                     + $"(~{rollouts / (double)Services:N0} rollouts per deployment)");
        _out.WriteLine($"  point of a pooled pod         : {pooled.Ns,6:N0} ns | {pooled.Bytes,6:N0} B");
        _out.WriteLine($"  point of a pod born after full: {late.Ns,6:N0} ns | {late.Bytes,6:N0} B   ({late.Bytes / pooled.Bytes:N1}x)");
        _out.WriteLine($"  RESET ({resets}) after {MetricLabelInterner.ResetInterval.TotalMinutes:N0} min; pool now {interner.Strings.ClaimedCount:N0} strings");
        _out.WriteLine($"  point of that late pod        : {lateAfterReset.Ns,6:N0} ns | {lateAfterReset.Bytes,6:N0} B");
        _out.WriteLine($"  point of the old pooled pod   : {pooledAfterReset.Ns,6:N0} ns | {pooledAfterReset.Bytes,6:N0} B");

        Assert.True(late.Bytes > pooled.Bytes, $"late {late.Bytes} B/point, pooled {pooled.Bytes}");
        Assert.Equal(1, resets);
        Assert.True(lateAfterReset.Bytes < late.Bytes, $"after the reset {lateAfterReset.Bytes} B/point, before {late.Bytes}");
    }

    // ── Measurement ───────────────────────────────────────────────────────────

    /// <summary>Per point, best of five runs of 200 exports; per-thread bytes (the parse runs on this thread).</summary>
    private static (double Ns, double Bytes) Best(Action export)
    {
        for (int i = 0; i < 20; i++) export();                        // warm JIT and the caches
        double ns = double.MaxValue, bytes = double.MaxValue;
        for (int run = 0; run < 5; run++)
        {
            const int iters = 200;
            long b0 = GC.GetAllocatedBytesForCurrentThread();
            long t0 = Stopwatch.GetTimestamp();
            for (int i = 0; i < iters; i++) export();
            double elapsedNs = Stopwatch.GetElapsedTime(t0).TotalNanoseconds;
            long   allocated = GC.GetAllocatedBytesForCurrentThread() - b0;
            ns    = Math.Min(ns,    elapsedNs / (iters * PointsPerBatch));
            bytes = Math.Min(bytes, (double)allocated / (iters * PointsPerBatch));
        }
        return (ns, bytes);
    }

    // ── The workload ──────────────────────────────────────────────────────────

    internal static byte[] PodBatch(int service, int generation, int replica)
    {
        string svc     = "svc-" + service.ToString("D2", CultureInfo.InvariantCulture);
        string rs      = svc + "-" + Hex(service * 7919 + generation * 104_729, 10);
        string pod     = rs + "-" + Hex(service * 31 + generation * 131 + replica * 17, 5);
        string uid     = Guid(service, generation, replica);
        string node    = "node-" + ((service + replica) % 12).ToString(CultureInfo.InvariantCulture);
        string cid     = Hex(service * 1_000_003 + generation * 7_919 + replica, 64);

        byte[] resource = OtlpProtoPayloads.Msg(res =>
        {
            Attr(res, "service.name", svc);
            Attr(res, "service.namespace", "shop");
            Attr(res, "service.instance.id", uid);                     // dropped by the parser
            Attr(res, "telemetry.sdk.name", "opentelemetry");          // dropped by the parser
            Attr(res, "k8s.namespace.name", "prod");
            Attr(res, "k8s.deployment.name", svc);
            Attr(res, "k8s.replicaset.name", rs);
            Attr(res, "k8s.pod.name", pod);
            Attr(res, "k8s.pod.uid", uid);
            Attr(res, "k8s.node.name", node);
            Attr(res, "host.name", node);
            Attr(res, "container.id", cid);
        });

        byte[] scope = OtlpProtoPayloads.Msg(sm =>
        {
            for (int m = 0; m < Instruments; m++)
            {
                int mi = m;
                OtlpProtoPayloads.Nested(sm, 2, OtlpProtoPayloads.Msg(metric =>
                {
                    metric.WriteTag(1, WireFormat.WireType.LengthDelimited);
                    metric.WriteString("http.server.metric." + mi.ToString(CultureInfo.InvariantCulture));
                    OtlpProtoPayloads.Nested(metric, 7, OtlpProtoPayloads.Msg(sum =>   // sum
                    {
                        for (int p = 0; p < SeriesEach; p++)
                        {
                            int pi = p;
                            OtlpProtoPayloads.Nested(sum, 1, OtlpProtoPayloads.Msg(dp =>
                            {
                                dp.WriteTag(3, WireFormat.WireType.Fixed64); dp.WriteFixed64(1_785_300_060_000_000_000UL);
                                dp.WriteTag(6, WireFormat.WireType.Fixed64); dp.WriteSFixed64(1_000 + pi);
                                Attr(dp, 7, "http.route", "/api/v1/r" + pi.ToString(CultureInfo.InvariantCulture));
                                Attr(dp, 7, "http.request.method", pi % 2 == 0 ? "GET" : "POST");
                                Attr(dp, 7, "http.response.status_code", pi % 5 == 0 ? "500" : "200");
                            }));
                        }
                        sum.WriteTag(3, WireFormat.WireType.Varint); sum.WriteBool(true);
                    }));
                }));
            }
        });

        return OtlpProtoPayloads.Msg(req => OtlpProtoPayloads.Nested(req, 1, OtlpProtoPayloads.Msg(rm =>
        {
            OtlpProtoPayloads.Nested(rm, 1, resource);
            OtlpProtoPayloads.Nested(rm, 2, scope);
        })));
    }

    private static void Attr(CodedOutputStream c, string key, string value) => Attr(c, 1, key, value);

    private static void Attr(CodedOutputStream c, int field, string key, string value) =>
        OtlpProtoPayloads.Nested(c, field, OtlpProtoPayloads.Msg(kv =>
        {
            kv.WriteTag(1, WireFormat.WireType.LengthDelimited); kv.WriteString(key);
            OtlpProtoPayloads.Nested(kv, 2, OtlpProtoPayloads.Msg(v =>
            {
                v.WriteTag(1, WireFormat.WireType.LengthDelimited); v.WriteString(value);
            }));
        }));

    /// <summary><paramref name="length"/> lowercase hex digits derived from <paramref name="seed"/> — deterministic, distinct per seed.</summary>
    private static string Hex(long seed, int length)
    {
        var chars = new char[length];
        ulong x = (ulong)seed * 0x9E3779B97F4A7C15UL + 0x632BE59BD9B4E019UL;
        for (int i = 0; i < length; i++)
        {
            x ^= x >> 29; x *= 0xBF58476D1CE4E5B9UL; x ^= x >> 32;
            chars[i] = "0123456789abcdef"[(int)(x & 15)];
        }
        return new string(chars);
    }

    private static string Guid(int service, int generation, int replica)
    {
        string h = Hex(service * 1_000_000_007L + generation * 1_009L + replica, 32);
        return $"{h[..8]}-{h[8..12]}-{h[12..16]}-{h[16..20]}-{h[20..]}";
    }
}
