import { ComponentFixture, TestBed } from '@angular/core/testing';
import { provideZonelessChangeDetection, signal } from '@angular/core';
import { HttpErrorResponse } from '@angular/common/http';
import { Router, provideRouter } from '@angular/router';
import { LUCIDE_ICONS, LucideIconProvider, icons } from 'lucide-angular';
import { of, throwError } from 'rxjs';
import { describe, it, expect, afterEach, vi } from 'vitest';

import { StatsComponent } from './stats';
import { ApiService } from '../../core/services/api.service';
import { ViewportService } from '../../core/services/viewport.service';
import { EventCountsDto, StatsDto } from '../../core/models/event.model';
import { LatencyServiceDto, TraceStatsDto } from '../../core/models/span.model';
import { MetricStatsDto } from '../../core/models/metric.model';

/**
 * The Statistics page over stubbed aggregates: three bands from three stores, one window for all
 * of them, each store's refusal its own band's state, and a phone's tabs and sheet in place of the
 * stacked bands and the popover.
 */

/** 2026-11-06 14:22:30 local. */
const NOW = new Date(2026, 10, 6, 14, 22, 30);
const STEP = 900_000;
const GRID = Math.floor((NOW.getTime() - 86_400_000) / STEP) * STEP;
const BUCKETS = [GRID, GRID + STEP, GRID + 2 * STEP];

const counts = (total: number): EventCountsDto => ({
  from: new Date(GRID).toISOString(), to: NOW.toISOString(), bucketSeconds: 900, total, sampled: total, truncated: false,
  buckets: BUCKETS,
  services: total ? [
    { service: 'gateway', count: 300, points: [100, 100, 100] },
    { service: 'orders', count: 60, points: [20, 20, 20] },
  ] : [],
  levels: total ? [
    { level: 'Information', count: 340, points: [115, 115, 110] },
    { level: 'Error', count: 20, points: [5, 5, 10] },
  ] : [],
});

const stats: StatsDto = {
  segments: 186, totalEvents: 41_200_000, compressedBytes: 3.1 * 1024 ** 3, uncompressedBytes: 30 * 1024 ** 3,
  oldestTimestamp: '2026-09-29T06:12:44.0000000+00:00', newestTimestamp: NOW.toISOString(),
};

const traceStats: TraceStatsDto = {
  totalTraces: 48_200, errorRate: 1.8, p50LatencyMs: 46, p95LatencyMs: 812, p99LatencyMs: 2100, throughputRps: 13.4,
  totalSparkline: [10, 20, 30], errorSparkline: [1, 0, 2],
  from: new Date(GRID).toISOString(), to: new Date(GRID + 3 * STEP).toISOString(),
  maxLatencyMs: 12_400, spanCount: 6_800_000, serviceCount: 12,
  histogram: { boundsMs: [1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10_000, 30_000, 60_000, 120_000, 300_000, 600_000, 1_800_000],
               counts: [4, 9, 18, 31, 52, 74, 92, 100, 88, 64, 41, 27, 18, 12, 9, 7, 5, 4, 3] },
  store: { spans: 6_800_000, hotSpans: 5000, segments: 94, fileBytes: 1.1 * 1024 ** 3, services: 12,
           oldestStartMs: Date.UTC(2026, 9, 2), newestStartMs: NOW.getTime() },
};

const latency: LatencyServiceDto[] = [
  { service: 'gateway', spanCount: 2_100_000, errorCount: 25_200, p50Ms: 20, p95Ms: 184, p99Ms: 500, p999Ms: 900, buckets: [] },
  { service: 'payments-api', spanCount: 1_400_000, errorCount: 57_400, p50Ms: 50, p95Ms: 812, p99Ms: 2100, p999Ms: 5000, buckets: [] },
];

const metricStats: MetricStatsDto = {
  from: new Date(GRID).toISOString(), to: NOW.toISOString(), bucketSeconds: 900, buckets: BUCKETS,
  window: { metrics: 142, bytesWritten: 412e6, peakBucketBytes: 26e6, rawBytes: 292e6, rollupBytes: 120e6 },
  store: { metrics: 148, series: 8400, segments: 1204, segmentsRaw: 812, segments5m: 290, segments1h: 102,
           bytes: 2.4 * 1024 ** 3, bytesRaw: 1.7 * 1024 ** 3, bytes5m: 520 * 1024 ** 2, bytes1h: 180 * 1024 ** 2,
           hotSeries: 900, hotPoints: 1_200_000, hotBytes: 76e6, oldestMs: Date.UTC(2026, 7, 6), newestMs: NOW.getTime() },
  kinds: [{ kind: 'Counter', metrics: 61, series: 3900 }, { kind: 'Gauge', metrics: 72, series: 3200 }, { kind: 'Histogram', metrics: 15, series: 1300 }],
  byMetric: [{ name: 'http_server_request_duration_seconds', kind: 'Histogram', unit: 's', series: 1900, bytes: 96e6, points: [30e6, 30e6, 36e6] }],
  otherBytes: 316e6, otherPoints: [100e6, 100e6, 116e6],
};

const sentence = 'The metric store has shut down and cannot answer. Metrics are available again once the server has restarted.';
const refused = () => throwError(() => new HttpErrorResponse({ status: 503, error: { error: sentence } }));

type ApiStub = Pick<ApiService, 'getEventCounts' | 'getStats' | 'getTraceStats' | 'getLatency' | 'getMetricStats'>;

function stubApi(over: Partial<ApiStub> = {}): ApiStub {
  return {
    getEventCounts: vi.fn(() => of(counts(360))),
    getStats: vi.fn(() => of(stats)),
    getTraceStats: vi.fn(() => of(traceStats)),
    getLatency: vi.fn(() => of(latency)),
    getMetricStats: vi.fn(() => of(metricStats)),
    ...over,
  };
}

/** The ViewportService as a phone or a desktop with a mouse. */
const viewport = (phone: boolean) => ({ phone: signal(phone), tablet: signal(false), coarse: signal(phone) });

async function mount(api: ApiStub, phone = false): Promise<{ fixture: ComponentFixture<StatsComponent>; el: HTMLElement; router: Router }> {
  vi.useFakeTimers({ toFake: ['Date'] });
  vi.setSystemTime(NOW);
  TestBed.configureTestingModule({
    imports: [StatsComponent],
    providers: [
      provideZonelessChangeDetection(),
      provideRouter([]),
      { provide: ApiService, useValue: api },
      { provide: ViewportService, useValue: viewport(phone) },
      { provide: LUCIDE_ICONS, multi: true, useValue: new LucideIconProvider(icons) },
    ],
  });
  const router = TestBed.inject(Router);
  vi.spyOn(router, 'navigate').mockResolvedValue(true);
  const fixture = TestBed.createComponent(StatsComponent);
  await fixture.whenStable();
  fixture.detectChanges();
  return { fixture, el: fixture.nativeElement as HTMLElement, router };
}

const text = (el: HTMLElement) => el.textContent ?? '';

async function click(fixture: ComponentFixture<StatsComponent>, target: Element | null): Promise<void> {
  expect(target).not.toBeNull();
  (target as HTMLElement).click();
  await fixture.whenStable();
  fixture.detectChanges();
}

afterEach(() => {
  TestBed.resetTestingModule();
  vi.useRealTimers();
  vi.restoreAllMocks();
});

describe('Statistics — the three bands', () => {
  it('draws logs, metrics and traces from the aggregates, each with its window and store tiles', async () => {
    const api = stubApi();
    const { el } = await mount(api);
    const body = text(el);

    expect(el.querySelectorAll('app-stats-section').length).toBe(3);
    // Logs: the window's events and error share; the store's events, segments, bytes and oldest event.
    expect(body).toContain('360');
    expect(body).toContain('5.6 %');
    expect(body).toContain('41.2M');
    expect(body).toContain('186');
    expect(body).toContain('3.1 GB');
    expect(body).toContain('29 Sep');
    // Metrics: the window's metrics and bytes; the store's series and hot tier.
    expect(body).toContain('142');
    expect(body).toContain('393 MB');
    expect(body).toContain('8.4K');
    expect(body).toContain('1.2M pts');
    // Traces: the window's figures and the store's.
    expect(body).toContain('48.2K');
    expect(body).toContain('1.8 %');
    expect(body).toContain('812 ms');
    expect(body).toContain('13.4 rps');
    expect(body).toContain('6.8M');

    // One window, three requests, one grid: the counts' bucket and the traces' column count agree.
    const countsArgs = (api.getEventCounts as ReturnType<typeof vi.fn>).mock.calls[0][0] as { from: string; to?: string; bucket: number };
    expect(countsArgs.bucket).toBe(900);
    expect(countsArgs.to).toBeUndefined();
    expect(Date.parse(countsArgs.from) % STEP).toBe(0);
    const traceArgs = (api.getTraceStats as ReturnType<typeof vi.fn>).mock.calls[0] as [string, string, number];
    expect(traceArgs[2]).toBe(97);
    expect(Date.parse(traceArgs[0])).toBe(Date.parse(countsArgs.from));
    const metricArgs = (api.getMetricStats as ReturnType<typeof vi.fn>).mock.calls[0][0] as { bucket: number };
    expect(metricArgs.bucket).toBe(900);

    // "Open Logs" carries the same window in the Logs page's words.
    const open = el.querySelector('a.sh-open') as HTMLAnchorElement;
    expect(open.getAttribute('href')).toBe('/events?preset=1d');
  });

  it("keeps the other two bands standing when one store refuses, and shows that store's own sentence", async () => {
    const { el } = await mount(stubApi({ getMetricStats: vi.fn(refused) }));
    const body = text(el);

    expect(body).toContain('Unavailable');
    expect(body).toContain(sentence);
    expect(body).toContain('41.2M');
    expect(body).toContain('48.2K');
    expect(el.querySelectorAll('.sh-tag').length).toBe(1);
  });

  it('says so when the window holds nothing, with the store\'s oldest record as the hint', async () => {
    const { el } = await mount(stubApi({ getEventCounts: vi.fn(() => of(counts(0))) }));
    const body = text(el);
    expect(body).toContain('No events in this window');
    expect(body).toContain("The store's oldest event is from 29 Sep.");
  });

  it('re-resolves every band against a new quick range and remembers it in the URL', async () => {
    const api = stubApi();
    const { fixture, el, router } = await mount(api);

    const quarter = [...el.querySelectorAll<HTMLButtonElement>('.seg-btn')].find(b => b.textContent?.trim() === '3m');
    await click(fixture, quarter ?? null);

    expect(quarter?.getAttribute('aria-pressed')).toBe('true');
    const last = (api.getEventCounts as ReturnType<typeof vi.fn>).mock.calls.at(-1)![0] as { bucket: number };
    expect(last.bucket).toBe(86_400);
    expect(router.navigate).toHaveBeenCalledWith([], expect.objectContaining({
      queryParams: expect.objectContaining({ preset: '3m' }),
      replaceUrl: true,
    }));
    expect(text(el)).toContain('buckets of 1 day');
  });

  it('opens a bar as its bucket on the Logs page', async () => {
    const { fixture, el, router } = await mount(stubApi());
    await click(fixture, el.querySelector('app-stats-logs button.bar'));

    expect(router.navigate).toHaveBeenCalledWith(['/events'], {
      queryParams: { preset: 'custom', from: expect.stringMatching(/^2026-11-05 /), to: expect.stringMatching(/^2026-11-05 /) },
    });
  });
});

describe('Statistics — on a phone', () => {
  it('shows one band behind three tabs, and the range sheet from the header button', async () => {
    const { fixture, el } = await mount(stubApi(), true);

    expect(el.querySelector('app-stats-logs')).not.toBeNull();
    expect(el.querySelector('app-stats-metrics')).toBeNull();
    expect(el.querySelector('app-page-header')).toBeNull();
    expect(el.querySelectorAll('.ph-tab').length).toBe(3);

    await click(fixture, el.querySelectorAll('.ph-tab')[1]);
    expect(el.querySelector('app-stats-logs')).toBeNull();
    expect(el.querySelector('app-stats-metrics')).not.toBeNull();

    await click(fixture, el.querySelector('.ph-range'));
    const sheet = el.querySelector('app-stats-range-sheet');
    expect(sheet).not.toBeNull();

    const chip = [...sheet!.querySelectorAll<HTMLButtonElement>('.bs-chip')].find(c => c.textContent?.trim() === '7d');
    await click(fixture, chip ?? null);
    await click(fixture, sheet!.querySelector('.bs-foot-apply'));

    expect(el.querySelector('app-stats-range-sheet')).toBeNull();
    expect(fixture.componentInstance.store.preset()).toBe('7d');
    expect(text(el)).toContain('buckets of 2 h');
  });
});
