import {
  Component, input, signal, computed, inject, OnChanges, SimpleChanges,
  ChangeDetectionStrategy, ChangeDetectorRef,
} from '@angular/core';
import { FormsModule } from '@angular/forms';
import { LucideAngularModule } from 'lucide-angular';
import { ApiService } from '../../../core/services/api.service';
import { LatencyServiceDto, LatencyBucketDto } from '../../../core/models/span.model';
import { serviceColor } from '../../../shared/utils/service-color';

@Component({
  selector: 'app-latency',
  standalone: true,
  imports: [FormsModule, LucideAngularModule],
  changeDetection: ChangeDetectionStrategy.OnPush,
  // The `data-label`s are read by the phone stylesheet, where the table becomes one card per
  // service and every number needs its own caption; the header row that carried them is gone.
  template: `
    <div class="lat-root">

      <div class="lat-toolbar">
        <span class="lat-title">Latency Distribution</span>
        <div class="lat-filter">
          <lucide-icon name="search" [size]="12" class="lat-filter-icon" />
          <input class="lat-filter-input" [(ngModel)]="serviceFilter"
                 placeholder="Filter service…" (input)="onFilterChange()" />
        </div>
        <button class="lat-btn" (click)="load()">
          <lucide-icon name="refresh-cw" [size]="12" [class.spin]="loading()" />
          Refresh
        </button>
      </div>

      @if (loading() && !rows().length) {
        <div class="lat-empty">
          <lucide-icon name="loader" [size]="18" class="spin" /> Loading…
        </div>
      } @else if (error()) {
        <div class="lat-empty lat-err">{{ error() }}</div>
      } @else if (!filtered().length) {
        <div class="lat-empty">No data</div>
      } @else {

        <div class="lat-head">
          <span class="lat-col-svc">Service</span>
          <span class="lat-col-chart">Distribution</span>
          <span class="lat-col-p">P50</span>
          <span class="lat-col-p">P95</span>
          <span class="lat-col-p">P99</span>
          <span class="lat-col-p">P999</span>
          <span class="lat-col-p">Spans</span>
          <span class="lat-col-p">Errors</span>
        </div>

        <div class="lat-body">
          @for (row of filtered(); track row.service) {
            <div class="lat-row">
              <div class="lat-col-svc">
                <span class="lat-svc-dot" [style.background]="svcColor(row.service)"></span>
                <span class="lat-svc-name">{{ row.service }}</span>
                @if (row.errorCount > 0) {
                  <span class="lat-err-badge">
                    {{ errPct(row) }}% err
                  </span>
                }
              </div>

              <div class="lat-col-chart">
                <svg class="lat-hist" viewBox="0 0 300 36" preserveAspectRatio="none">
                  @let maxCount = maxBucket(row.buckets);
                  @for (b of row.buckets; track $index) {
                    @let bh = maxCount > 0 ? (b.count / maxCount) * 32 : 0;
                    <rect
                      [attr.x]="$index * (300 / row.buckets.length)"
                      [attr.y]="36 - bh"
                      [attr.width]="(300 / row.buckets.length) - 1"
                      [attr.height]="bh"
                      [attr.fill]="barFill(b, row)"
                      rx="1" />
                  }
                  <!-- P50 marker -->
                  @let p50x = pctToX(row.p50Ms, row.buckets);
                  @let p95x = pctToX(row.p95Ms, row.buckets);
                  @let p99x = pctToX(row.p99Ms, row.buckets);
                  <line [attr.x1]="p50x" [attr.x2]="p50x" y1="0" y2="36" stroke="#22c55e" stroke-width="1.5" stroke-dasharray="3,2" />
                  <line [attr.x1]="p95x" [attr.x2]="p95x" y1="0" y2="36" stroke="#f59e0b" stroke-width="1.5" stroke-dasharray="3,2" />
                  <line [attr.x1]="p99x" [attr.x2]="p99x" y1="0" y2="36" stroke="#ef4444" stroke-width="1.5" stroke-dasharray="3,2" />
                </svg>
              </div>

              <span class="lat-col-p p50"  data-label="P50">{{ fmtMs(row.p50Ms) }}</span>
              <span class="lat-col-p p95"  data-label="P95">{{ fmtMs(row.p95Ms) }}</span>
              <span class="lat-col-p p99"  data-label="P99">{{ fmtMs(row.p99Ms) }}</span>
              <span class="lat-col-p p999" data-label="P999">{{ fmtMs(row.p999Ms) }}</span>
              <span class="lat-col-p lat-col-p--spans"  data-label="Spans">{{ fmtCount(row.spanCount) }}</span>
              <span class="lat-col-p lat-col-p--errors" data-label="Errors" [class.red]="row.errorCount > 0">{{ row.errorCount }}</span>
            </div>
          }
        </div>

        <div class="lat-legend">
          <span class="lat-leg-item">
            <svg width="24" height="4"><line x1="0" y1="2" x2="24" y2="2" stroke="#22c55e" stroke-width="1.5" stroke-dasharray="3,2"/></svg>
            P50
          </span>
          <span class="lat-leg-item">
            <svg width="24" height="4"><line x1="0" y1="2" x2="24" y2="2" stroke="#f59e0b" stroke-width="1.5" stroke-dasharray="3,2"/></svg>
            P95
          </span>
          <span class="lat-leg-item">
            <svg width="24" height="4"><line x1="0" y1="2" x2="24" y2="2" stroke="#ef4444" stroke-width="1.5" stroke-dasharray="3,2"/></svg>
            P99
          </span>
        </div>
      }
    </div>
  `,
  // Inline styles cannot @use the shared breakpoints partial, so the tablet and phone queries
  // below spell out the same values: 899px is bp.$bp-md − 1, 639px is bp.$bp-sm − 1.
  styles: [`
    :host { display: flex; flex-direction: column; flex: 1; min-height: 0; }

    .lat-root {
      display: flex;
      flex-direction: column;
      flex: 1;
      min-height: 0;
      overflow: hidden;
      /* The table's columns, in one place for the three widths: the service, the histogram
         taking what is left, then six 56px numbers (design canvas). */
      --lat-cols: 180px minmax(0, 1fr) repeat(6, 56px);
    }

    .lat-toolbar {
      display: flex;
      align-items: center;
      gap: 10px;
      padding: 6px 12px;
      border-bottom: 1px solid var(--border);
      flex-shrink: 0;
    }

    .lat-title {
      font-size: var(--fs-md);
      font-weight: var(--fw-semibold);
      color: var(--text-primary);
      flex: 1;
    }

    .lat-filter {
      position: relative;
      display: flex;
      align-items: center;
    }

    .lat-filter-icon {
      position: absolute;
      left: 8px;
      color: var(--text-muted);
      pointer-events: none;
    }

    .lat-filter-input {
      height: var(--ctl-h);
      width: 180px;
      padding: 0 8px 0 26px;
      background: var(--bg-elevated);
      border: 1px solid var(--border);
      border-radius: var(--radius);
      color: var(--text-primary);
      font-size: var(--fs-sm);
      outline: none;

      &::placeholder { color: var(--text-muted); }
      &:focus { border-color: var(--accent); }
    }

    .lat-btn {
      display: flex;
      align-items: center;
      gap: 5px;
      height: var(--ctl-h);
      padding: 0 10px;
      background: none;
      border: 1px solid var(--border);
      border-radius: var(--radius);
      color: var(--text-secondary);
      font-size: var(--fs-sm);
      cursor: pointer;
      white-space: nowrap;

      &:hover { border-color: var(--accent); color: var(--accent); }
    }

    .lat-empty {
      flex: 1;
      display: flex;
      align-items: center;
      justify-content: center;
      gap: 8px;
      color: var(--text-muted);
      font-size: var(--fs-sm);
    }
    .lat-err { color: var(--error); }

    .lat-head {
      display: grid;
      grid-template-columns: var(--lat-cols);
      gap: 0;
      background: var(--bg-elevated);
      border-bottom: 1px solid var(--border);
      flex-shrink: 0;
      font-size: var(--fs-2xs);
      font-weight: var(--fw-semibold);
      text-transform: uppercase;
      letter-spacing: var(--ls-caps);
      color: var(--text-muted);
      padding: 4px 12px;
    }

    .lat-body {
      flex: 1;
      overflow-y: auto;
    }

    .lat-row {
      display: grid;
      grid-template-columns: var(--lat-cols);
      align-items: center;
      padding: 4px 12px;
      border-bottom: 1px solid var(--border);
      gap: 0;

      &:last-child { border-bottom: none; }
      &:hover { background: var(--bg-elevated); }
    }

    .lat-col-svc {
      display: flex;
      align-items: center;
      gap: 6px;
      overflow: hidden;
      min-width: 0;
    }

    .lat-svc-dot {
      width: 8px;
      height: 8px;
      border-radius: 50%;
      flex-shrink: 0;
    }

    .lat-svc-name {
      font-size: var(--fs-sm);
      color: var(--text-primary);
      font-weight: var(--fw-semibold);
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }

    .lat-err-badge {
      font-size: var(--fs-2xs);
      font-weight: var(--fw-semibold);
      padding: 1px 5px;
      border-radius: 3px;
      background: color-mix(in srgb, var(--error) 15%, transparent);
      color: var(--error);
      flex-shrink: 0;
      white-space: nowrap;
    }

    .lat-col-chart {
      padding: 0 8px;
      min-width: 0;
    }

    .lat-hist {
      width: 100%;
      height: 30px;
      display: block;
    }

    .lat-col-p {
      font-size: var(--fs-xs);
      font-family: var(--font-ui);
      color: var(--text-secondary);
      text-align: right;
      padding-right: 6px;
      white-space: nowrap;

      &.p50  { color: #22c55e; }
      &.p95  { color: #f59e0b; }
      &.p99  { color: #ef4444; }
      &.p999 { color: #a78bfa; }
      &.red  { color: var(--error); }
    }

    .lat-legend {
      display: flex;
      align-items: center;
      gap: 14px;
      padding: 4px 12px;
      border-top: 1px solid var(--border);
      flex-shrink: 0;
      font-size: var(--fs-xs);
      color: var(--text-muted);
    }

    .lat-leg-item {
      display: flex;
      align-items: center;
      gap: 5px;
    }

    @keyframes spin { to { transform: rotate(360deg); } }
    .spin { animation: spin 1s linear infinite; }

    /* Tablet: the same table with less room for the service name and the numbers. */
    @media (max-width: 899px) {
      .lat-root { --lat-cols: 140px minmax(0, 1fr) repeat(6, 52px); }
    }

    /* Phone: one card per service — the name, the histogram, then the six numbers in two rows
       of three, each captioned from its data-label since the header row is gone. */
    @media (max-width: 639px) {
      .lat-toolbar { flex-wrap: wrap; gap: 8px; }
      .lat-title { flex-basis: 100%; }
      .lat-filter { flex: 1; }
      .lat-filter-input { width: 100%; height: 32px; }
      .lat-btn { height: 32px; }

      .lat-head { display: none; }

      .lat-row {
        grid-template-columns: repeat(3, minmax(0, 1fr));
        grid-template-areas:
          "svc   svc   svc"
          "chart chart chart"
          "p50   p95   p99"
          "p999  spans errors";
        gap: 6px 8px;
        padding: 10px 12px;
      }

      .lat-col-svc   { grid-area: svc; }
      .lat-col-chart { grid-area: chart; padding: 0; }
      .lat-hist      { height: 36px; }

      .lat-col-p {
        display: flex;
        flex-direction: column;
        text-align: left;
        padding: 0;
        font-size: var(--fs-md);

        &::before {
          content: attr(data-label);
          font-size: var(--fs-3xs);
          font-weight: var(--fw-semibold);
          letter-spacing: var(--ls-caps);
          text-transform: uppercase;
          color: var(--text-muted);
        }
      }

      .p50  { grid-area: p50; }
      .p95  { grid-area: p95; }
      .p99  { grid-area: p99; }
      .p999 { grid-area: p999; }
      .lat-col-p--spans  { grid-area: spans; }
      .lat-col-p--errors { grid-area: errors; }
    }
  `],
})
export class LatencyComponent implements OnChanges {
  readonly from = input<string | undefined>(undefined);
  readonly to   = input<string | undefined>(undefined);

  private api = inject(ApiService);
  private cdr = inject(ChangeDetectorRef);

  loading      = signal(false);
  error        = signal('');
  rows         = signal<LatencyServiceDto[]>([]);
  serviceFilter = '';

  filtered = computed(() => {
    const f = this.serviceFilter.trim().toLowerCase();
    return f ? this.rows().filter(r => r.service.toLowerCase().includes(f)) : this.rows();
  });

  ngOnChanges(ch: SimpleChanges) {
    if (ch['from'] || ch['to']) this.load();
  }

  load() {
    this.loading.set(true);
    this.error.set('');
    this.api.getLatency(this.from(), this.to()).subscribe({
      next: data => {
        this.rows.set(data.sort((a, b) => b.spanCount - a.spanCount));
        this.loading.set(false);
        this.cdr.markForCheck();
      },
      error: err => {
        this.error.set(err?.error?.message ?? 'Failed to load latency data');
        this.loading.set(false);
        this.cdr.markForCheck();
      },
    });
  }

  onFilterChange() { /* computed handles it */ }

  maxBucket(buckets: LatencyBucketDto[]): number {
    return Math.max(...buckets.map(b => b.count), 1);
  }

  barFill(b: LatencyBucketDto, row: LatencyServiceDto): string {
    if (b.count === 0) return 'transparent';
    const errorFraction = row.spanCount > 0 ? row.errorCount / row.spanCount : 0;
    return errorFraction > 0.3 ? '#ef444488' : '#38bdf855';
  }

  /** Convert a latency ms value to an X coordinate in the 300-wide SVG.
   *  Uses log scale since bucket bounds are exponential. */
  pctToX(ms: number, buckets: LatencyBucketDto[]): number {
    if (!buckets.length) return 0;
    // find which bucket index ms would fall in
    let idx = buckets.findIndex(b => b.upperMs >= ms);
    if (idx < 0) idx = buckets.length - 1;
    const step = 300 / buckets.length;
    return idx * step + step / 2;
  }

  errPct(row: LatencyServiceDto): string {
    if (!row.spanCount) return '0';
    return ((row.errorCount / row.spanCount) * 100).toFixed(1);
  }

  fmtMs(ms: number): string {
    if (!ms) return '—';
    if (ms < 1)     return `${(ms * 1000).toFixed(0)}µs`;
    if (ms < 1000)  return `${ms.toFixed(2)}ms`;
    return `${(ms / 1000).toFixed(2)}s`;
  }

  fmtCount(n: number): string {
    if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
    if (n >= 1_000)     return `${(n / 1_000).toFixed(1)}K`;
    return String(n);
  }

  /** Stable per-service colour (shared hash palette — consistent with Logs / Stats). */
  svcColor(name: string): string {
    return serviceColor(name);
  }
}
