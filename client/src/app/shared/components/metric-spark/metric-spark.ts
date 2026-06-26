import { Component, computed, input, ChangeDetectionStrategy } from '@angular/core';
import { MetricSeriesDto } from '../../../core/models/metric.model';

const W = 220;
const H = 44;
const PAD_X = 2;
const PAD_Y = 4;

function fmtV(v: number): string {
  if (!Number.isFinite(v)) return '—';
  if (Math.abs(v) >= 1e9)  return `${(v / 1e9).toFixed(1)}G`;
  if (Math.abs(v) >= 1e6)  return `${(v / 1e6).toFixed(1)}M`;
  if (Math.abs(v) >= 1e3)  return `${(v / 1e3).toFixed(1)}K`;
  if (Number.isInteger(v)) return String(v);
  return v.toFixed(2);
}

@Component({
  selector: 'app-metric-spark',
  standalone: true,
  changeDetection: ChangeDetectionStrategy.OnPush,
  host: { class: 'msp' },
  template: `
    @if (pts().length > 1) {
      <svg [attr.viewBox]="'0 0 ' + W + ' ' + H" [attr.width]="W" [attr.height]="H" overflow="visible" class="msp-svg">
        <defs>
          <linearGradient [id]="gradId" x1="0" x2="0" y1="0" y2="1">
            <stop offset="0%"   stop-color="currentColor" stop-opacity="0.25" />
            <stop offset="100%" stop-color="currentColor" stop-opacity="0.02" />
          </linearGradient>
        </defs>
        <path [attr.d]="areaPath()" [attr.fill]="'url(#' + gradId + ')'" />
        <path [attr.d]="linePath()" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linejoin="round" />
        @if (markerX() !== null) {
          <line [attr.x1]="markerX()" [attr.x2]="markerX()"
                [attr.y1]="PAD_Y" [attr.y2]="H - PAD_Y"
                stroke="currentColor" stroke-width="1" stroke-dasharray="2,3" opacity="0.55" />
          <circle [attr.cx]="markerX()" [attr.cy]="markerY()" r="3.5"
                  fill="currentColor" opacity="0.9" />
        }
      </svg>
    } @else {
      <div class="msp-no-data">no data</div>
    }
  `,
  styleUrl: './metric-spark.scss',
})
export class MetricSparkComponent {
  readonly series     = input.required<MetricSeriesDto>();
  /** Event timestamp in milliseconds. */
  readonly eventTsMs  = input.required<number>();

  protected readonly W     = W;
  protected readonly H     = H;
  protected readonly PAD_Y = PAD_Y;
  protected readonly gradId = `msp-grad-${Math.random().toString(36).slice(2, 7)}`;

  readonly pts = computed(() => this.series().points);

  private bounds = computed(() => {
    const pts = this.pts();
    if (!pts.length) return { minTs: 0, maxTs: 1, minV: 0, maxV: 1 };
    let minTs = pts[0].ts, maxTs = pts[0].ts;
    let minV  = pts[0].value, maxV = pts[0].value;
    for (const p of pts) {
      if (p.ts < minTs) minTs = p.ts;
      if (p.ts > maxTs) maxTs = p.ts;
      if (p.value < minV) minV = p.value;
      if (p.value > maxV) maxV = p.value;
    }
    return { minTs, maxTs, minV, maxV };
  });

  private tx(tsNs: number): number {
    const { minTs, maxTs } = this.bounds();
    return PAD_X + ((tsNs - minTs) / Math.max(1, maxTs - minTs)) * (W - 2 * PAD_X);
  }

  private ty(v: number): number {
    const { minV, maxV } = this.bounds();
    return H - PAD_Y - ((v - minV) / Math.max(1e-9, maxV - minV)) * (H - 2 * PAD_Y);
  }

  linePath = computed(() => {
    const pts = this.pts();
    if (pts.length < 2) return '';
    return pts.map((p, i) =>
      `${i === 0 ? 'M' : 'L'}${this.tx(p.ts).toFixed(1)},${this.ty(p.value).toFixed(1)}`
    ).join(' ');
  });

  areaPath = computed(() => {
    const pts = this.pts();
    if (pts.length < 2) return '';
    const line = pts.map((p, i) =>
      `${i === 0 ? 'M' : 'L'}${this.tx(p.ts).toFixed(1)},${this.ty(p.value).toFixed(1)}`
    ).join(' ');
    return `${line} L${this.tx(pts[pts.length - 1].ts).toFixed(1)},${H} L${this.tx(pts[0].ts).toFixed(1)},${H} Z`;
  });

  markerX = computed<number | null>(() => {
    const { minTs, maxTs } = this.bounds();
    const tsNs = this.eventTsMs() * 1_000_000;
    if (tsNs < minTs || tsNs > maxTs) return null;
    return this.tx(tsNs);
  });

  markerY = computed(() => {
    const pts = this.pts();
    if (!pts.length) return H / 2;
    const tsNs = this.eventTsMs() * 1_000_000;
    const closest = pts.reduce((a, b) =>
      Math.abs(b.ts - tsNs) < Math.abs(a.ts - tsNs) ? b : a
    );
    return this.ty(closest.value);
  });

  /** Value at event timestamp (closest point). */
  valueAtEvent = computed(() => {
    const pts = this.pts();
    if (!pts.length) return null;
    const tsNs = this.eventTsMs() * 1_000_000;
    const closest = pts.reduce((a, b) =>
      Math.abs(b.ts - tsNs) < Math.abs(a.ts - tsNs) ? b : a
    );
    return fmtV(closest.value);
  });

  unit = computed(() => this.series().unit ?? '');
}
