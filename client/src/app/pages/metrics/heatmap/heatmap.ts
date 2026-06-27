import {
  Component, input, computed, effect, viewChild, ElementRef,
  ChangeDetectionStrategy, OnDestroy,
} from '@angular/core';
import { Chart, LinearScale, Tooltip } from 'chart.js';
import { MatrixController, MatrixElement } from 'chartjs-chart-matrix';
import { format } from 'date-fns';
import { HeatmapDto } from '../../../core/models/metric.model';

Chart.register(MatrixController, MatrixElement, LinearScale, Tooltip);

interface Cell { x: number; y: number; v: number; }

@Component({
  selector: 'app-heatmap',
  standalone: true,
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: `
    @if (!hasData()) {
      <div class="hm-empty">No histogram data</div>
    }
    <div class="hm-wrap" [style.display]="hasData() ? 'block' : 'none'">
      <canvas #cv></canvas>
    </div>
  `,
  styles: [`
    :host { display: block; width: 100%; }
    .hm-wrap { position: relative; height: 240px; }
    .hm-empty {
      height: 240px; display: flex; align-items: center; justify-content: center;
      color: var(--text-muted); font-size: 13px;
    }
  `],
})
export class HeatmapComponent implements OnDestroy {
  readonly data  = input<HeatmapDto | null>(null);
  /** Multiply bucket bounds for display (e.g. 1000 for seconds → ms). */
  readonly scale = input<number>(1);

  private cv = viewChild<ElementRef<HTMLCanvasElement>>('cv');
  private chart?: Chart;

  hasData = computed(() => {
    const d = this.data();
    return !!d && d.columns.length > 0 && d.bounds.length > 0;
  });

  constructor() {
    effect(() => {
      const d = this.data();
      const scale = this.scale();
      const canvas = this.cv()?.nativeElement;
      if (!canvas || !d || !this.hasData()) { this.destroy(); return; }
      this.render(canvas, d, scale);
    });
  }

  ngOnDestroy() { this.destroy(); }
  private destroy() { this.chart?.destroy(); this.chart = undefined; }

  private render(canvas: HTMLCanvasElement, d: HeatmapDto, scale: number) {
    this.destroy();

    const nBuckets = d.bounds.length + 1;
    const cols = d.columns;

    let max = 0;
    const cells: Cell[] = [];
    for (const c of cols) {
      for (let bi = 0; bi < nBuckets; bi++) {
        const v = c.counts[bi] ?? 0;
        if (v > max) max = v;
        if (v > 0) cells.push({ x: c.ts, y: bi, v });
      }
    }
    if (max <= 0) return;

    const xMin = cols[0].ts;
    const xMax = cols[cols.length - 1].ts;

    const boundLabel = (bi: number): string => {
      if (bi <= 0) return '0';
      if (bi > d.bounds.length) return '∞';
      return fmtNum((d.bounds[bi - 1] ?? 0) * scale);
    };
    const bucketRange = (bi: number): string => {
      const lo = bi === 0 ? 0 : d.bounds[bi - 1] * scale;
      const hi = bi < d.bounds.length ? d.bounds[bi] * scale : Infinity;
      return hi === Infinity ? `> ${fmtNum(lo)}` : `${fmtNum(lo)}–${fmtNum(hi)}`;
    };

    this.chart = new Chart(canvas, {
      type: 'matrix',
      data: {
        datasets: [{
          label: 'distribution',
          data: cells as any,
          backgroundColor: (ctx: any) => colorRamp((ctx.raw?.v ?? 0) / max),
          borderWidth: 0,
          width:  ({ chart }: any) => Math.max(1, (chart.chartArea?.width  ?? 0) / Math.max(1, cols.length)),
          height: ({ chart }: any) => Math.max(1, (chart.chartArea?.height ?? 0) / nBuckets),
        } as any],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        animation: false,
        scales: {
          x: {
            type: 'linear',
            min: xMin, max: xMax,
            offset: false,
            ticks: { color: '#64748b', maxTicksLimit: 6, font: { size: 10 },
                     callback: (v: any) => format(new Date(Number(v)), 'HH:mm') },
            grid: { display: false }, border: { display: false },
          },
          y: {
            type: 'linear',
            min: -0.5, max: nBuckets - 0.5,
            offset: false,
            ticks: { color: '#64748b', font: { size: 9 }, stepSize: 1,
                     callback: (v: any) => boundLabel(Math.round(Number(v))) },
            grid: { display: false }, border: { display: false },
          },
        },
        plugins: {
          legend: { display: false },
          tooltip: {
            backgroundColor: '#0f172a', borderColor: '#263244', borderWidth: 1,
            titleColor: '#e2e8f0', bodyColor: '#94a3b8', padding: 8,
            callbacks: {
              title: (items: any) => format(new Date(Number(items[0].raw.x)), 'HH:mm:ss'),
              label: (item: any) => `${bucketRange(item.raw.y)}: ${item.raw.v}`,
            },
          },
        },
      },
    });
  }
}

function fmtNum(v: number): string {
  if (!isFinite(v)) return '∞';
  if (v >= 1000) return (v / 1000).toFixed(1) + 'k';
  if (v >= 1)    return v.toFixed(0);
  return v.toFixed(2);
}

/** 0..1 → near-bg → blue → teal → amber → red. sqrt lifts low counts. */
function colorRamp(t: number): string {
  t = Math.max(0, Math.min(1, Math.sqrt(t)));
  const stops = [
    [15, 23, 42], [30, 58, 138], [13, 148, 136], [217, 119, 6], [220, 38, 38],
  ];
  const seg = t * (stops.length - 1);
  const i = Math.min(stops.length - 2, Math.floor(seg));
  const f = seg - i;
  const a = stops[i], b = stops[i + 1];
  return `rgb(${Math.round(a[0] + (b[0] - a[0]) * f)},${Math.round(a[1] + (b[1] - a[1]) * f)},${Math.round(a[2] + (b[2] - a[2]) * f)})`;
}
