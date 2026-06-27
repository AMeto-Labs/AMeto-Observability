import {
  Component, input, signal, computed, inject, OnChanges, SimpleChanges,
  ChangeDetectionStrategy, ChangeDetectorRef,
} from '@angular/core';
import { ApiService } from '../../../core/services/api.service';

export interface FlamegraphNode {
  spanId:    string;
  name:      string;
  service:   string;
  kind:      string;
  status:    string;
  totalMs:   number;
  selfMs:    number;
  children:  FlamegraphNode[];
  // layout (computed)
  _x?:      number; // 0–1 fraction
  _w?:      number; // width fraction
  _depth?:  number;
}

@Component({
  selector:        'app-flame-graph',
  standalone:      true,
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: `
    <div class="fg-root">
      @if (loading()) {
        <div class="fg-empty">
          <svg class="fg-spin" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <path d="M12 2v4M12 18v4M4.93 4.93l2.83 2.83M16.24 16.24l2.83 2.83M2 12h4M18 12h4M4.93 19.07l2.83-2.83M16.24 7.76l2.83-2.83"/>
          </svg>
          Loading flamegraph…
        </div>
      } @else if (error()) {
        <div class="fg-empty fg-err">{{ error() }}</div>
      } @else if (!flatNodes().length) {
        <div class="fg-empty">No data</div>
      } @else {

        <div class="fg-toolbar">
          <span class="fg-stat">{{ flatNodes().length }} spans · {{ fmtMs(rootMs()) }}</span>
          @if (focused()) {
            <button class="fg-btn" (click)="resetFocus()">
              ↩ Reset zoom
            </button>
          }
          @if (hovered()) {
            <div class="fg-tooltip">
              <strong>{{ hovered()!.name }}</strong>
              <span class="fg-tt-svc">{{ hovered()!.service }}</span>
              <span class="fg-tt-dur">{{ fmtMs(hovered()!.totalMs) }} total · {{ fmtMs(hovered()!.selfMs) }} self</span>
              <span class="fg-tt-pct">{{ pct(hovered()!.totalMs) }}%</span>
            </div>
          }
        </div>

        <div class="fg-canvas" #canvas>
          @for (node of visibleNodes(); track node.spanId + node._depth) {
            <div class="fg-bar"
                 [title]="node.name + ' · ' + fmtMs(node.totalMs)"
                 [style.left.%]="barLeft(node)"
                 [style.width.%]="barWidth(node)"
                 [style.top.px]="node._depth! * ROW_H"
                 [style.height.px]="ROW_H - 2"
                 [style.background]="barColor(node)"
                 [class.fg-bar--error]="node.status === 'Error'"
                 [class.fg-bar--focused]="focused()?.spanId === node.spanId"
                 (mouseenter)="hovered.set(node)"
                 (mouseleave)="hovered.set(null)"
                 (click)="focusNode(node)">
              @if (barWidth(node) > 4) {
                <span class="fg-label">{{ node.name }}</span>
              }
            </div>
          }
        </div>

        <div class="fg-legend">
          <span class="fg-legend-item">
            <span class="fg-legend-dot" style="background:#38bdf8"></span> Normal
          </span>
          <span class="fg-legend-item">
            <span class="fg-legend-dot" style="background:#ef4444"></span> Error
          </span>
          <span class="fg-legend-item fg-legend-hint">Click to zoom · Click again to reset</span>
        </div>

      }
    </div>
  `,
  styles: [`
    :host { display: flex; flex-direction: column; flex: 1; min-height: 0; }

    .fg-root {
      display: flex;
      flex-direction: column;
      flex: 1;
      min-height: 0;
      overflow: hidden;
    }

    .fg-empty {
      flex: 1;
      display: flex;
      align-items: center;
      justify-content: center;
      gap: 8px;
      color: var(--text-muted);
      font-size: 13px;
    }
    .fg-err  { color: var(--error); }
    .fg-spin { width: 18px; height: 18px; animation: fgspin 1s linear infinite; }
    @keyframes fgspin { to { transform: rotate(360deg); } }

    .fg-toolbar {
      display: flex;
      align-items: center;
      gap: 10px;
      padding: 7px 12px;
      border-bottom: 1px solid var(--border);
      flex-shrink: 0;
      min-height: 38px;
    }

    .fg-stat { font-size: 12px; color: var(--text-muted); }

    .fg-btn {
      background: none;
      border: 1px solid var(--border);
      border-radius: 5px;
      color: var(--text-secondary);
      font-size: 12px;
      padding: 3px 10px;
      cursor: pointer;
      transition: border-color .15s, color .15s;
      &:hover { border-color: var(--accent); color: var(--accent); }
    }

    .fg-tooltip {
      margin-left: auto;
      display: flex;
      align-items: center;
      gap: 8px;
      font-size: 12px;
      strong { color: var(--text-primary); }
    }
    .fg-tt-svc  { color: var(--text-muted); }
    .fg-tt-dur  { color: var(--info); font-family: var(--font-ui); }
    .fg-tt-pct  { color: var(--accent); font-weight: 700; }

    .fg-canvas {
      flex: 1;
      position: relative;
      overflow-y: auto;
      overflow-x: hidden;
      padding-bottom: 8px;
    }

    .fg-bar {
      position: absolute;
      border-radius: 2px;
      cursor: pointer;
      overflow: hidden;
      transition: filter .1s, outline .1s;
      box-sizing: border-box;

      &:hover { filter: brightness(1.25); outline: 1px solid #fff4; }
    }
    .fg-bar.fg-bar--error   { outline: 1px solid #ef4444; }
    .fg-bar.fg-bar--focused { outline: 2px solid var(--accent); filter: brightness(1.2); }

    .fg-label {
      display: block;
      font-size: 10px;
      color: #0f172a;
      padding: 0 4px;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
      line-height: 1;
      margin-top: 4px;
      pointer-events: none;
      font-weight: 600;
    }

    .fg-legend {
      display: flex;
      align-items: center;
      gap: 12px;
      padding: 6px 12px;
      border-top: 1px solid var(--border);
      flex-shrink: 0;
    }
    .fg-legend-item   { display: flex; align-items: center; gap: 5px; font-size: 11px; color: var(--text-muted); }
    .fg-legend-dot    { width: 10px; height: 10px; border-radius: 2px; flex-shrink: 0; }
    .fg-legend-hint   { margin-left: auto; font-style: italic; }
  `],
})
export class FlamegraphComponent implements OnChanges {
  readonly traceId = input<string | null>(null);

  private api = inject(ApiService);
  private cdr = inject(ChangeDetectorRef);

  readonly ROW_H = 24;

  loading = signal(false);
  error   = signal('');
  root    = signal<FlamegraphNode | null>(null);
  focused = signal<FlamegraphNode | null>(null);
  hovered = signal<FlamegraphNode | null>(null);

  rootMs = computed(() => this.root()?.totalMs ?? 1);

  flatNodes = computed(() => {
    const r = this.root();
    if (!r) return [];
    const out: FlamegraphNode[] = [];
    const walk = (n: FlamegraphNode, depth: number, x: number, w: number) => {
      n._depth = depth;
      n._x = x;
      n._w = w;
      out.push(n);
      let cx = x;
      for (const c of n.children) {
        const cw = w * (c.totalMs / n.totalMs);
        walk(c, depth + 1, cx, cw);
        cx += cw;
      }
    };
    walk(r, 0, 0, 1);
    return out;
  });

  /** When a node is focused, show only its subtree re-scaled to full width. */
  visibleNodes = computed(() => {
    const all = this.flatNodes();
    const f   = this.focused();
    if (!f) return all;

    // collect subtree
    const ids = new Set<string>();
    const collect = (n: FlamegraphNode) => { ids.add(n.spanId); n.children.forEach(collect); };
    collect(f);

    // re-scale: f._x..f._x+f._w → 0..1
    const ox = f._x ?? 0, ow = f._w ?? 1;
    const od = f._depth ?? 0;
    return all
      .filter(n => ids.has(n.spanId))
      .map(n => ({ ...n, _x: (n._x! - ox) / ow, _w: n._w! / ow, _depth: n._depth! - od }));
  });

  ngOnChanges(ch: SimpleChanges) {
    if (ch['traceId']) {
      this.root.set(null);
      this.focused.set(null);
      this.hovered.set(null);
      const id = this.traceId();
      if (id) this.load(id);
    }
  }

  private load(traceId: string) {
    this.loading.set(true);
    this.error.set('');
    this.api.getFlamegraph(traceId).subscribe({
      next: node => {
        this.root.set(node);
        this.loading.set(false);
        this.cdr.markForCheck();
      },
      error: err => {
        this.error.set(err?.error?.message ?? 'Failed to load flamegraph');
        this.loading.set(false);
        this.cdr.markForCheck();
      },
    });
  }

  focusNode(node: FlamegraphNode) {
    if (this.focused()?.spanId === node.spanId) {
      this.focused.set(null);
    } else {
      this.focused.set(node);
    }
  }

  resetFocus() { this.focused.set(null); }

  // ── Bar geometry ──────────────────────────────────────────────────────────

  barLeft(n: FlamegraphNode): number  { return (n._x ?? 0) * 100; }
  barWidth(n: FlamegraphNode): number { return Math.max(0.05, (n._w ?? 0) * 100); }

  // ── Colors ────────────────────────────────────────────────────────────────

  private readonly PALETTE = [
    '#38bdf8','#f59e0b','#22c55e','#a78bfa',
    '#f97316','#ec4899','#06b6d4','#84cc16',
  ];

  barColor(n: FlamegraphNode): string {
    if (n.status === 'Error') return '#ef4444';
    let h = 0;
    for (const c of n.service) h = (h * 31 + c.charCodeAt(0)) & 0x7fff_ffff;
    const base = this.PALETTE[h % this.PALETTE.length];
    // darken slightly for self-time visual distinction
    return base;
  }

  // ── Helpers ───────────────────────────────────────────────────────────────

  pct(ms: number): string {
    return ((ms / this.rootMs()) * 100).toFixed(1);
  }

  fmtMs(ms: number): string {
    if (ms < 0.001) return `${(ms * 1_000_000).toFixed(0)}ns`;
    if (ms < 1)     return `${(ms * 1_000).toFixed(0)}µs`;
    if (ms < 1_000) return `${ms.toFixed(2)}ms`;
    return `${(ms / 1_000).toFixed(3)}s`;
  }
}
