import {
  Component, inject, signal, computed, input, OnChanges, SimpleChanges,
  ChangeDetectionStrategy, ChangeDetectorRef, ElementRef, viewChild,
} from '@angular/core';
import { ApiService } from '../../../core/services/api.service';

export interface ServiceNode { id: string; label: string; errorRate: number; callCount: number; }
export interface ServiceEdge { from: string; to: string; callCount: number; errorCount: number; }

interface LayoutNode extends ServiceNode {
  x: number; y: number; r: number;
  vx: number; vy: number;
}

interface LayoutEdge extends ServiceEdge {
  fromNode: LayoutNode;
  toNode:   LayoutNode;
}

@Component({
  selector:         'app-service-graph',
  standalone:       true,
  changeDetection:  ChangeDetectionStrategy.OnPush,
  template: `
    <div class="sg-root">
      @if (loading()) {
        <div class="sg-empty">
          <svg class="sg-spin" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <path d="M12 2v4M12 18v4M4.93 4.93l2.83 2.83M16.24 16.24l2.83 2.83M2 12h4M18 12h4M4.93 19.07l2.83-2.83M16.24 7.76l2.83-2.83"/>
          </svg>
          Loading service graph…
        </div>
      } @else if (error()) {
        <div class="sg-empty sg-err">{{ error() }}</div>
      } @else if (nodes().length === 0) {
        <div class="sg-empty">No service data yet</div>
      } @else {
        <div class="sg-toolbar">
          <span class="sg-stat">{{ nodes().length }} services</span>
          <span class="sg-sep">·</span>
          <span class="sg-stat">{{ edges().length }} connections</span>
          <button class="sg-btn" (click)="relayout()">
            <svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" width="13" height="13">
              <path d="M2 8a6 6 0 1 1 1.5 4"/>
              <path d="M2 12V8h4" stroke-linecap="round" stroke-linejoin="round"/>
            </svg>
            Re-layout
          </button>
        </div>

        <svg #svgEl class="sg-svg"
             [attr.viewBox]="viewBox()"
             (mousedown)="onMouseDown($event)"
             (mousemove)="onMouseMove($event)"
             (mouseup)="onMouseUp()"
             (mouseleave)="onMouseUp()"
             (wheel)="onWheel($event)">

          <defs>
            <marker id="arrow-ok"   markerWidth="8" markerHeight="8" refX="6" refY="3" orient="auto">
              <path d="M0,0 L0,6 L8,3 z" fill="#475569"/>
            </marker>
            <marker id="arrow-err"  markerWidth="8" markerHeight="8" refX="6" refY="3" orient="auto">
              <path d="M0,0 L0,6 L8,3 z" fill="#ef4444"/>
            </marker>
          </defs>

          <g [attr.transform]="'translate(' + panX() + ',' + panY() + ') scale(' + zoom() + ')'">

            <!-- Edges -->
            @for (e of layoutEdges(); track e.from + e.to) {
              @let isErr = e.errorCount / Math.max(e.callCount, 1) > 0.1;
              @let mid = edgeMid(e);
              <line
                [attr.x1]="edgeSrc(e).x" [attr.y1]="edgeSrc(e).y"
                [attr.x2]="edgeDst(e).x" [attr.y2]="edgeDst(e).y"
                [attr.stroke]="isErr ? '#ef4444' : '#475569'"
                [attr.stroke-width]="edgeWidth(e)"
                [attr.marker-end]="isErr ? 'url(#arrow-err)' : 'url(#arrow-ok)'"
                stroke-opacity="0.7"
              />
              <text [attr.x]="mid.x" [attr.y]="mid.y - 4"
                    text-anchor="middle" class="sg-edge-label"
                    [attr.fill]="isErr ? '#ef4444' : '#94a3b8'">
                {{ fmtCount(e.callCount) }}{{ isErr ? ' ⚠' : '' }}
              </text>
            }

            <!-- Nodes -->
            @for (n of layoutNodes(); track n.id) {
              <g class="sg-node"
                 [attr.transform]="'translate(' + n.x + ',' + n.y + ')'"
                 (mousedown)="startDrag($event, n)">
                <circle
                  [attr.r]="n.r"
                  [attr.fill]="nodeFill(n)"
                  [attr.stroke]="nodeStroke(n)"
                  stroke-width="1.5"
                />
                <text y="4" text-anchor="middle" class="sg-node-label"
                      [attr.fill]="n.errorRate > 0.1 ? '#fca5a5' : '#e2e8f0'">
                  {{ n.label.length > 14 ? n.label.slice(0, 12) + '…' : n.label }}
                </text>
                @if (n.callCount > 0) {
                  <text y="18" text-anchor="middle" class="sg-node-sub">
                    {{ fmtCount(n.callCount) }}/req
                  </text>
                }
              </g>
            }

          </g>
        </svg>
      }
    </div>
  `,
  styles: [`
    :host { display: flex; flex-direction: column; flex: 1; min-height: 0; }

    .sg-root {
      display: flex;
      flex-direction: column;
      flex: 1;
      min-height: 0;
      background: var(--bg-elevated);
      border-radius: 0 0 8px 8px;
    }

    .sg-toolbar {
      display: flex;
      align-items: center;
      gap: 8px;
      padding: 8px 14px;
      border-bottom: 1px solid var(--border);
      flex-shrink: 0;
    }

    .sg-stat { font-size: 12px; color: var(--text-muted); }
    .sg-sep  { color: var(--border); }

    .sg-btn {
      margin-left: auto;
      display: flex;
      align-items: center;
      gap: 5px;
      background: none;
      border: 1px solid var(--border);
      border-radius: 5px;
      color: var(--text-secondary);
      font-size: 12px;
      padding: 4px 10px;
      cursor: pointer;
      transition: border-color .15s, color .15s;
      &:hover { border-color: var(--accent); color: var(--accent); }
    }

    .sg-svg {
      flex: 1;
      width: 100%;
      cursor: grab;
      &:active { cursor: grabbing; }
    }

    .sg-empty {
      flex: 1;
      display: flex;
      align-items: center;
      justify-content: center;
      gap: 10px;
      color: var(--text-muted);
      font-size: 13px;
    }

    .sg-err { color: var(--error); }

    .sg-spin {
      width: 18px; height: 18px;
      color: var(--text-muted);
      animation: sgspin 1s linear infinite;
    }
    @keyframes sgspin { to { transform: rotate(360deg); } }

    .sg-node { cursor: grab; user-select: none; }
    .sg-node-label { font-size: 11px; font-weight: 600; pointer-events: none; }
    .sg-node-sub   { font-size: 9px; fill: #64748b; pointer-events: none; }
    .sg-edge-label { font-size: 9px; pointer-events: none; }
  `],
})
export class ServiceGraphComponent implements OnChanges {
  readonly from = input<string | undefined>(undefined);
  readonly to   = input<string | undefined>(undefined);

  private api = inject(ApiService);
  private cdr = inject(ChangeDetectorRef);

  readonly svgEl = viewChild<ElementRef<SVGSVGElement>>('svgEl');

  loading = signal(true);
  error   = signal('');
  nodes   = signal<LayoutNode[]>([]);
  edges   = signal<LayoutEdge[]>([]);

  // Viewport state
  zoom  = signal(1);
  panX  = signal(0);
  panY  = signal(0);

  readonly Math = Math;

  viewBox = computed(() => {
    const W = 900, H = 560;
    return `0 0 ${W} ${H}`;
  });

  layoutNodes = computed(() => this.nodes());
  layoutEdges = computed(() => this.edges());

  // Drag state
  private _dragging: LayoutNode | null = null;
  private _dragOffX = 0;
  private _dragOffY = 0;
  private _panning = false;
  private _panStartX = 0;
  private _panStartY = 0;

  ngOnChanges(ch: SimpleChanges) {
    if (ch['from'] || ch['to']) this.load();
  }

  load() {
    // Only show the full-screen spinner on the first load — a refresh keeps the
    // existing graph on screen (no blank flash).
    if (this.nodes().length === 0) this.loading.set(true);
    this.error.set('');
    this.api.getServiceGraph(this.from(), this.to()).subscribe({
      next: data => {
        this.buildLayout(data.nodes ?? [], data.edges ?? []);
        this.loading.set(false);
        this.cdr.markForCheck();
      },
      error: err => {
        this.error.set(err?.message ?? 'Failed to load service graph');
        this.loading.set(false);
        this.cdr.markForCheck();
      },
    });
  }

  relayout() {
    const ns = this.nodes();
    const W = 900, H = 500;
    const cx = W / 2, cy = H / 2;
    const r = Math.min(cx, cy) * 0.7;
    ns.forEach((n, i) => {
      const a = (2 * Math.PI * i) / ns.length - Math.PI / 2;
      n.x = cx + r * Math.cos(a);
      n.y = cy + r * Math.sin(a);
    });
    this.nodes.set([...ns]);
    this.cdr.markForCheck();
  }

  private buildLayout(rawNodes: any[], rawEdges: any[]) {
    const W = 900, H = 500;
    const cx = W / 2, cy = H / 2;
    const count = rawNodes.length;
    const r = Math.min(cx, cy) * 0.7;

    // Reuse the position of any node that already exists so a data refresh does
    // NOT relayout the whole graph (that was the "jumping"). Only brand-new nodes
    // get a fresh position, and the force simulation runs only on the first layout.
    const prev = new Map(this.nodes().map(n => [n.id, n]));
    const nodeById = new Map<string, LayoutNode>();

    const ns: LayoutNode[] = rawNodes.map((n, i) => {
      const a = (2 * Math.PI * i) / count - Math.PI / 2;
      const calls = n.callCount ?? n.requestCount ?? 0;
      const errs  = n.errorCount ?? 0;
      const id    = n.id ?? n.serviceName ?? n.name ?? String(i);
      const old   = prev.get(id);
      const ln: LayoutNode = {
        id,
        label:     n.label ?? n.serviceName ?? n.name ?? String(i),
        errorRate: calls > 0 ? errs / calls : 0,
        callCount: calls,
        x:  old ? old.x : (count === 1 ? cx : cx + r * Math.cos(a)),
        y:  old ? old.y : (count === 1 ? cy : cy + r * Math.sin(a)),
        r: Math.max(30, Math.min(52, 28 + Math.log10(Math.max(calls, 1)) * 6)),
        vx: 0, vy: 0,
      };
      nodeById.set(ln.id, ln);
      return ln;
    });

    const es: LayoutEdge[] = [];
    for (const e of rawEdges) {
      const fn = nodeById.get(e.from ?? e.fromService);
      const tn = nodeById.get(e.to   ?? e.toService);
      if (fn && tn && fn !== tn)
        es.push({ from: fn.id, to: tn.id, callCount: e.callCount ?? 0, errorCount: e.errorCount ?? 0, fromNode: fn, toNode: tn });
    }

    this.nodes.set(ns);
    this.edges.set(es);
    // First layout → position from scratch. Refresh → keep existing positions (no jump).
    if (prev.size === 0) this.runForceSimulation(ns, es);
    else this.cdr.markForCheck();
  }

  /**
   * Fruchterman–Reingold force-directed layout. Forces (vx/vy) are recomputed
   * fresh every iteration and applied as a displacement capped by a temperature
   * that cools over time — so the layout converges to a spread instead of the
   * runaway that piled every node into the corners.
   */
  private runForceSimulation(ns: LayoutNode[], es: LayoutEdge[]) {
    const W = 900, H = 500;
    const cx = W / 2, cy = H / 2;
    const IDEAL = 150, REPEL = 14000, LINK = 0.05, GRAVITY = 0.02;
    const ITER  = 320;
    let   temp  = 42;                 // max displacement per node per iteration

    for (let t = 0; t < ITER; t++) {
      for (const n of ns) { n.vx = 0; n.vy = 0; }   // fresh force accumulation

      // Repulsion between every pair (~1/d²).
      for (let i = 0; i < ns.length; i++) {
        for (let j = i + 1; j < ns.length; j++) {
          let dx = ns[i].x - ns[j].x;
          let dy = ns[i].y - ns[j].y;
          let d2 = dx * dx + dy * dy;
          if (d2 < 0.01) { dx = Math.random() - 0.5; dy = Math.random() - 0.5; d2 = 0.01; }
          const d = Math.sqrt(d2);
          const f = REPEL / d2;
          ns[i].vx += (dx / d) * f; ns[i].vy += (dy / d) * f;
          ns[j].vx -= (dx / d) * f; ns[j].vy -= (dy / d) * f;
        }
      }
      // Spring attraction along edges toward the ideal length.
      for (const e of es) {
        const dx = e.toNode.x - e.fromNode.x;
        const dy = e.toNode.y - e.fromNode.y;
        const d  = Math.sqrt(dx * dx + dy * dy) || 1;
        const f  = LINK * (d - IDEAL);
        e.fromNode.vx += (dx / d) * f; e.fromNode.vy += (dy / d) * f;
        e.toNode.vx   -= (dx / d) * f; e.toNode.vy   -= (dy / d) * f;
      }
      // Gentle gravity toward the centre so disconnected nodes don't drift off.
      for (const n of ns) {
        n.vx += (cx - n.x) * GRAVITY;
        n.vy += (cy - n.y) * GRAVITY;
      }
      // Integrate: move by the force direction, capped at the current temperature.
      for (const n of ns) {
        const disp = Math.sqrt(n.vx * n.vx + n.vy * n.vy) || 1;
        const step = Math.min(disp, temp);
        n.x = Math.max(n.r + 10, Math.min(W - n.r - 10, n.x + (n.vx / disp) * step));
        n.y = Math.max(n.r + 10, Math.min(H - n.r - 10, n.y + (n.vy / disp) * step));
      }
      temp = Math.max(2, temp * 0.985);   // cool down
    }

    this.nodes.set([...ns]);
    this.edges.set([...es]);
    this.cdr.markForCheck();
  }

  // ── Edge geometry (shorten to node radius) ────────────────────────────────

  edgeSrc(e: LayoutEdge): { x: number; y: number } { return this.trimEdge(e.fromNode, e.toNode, e.fromNode.r + 2); }
  edgeDst(e: LayoutEdge): { x: number; y: number } { return this.trimEdge(e.toNode,   e.fromNode, e.toNode.r + 10); }

  edgeMid(e: LayoutEdge): { x: number; y: number } {
    return { x: (e.fromNode.x + e.toNode.x) / 2, y: (e.fromNode.y + e.toNode.y) / 2 };
  }

  private trimEdge(from: LayoutNode, to: LayoutNode, dist: number): { x: number; y: number } {
    const dx = to.x - from.x, dy = to.y - from.y;
    const d  = Math.sqrt(dx * dx + dy * dy) || 1;
    return { x: from.x + dx / d * dist, y: from.y + dy / d * dist };
  }

  edgeWidth(e: LayoutEdge): number {
    return Math.max(1, Math.min(5, 1 + Math.log10(Math.max(e.callCount, 1))));
  }

  // ── Node colors ───────────────────────────────────────────────────────────

  nodeFill(n: LayoutNode): string {
    if (n.errorRate > 0.3) return '#7f1d1d';
    if (n.errorRate > 0.1) return '#451a03';
    return '#1e293b';
  }

  nodeStroke(n: LayoutNode): string {
    if (n.errorRate > 0.3) return '#ef4444';
    if (n.errorRate > 0.1) return '#f97316';
    return '#38bdf8';
  }

  // ── Interaction ───────────────────────────────────────────────────────────

  startDrag(ev: MouseEvent, node: LayoutNode) {
    ev.stopPropagation();
    this._dragging = node;
    const pt = this.svgPoint(ev);
    this._dragOffX = pt.x - node.x;
    this._dragOffY = pt.y - node.y;
  }

  onMouseDown(ev: MouseEvent) {
    if (ev.target === this.svgEl()?.nativeElement || (ev.target as SVGElement).tagName === 'svg') {
      this._panning = true;
      this._panStartX = ev.clientX - this.panX();
      this._panStartY = ev.clientY - this.panY();
    }
  }

  onMouseMove(ev: MouseEvent) {
    if (this._dragging) {
      const pt = this.svgPoint(ev);
      this._dragging.x = pt.x - this._dragOffX;
      this._dragging.y = pt.y - this._dragOffY;
      this.nodes.set([...this.nodes()]);
      this.cdr.markForCheck();
      return;
    }
    if (this._panning) {
      this.panX.set(ev.clientX - this._panStartX);
      this.panY.set(ev.clientY - this._panStartY);
    }
  }

  onMouseUp() {
    this._dragging = null;
    this._panning  = false;
  }

  onWheel(ev: WheelEvent) {
    ev.preventDefault();
    const factor = ev.deltaY < 0 ? 1.1 : 0.9;
    this.zoom.update(z => Math.max(0.3, Math.min(3, z * factor)));
  }

  private svgPoint(ev: MouseEvent): { x: number; y: number } {
    const svg = this.svgEl()?.nativeElement;
    if (!svg) return { x: ev.offsetX, y: ev.offsetY };
    const pt = svg.createSVGPoint();
    pt.x = ev.clientX;
    pt.y = ev.clientY;
    const mat = svg.getScreenCTM()?.inverse();
    if (!mat) return { x: ev.offsetX, y: ev.offsetY };
    const tp = pt.matrixTransform(mat);
    return { x: (tp.x - this.panX()) / this.zoom(), y: (tp.y - this.panY()) / this.zoom() };
  }

  // ── Helpers ───────────────────────────────────────────────────────────────

  fmtCount(n: number): string {
    if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
    if (n >= 1_000)     return `${(n / 1_000).toFixed(1)}K`;
    return String(n);
  }
}
