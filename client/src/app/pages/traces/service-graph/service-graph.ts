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

/** The svg's drawing space: `viewBox="0 0 W H"`, whatever the element's size on screen. */
const VIEW_W = 900;
const VIEW_H = 560;
const ZOOM_MIN = 0.3;
const ZOOM_MAX = 3;

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
          <!-- The zoom as buttons: the wheel and the pinch do the same, but a tablet has neither
               a wheel nor, on a finger-sized target, much patience for a pinch. -->
          <span class="sg-zoom">
            <button class="sg-zoom-btn" (click)="zoomBy(1 / 1.25)" title="Zoom out">−</button>
            <button class="sg-zoom-btn" (click)="zoomBy(1.25)" title="Zoom in">+</button>
            <button class="sg-zoom-btn" (click)="fit()" title="Fit the whole graph">
              <svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" width="13" height="13" stroke-linecap="round" stroke-linejoin="round">
                <path d="M6 2H2v4M10 2h4v4M6 14H2v-4M10 14h4v-4"/>
              </svg>
            </button>
          </span>
          <button class="sg-btn" (click)="relayout()">
            <svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" width="13" height="13">
              <path d="M2 8a6 6 0 1 1 1.5 4"/>
              <path d="M2 12V8h4" stroke-linecap="round" stroke-linejoin="round"/>
            </svg>
            Re-layout
          </button>
        </div>

        <!-- Pointer events, not mouse events: one path for a mouse, a pen and a finger. With
             touch-action: none (styles) the browser hands over the gestures it would otherwise
             spend on scrolling, so one finger pans or drags a node and two pinch the zoom. -->
        <svg #svgEl class="sg-svg"
             [attr.viewBox]="viewBox()"
             (pointerdown)="onPointerDown($event)"
             (pointermove)="onPointerMove($event)"
             (pointerup)="onPointerUp($event)"
             (pointercancel)="onPointerUp($event)"
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
                 [class.sg-node--selected]="selected() === n"
                 [attr.transform]="'translate(' + n.x + ',' + n.y + ')'"
                 (pointerdown)="startDrag($event, n)">
                <circle
                  [attr.r]="n.r"
                  [attr.fill]="nodeFill(n)"
                  [attr.stroke]="nodeStroke(n)"
                  [attr.stroke-width]="selected() === n ? 3 : 1.5"
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

        <!-- The tapped node's card: a corner card over the graph, a bottom sheet on a phone. A
             press that does not travel opens it; a drag never does. -->
        @if (selected(); as n) {
          <div class="sg-sheet">
            <div class="sg-sheet-hdr">
              <span class="sg-sheet-dot" [style.background]="nodeStroke(n)"></span>
              <strong class="sg-sheet-title" [title]="n.label">{{ n.label }}</strong>
              <button class="sg-sheet-close" (click)="selected.set(null)" title="Close">×</button>
            </div>
            <div class="sg-kpis">
              <div class="sg-kpi">
                <span class="sg-kpi-k">Requests</span>
                <span class="sg-kpi-v">{{ fmtCount(n.callCount) }}</span>
              </div>
              <div class="sg-kpi">
                <span class="sg-kpi-k">Error rate</span>
                <span class="sg-kpi-v" [class.sg-red]="n.errorRate > 0.1">{{ (n.errorRate * 100).toFixed(1) }}%</span>
              </div>
              <div class="sg-kpi">
                <span class="sg-kpi-k">Callers / callees</span>
                <span class="sg-kpi-v">{{ selectedDegree().in }} / {{ selectedDegree().out }}</span>
              </div>
            </div>
          </div>
        }
      }
    </div>
  `,
  // Inline styles cannot @use the shared breakpoints partial, so the phone query below spells
  // out the same value: 639px is bp.$bp-sm − 1.
  styles: [`
    :host { display: flex; flex-direction: column; flex: 1; min-height: 0; }

    .sg-root {
      position: relative;
      display: flex;
      flex-direction: column;
      flex: 1;
      min-height: 0;
      background: var(--bg-elevated);
      border-radius: 0 0 var(--radius-card) var(--radius-card);
    }

    .sg-toolbar {
      display: flex;
      align-items: center;
      gap: 8px;
      padding: 4px 12px;
      min-height: 32px;
      border-bottom: 1px solid var(--border);
      flex-shrink: 0;
    }

    .sg-stat { font-size: var(--fs-sm); color: var(--text-muted); white-space: nowrap; }
    .sg-sep  { color: var(--border); }

    .sg-zoom { display: flex; gap: 4px; margin-left: auto; }
    .sg-zoom-btn {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      width: var(--icon-btn);
      height: var(--icon-btn);
      padding: 0;
      background: none;
      border: 1px solid var(--border);
      border-radius: var(--radius);
      color: var(--text-secondary);
      font-size: var(--fs-lg);
      line-height: 1;
      cursor: pointer;
      transition: border-color .15s, color .15s;
      &:hover { border-color: var(--accent); color: var(--accent); }
    }

    .sg-btn {
      display: flex;
      align-items: center;
      gap: 5px;
      height: var(--ctl-h-sm);
      background: none;
      border: 1px solid var(--border);
      border-radius: 5px;
      color: var(--text-secondary);
      font-size: var(--fs-sm);
      padding: 0 8px;
      cursor: pointer;
      white-space: nowrap;
      transition: border-color .15s, color .15s;
      &:hover { border-color: var(--accent); color: var(--accent); }
    }

    .sg-svg {
      flex: 1;
      width: 100%;
      min-height: 0;
      cursor: grab;
      touch-action: none;
      user-select: none;
      -webkit-user-select: none;
      &:active { cursor: grabbing; }
    }

    .sg-empty {
      flex: 1;
      display: flex;
      align-items: center;
      justify-content: center;
      gap: 10px;
      color: var(--text-muted);
      font-size: var(--fs-sm);
    }

    .sg-err { color: var(--error); }

    .sg-spin {
      width: 18px; height: 18px;
      color: var(--text-muted);
      animation: sgspin 1s linear infinite;
    }
    @keyframes sgspin { to { transform: rotate(360deg); } }

    .sg-node { cursor: grab; user-select: none; }
    .sg-node-label { font-size: var(--fs-xs); font-weight: var(--fw-semibold); pointer-events: none; }
    .sg-node-sub   { font-size: var(--fs-3xs); fill: var(--text-muted); pointer-events: none; }
    .sg-edge-label { font-size: var(--fs-3xs); pointer-events: none; }

    .sg-sheet {
      position: absolute;
      left: 10px;
      bottom: 10px;
      width: 260px;
      padding: 8px 10px 10px;
      background: var(--bg-card);
      border: 1px solid var(--border);
      border-radius: var(--radius-card);
      box-shadow: var(--shadow);
      z-index: 2;
    }

    .sg-sheet-hdr {
      display: flex;
      align-items: center;
      gap: 6px;
      margin-bottom: 8px;
    }
    .sg-sheet-dot { width: 8px; height: 8px; border-radius: 50%; flex-shrink: 0; }
    .sg-sheet-title {
      flex: 1;
      min-width: 0;
      font-size: var(--fs-md);
      color: var(--text-primary);
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    .sg-sheet-close {
      width: var(--icon-btn);
      height: var(--icon-btn);
      padding: 0;
      background: none;
      border: none;
      border-radius: var(--radius-sm);
      color: var(--text-muted);
      font-size: var(--fs-lg);
      line-height: 1;
      cursor: pointer;
      &:hover { color: var(--text-primary); background: var(--bg-elevated); }
    }

    .sg-kpis { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 6px; }
    .sg-kpi {
      display: flex;
      flex-direction: column;
      gap: 2px;
      padding: 6px 8px;
      background: var(--bg-elevated);
      border: 1px solid var(--border);
      border-radius: var(--radius);
      min-width: 0;
    }
    .sg-kpi-k {
      font-size: var(--fs-3xs);
      font-weight: var(--fw-semibold);
      letter-spacing: var(--ls-caps);
      text-transform: uppercase;
      color: var(--text-muted);
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    .sg-kpi-v { font-size: var(--fs-md); font-weight: var(--fw-semibold); color: var(--text-primary); white-space: nowrap; }
    .sg-red { color: var(--error); }

    /* Phone: the node card is a sheet over the bottom nav, and the zoom buttons grow to fingers. */
    @media (max-width: 639px) {
      .sg-toolbar { flex-wrap: wrap; row-gap: 4px; padding: 6px 8px; }
      .sg-zoom-btn { width: 32px; height: 32px; }

      .sg-sheet {
        position: fixed;
        left: 0;
        right: 0;
        bottom: var(--bottom-nav-h, 0px);
        width: auto;
        padding: 18px 12px calc(12px + env(safe-area-inset-bottom, 0px));
        border-radius: 12px 12px 0 0;
        border-left: none;
        border-right: none;
        border-bottom: none;
        z-index: 60;
      }
      /* The handle pill every mobile sheet has. */
      .sg-sheet::before {
        content: '';
        position: absolute;
        top: 6px;
        left: 50%;
        width: 36px;
        height: 4px;
        border-radius: 2px;
        background: var(--border);
        transform: translateX(-50%);
      }
      .sg-sheet-close { width: 32px; height: 32px; }
    }
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

  /** The node whose card is open — the one last tapped without being dragged. */
  readonly selected = signal<LayoutNode | null>(null);

  // Viewport state
  zoom  = signal(1);
  panX  = signal(0);
  panY  = signal(0);

  readonly Math = Math;

  viewBox = computed(() => `0 0 ${VIEW_W} ${VIEW_H}`);

  layoutNodes = computed(() => this.nodes());
  layoutEdges = computed(() => this.edges());

  /** How many edges come into and go out of the selected node. */
  readonly selectedDegree = computed(() => {
    const n = this.selected();
    let into = 0, out = 0;
    if (n) for (const e of this.edges()) { if (e.toNode === n) into++; if (e.fromNode === n) out++; }
    return { in: into, out };
  });

  // ── Interaction state ──────────────────────────────────────────────────────
  /** Every pointer currently down on the svg, by id, at its last client position. */
  private readonly pointers = new Map<number, { x: number; y: number }>();
  private _dragging: LayoutNode | null = null;
  private _dragOffX = 0;
  private _dragOffY = 0;
  private _panning = false;
  /** The svg-space point the pan started from, less the pan at that moment. */
  private _panStartX = 0;
  private _panStartY = 0;
  /** Where the press on a node began, to tell a tap (select) from a drag (move). */
  private _downNode: LayoutNode | null = null;
  private _downX = 0;
  private _downY = 0;
  /** The pinch as it began: the distance between the two fingers and the zoom at that moment. */
  private _pinch: { dist: number; zoom: number } | null = null;

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
    // The card follows the node across a refresh — the nodes are new objects, so by id — and
    // closes if the service has gone from the window.
    const sel = this.selected();
    if (sel) this.selected.set(nodeById.get(sel.id) ?? null);
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
  // Everything is measured in the svg's own units (through getScreenCTM), never in client
  // pixels: the viewBox is 900 units wide whatever the element's width, and a pan counted in
  // pixels ran 2.5× too fast on a 360px phone.

  /** A press on a node: the start of a drag, or of a tap if it does not travel. */
  startDrag(ev: PointerEvent, node: LayoutNode) {
    ev.stopPropagation();
    this.capture(ev);
    this.pointers.set(ev.pointerId, { x: ev.clientX, y: ev.clientY });
    if (this.pointers.size > 1) { this.beginPinch(); return; }
    this._dragging = node;
    this._downNode = node;
    this._downX = ev.clientX;
    this._downY = ev.clientY;
    const pt = this.worldPoint(ev.clientX, ev.clientY);
    this._dragOffX = pt.x - node.x;
    this._dragOffY = pt.y - node.y;
  }

  /** A press on the background: the start of a pan — or of a pinch, once a second finger lands. */
  onPointerDown(ev: PointerEvent) {
    this.capture(ev);
    this.pointers.set(ev.pointerId, { x: ev.clientX, y: ev.clientY });
    if (this.pointers.size > 1) { this.beginPinch(); return; }
    this._panning = true;
    const p = this.svgPoint(ev.clientX, ev.clientY);
    this._panStartX = p.x - this.panX();
    this._panStartY = p.y - this.panY();
  }

  onPointerMove(ev: PointerEvent) {
    if (!this.pointers.has(ev.pointerId)) return;
    this.pointers.set(ev.pointerId, { x: ev.clientX, y: ev.clientY });
    if (this._pinch && this.pointers.size >= 2) { this.pinchMove(); return; }
    if (this._dragging) {
      const pt = this.worldPoint(ev.clientX, ev.clientY);
      this._dragging.x = pt.x - this._dragOffX;
      this._dragging.y = pt.y - this._dragOffY;
      this.nodes.set([...this.nodes()]);
      this.cdr.markForCheck();
      return;
    }
    if (this._panning) {
      const p = this.svgPoint(ev.clientX, ev.clientY);
      this.panX.set(p.x - this._panStartX);
      this.panY.set(p.y - this._panStartY);
    }
  }

  onPointerUp(ev: PointerEvent) {
    this.pointers.delete(ev.pointerId);
    // A press that did not travel is a tap: open the node's card. The threshold is in client
    // pixels and generous, because a finger wobbles where a mouse does not.
    if (this._downNode && this._dragging === this._downNode) {
      const dx = ev.clientX - this._downX, dy = ev.clientY - this._downY;
      if (dx * dx + dy * dy < 36) this.selected.set(this._downNode);
    }
    this._downNode = null;
    this._dragging = null;
    this._panning  = false;
    if (this.pointers.size < 2) this._pinch = null;
  }

  onWheel(ev: WheelEvent) {
    ev.preventDefault();
    this.zoomTo(this.zoom() * (ev.deltaY < 0 ? 1.1 : 0.9), this.svgPoint(ev.clientX, ev.clientY));
  }

  /** The ± buttons: about the centre of the view. */
  zoomBy(factor: number) {
    this.zoomTo(this.zoom() * factor, { x: VIEW_W / 2, y: VIEW_H / 2 });
  }

  /** Back to the whole graph — the viewBox already fits it to the element. */
  fit() {
    this.zoom.set(1);
    this.panX.set(0);
    this.panY.set(0);
  }

  /** A second finger: whatever one finger was doing stops, and the two now set the zoom. */
  private beginPinch() {
    const [a, b] = [...this.pointers.values()];
    this._pinch = { dist: Math.hypot(a.x - b.x, a.y - b.y) || 1, zoom: this.zoom() };
    this._dragging = null;
    this._downNode = null;
    this._panning  = false;
  }

  /** The zoom follows the distance between the fingers, anchored on the point between them —
   *  which also lets the two fingers pan together, since the anchor moves with them. */
  private pinchMove() {
    const [a, b] = [...this.pointers.values()];
    const dist = Math.hypot(a.x - b.x, a.y - b.y) || 1;
    this.zoomTo(this._pinch!.zoom * dist / this._pinch!.dist,
                this.svgPoint((a.x + b.x) / 2, (a.y + b.y) / 2));
  }

  /** Zooms to `z` (clamped) about `anchor`, in svg units: what is under it stays under it. */
  private zoomTo(z: number, anchor: { x: number; y: number }) {
    const next = Math.max(ZOOM_MIN, Math.min(ZOOM_MAX, z));
    const cur  = this.zoom();
    const wx = (anchor.x - this.panX()) / cur;
    const wy = (anchor.y - this.panY()) / cur;
    this.zoom.set(next);
    this.panX.set(anchor.x - wx * next);
    this.panY.set(anchor.y - wy * next);
  }

  /** Routes the rest of this pointer's events to the svg, wherever the finger goes. */
  private capture(ev: PointerEvent) {
    const svg = this.svgEl()?.nativeElement;
    try { svg?.setPointerCapture(ev.pointerId); }
    catch { /* a pointer that is already gone — the up handler clears it */ }
  }

  /** A client point in the svg's user units (the viewBox), before the pan / zoom transform. */
  private svgPoint(clientX: number, clientY: number): { x: number; y: number } {
    const svg = this.svgEl()?.nativeElement;
    const mat = svg?.getScreenCTM()?.inverse();
    if (!svg || !mat) return { x: clientX, y: clientY };
    const tp = new DOMPoint(clientX, clientY).matrixTransform(mat);
    return { x: tp.x, y: tp.y };
  }

  /** The same point in the graph's own coordinates — through the pan and the zoom. */
  private worldPoint(clientX: number, clientY: number): { x: number; y: number } {
    const p = this.svgPoint(clientX, clientY);
    return { x: (p.x - this.panX()) / this.zoom(), y: (p.y - this.panY()) / this.zoom() };
  }

  // ── Helpers ───────────────────────────────────────────────────────────────

  fmtCount(n: number): string {
    if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
    if (n >= 1_000)     return `${(n / 1_000).toFixed(1)}K`;
    return String(n);
  }
}
