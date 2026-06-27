import {
  Component, signal, computed, inject,
  ChangeDetectionStrategy, ChangeDetectorRef,
} from '@angular/core';
import { FormsModule } from '@angular/forms';
import { LucideAngularModule } from 'lucide-angular';
import { ApiService } from '../../../core/services/api.service';
import { SpanDto } from '../../../core/models/span.model';

interface AlignedRow {
  name:     string;
  aSpan:    SpanDto | null;
  bSpan:    SpanDto | null;
  diffMs:   number | null;  // b - a in ms
}

@Component({
  selector:        'app-compare-trace',
  standalone:      true,
  imports:         [FormsModule, LucideAngularModule],
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: `
    <div class="cmp-root">

      <!-- Inputs -->
      <div class="cmp-inputs">
        <div class="cmp-input-group">
          <label class="cmp-label">Trace A</label>
          <input class="cmp-id-input" [(ngModel)]="traceAInput"
                 placeholder="32-char hex trace ID"
                 spellcheck="false"
                 (keyup.enter)="compare()" />
        </div>
        <span class="cmp-vs">vs</span>
        <div class="cmp-input-group">
          <label class="cmp-label">Trace B</label>
          <input class="cmp-id-input" [(ngModel)]="traceBInput"
                 placeholder="32-char hex trace ID"
                 spellcheck="false"
                 (keyup.enter)="compare()" />
        </div>
        <button class="cmp-btn" [disabled]="!canCompare()" (click)="compare()">
          <lucide-icon name="git-compare" [size]="13" [class.spin]="loading()" />
          Compare
        </button>
      </div>

      @if (error()) {
        <div class="cmp-error">
          <lucide-icon name="alert-circle" [size]="12" />
          {{ error() }}
        </div>
      }

      @if (loading() && !rows().length) {
        <div class="cmp-empty">
          <lucide-icon name="loader" [size]="18" class="spin" /> Loading…
        </div>
      } @else if (rows().length) {

        <!-- Summary banner -->
        <div class="cmp-summary">
          <div class="cmp-sum-item">
            <span class="cmp-sum-label">Trace A total</span>
            <span class="cmp-sum-val">{{ fmtMs(totalA()) }}</span>
          </div>
          <div class="cmp-sum-item">
            <span class="cmp-sum-label">Trace B total</span>
            <span class="cmp-sum-val">{{ fmtMs(totalB()) }}</span>
          </div>
          <div class="cmp-sum-item">
            <span class="cmp-sum-label">Δ total</span>
            <span class="cmp-sum-val" [class.faster]="totalB() < totalA()"
                  [class.slower]="totalB() > totalA()">
              {{ diffLabel(totalB() - totalA()) }}
            </span>
          </div>
          <div class="cmp-sum-item">
            <span class="cmp-sum-label">A spans</span>
            <span class="cmp-sum-val">{{ spansA().length }}</span>
          </div>
          <div class="cmp-sum-item">
            <span class="cmp-sum-label">B spans</span>
            <span class="cmp-sum-val">{{ spansB().length }}</span>
          </div>
        </div>

        <!-- Table -->
        <div class="cmp-table-wrap">
          <div class="cmp-head">
            <span class="cmp-col-name">Operation</span>
            <span class="cmp-col-a">Trace A</span>
            <span class="cmp-col-b">Trace B</span>
            <span class="cmp-col-diff">Δ (B−A)</span>
            <span class="cmp-col-bar">Visual diff</span>
          </div>
          <div class="cmp-body">
            @for (row of rows(); track row.name) {
              <div class="cmp-row" [class.only-a]="!row.bSpan" [class.only-b]="!row.aSpan">
                <div class="cmp-col-name">
                  <span class="cmp-dot"
                        [style.background]="row.aSpan ? svcColor(row.aSpan.serviceName)
                                           : row.bSpan ? svcColor(row.bSpan.serviceName) : '#94a3b8'"></span>
                  <div class="cmp-names">
                    <span class="cmp-op">{{ row.name }}</span>
                    @if (row.aSpan?.serviceName || row.bSpan?.serviceName) {
                      <span class="cmp-svc">
                        {{ row.aSpan?.serviceName || row.bSpan?.serviceName }}
                      </span>
                    }
                  </div>
                </div>

                <span class="cmp-col-a mono">
                  {{ row.aSpan ? fmtMs(row.aSpan.durationNanos / 1_000_000) : '—' }}
                  @if (!row.aSpan) { <span class="badge-missing">missing</span> }
                </span>

                <span class="cmp-col-b mono">
                  {{ row.bSpan ? fmtMs(row.bSpan.durationNanos / 1_000_000) : '—' }}
                  @if (!row.bSpan) { <span class="badge-missing">missing</span> }
                </span>

                <span class="cmp-col-diff mono"
                      [class.faster]="(row.diffMs ?? 0) < 0"
                      [class.slower]="(row.diffMs ?? 0) > 0">
                  {{ row.diffMs !== null ? diffLabel(row.diffMs) : '—' }}
                </span>

                <div class="cmp-col-bar">
                  @if (row.aSpan && row.bSpan) {
                    @let maxDur = Math.max(row.aSpan.durationNanos, row.bSpan.durationNanos, 1);
                    <div class="diff-bars">
                      <div class="diff-bar diff-bar--a"
                           [style.width.%]="(row.aSpan.durationNanos / maxDur) * 100"
                           title="A: {{ fmtMs(row.aSpan.durationNanos / 1_000_000) }}">
                      </div>
                      <div class="diff-bar diff-bar--b"
                           [style.width.%]="(row.bSpan.durationNanos / maxDur) * 100"
                           title="B: {{ fmtMs(row.bSpan.durationNanos / 1_000_000) }}">
                      </div>
                    </div>
                  }
                </div>
              </div>
            }
          </div>
        </div>

      } @else if (!loading()) {
        <div class="cmp-empty">Enter two trace IDs above and click Compare</div>
      }
    </div>
  `,
  styles: [`
    :host { display: flex; flex-direction: column; flex: 1; min-height: 0; }

    .cmp-root {
      display: flex;
      flex-direction: column;
      flex: 1;
      min-height: 0;
      overflow: hidden;
    }

    .cmp-inputs {
      display: flex;
      align-items: flex-end;
      gap: 12px;
      padding: 12px 16px;
      border-bottom: 1px solid var(--border);
      flex-shrink: 0;
      flex-wrap: wrap;
    }

    .cmp-input-group {
      display: flex;
      flex-direction: column;
      gap: 4px;
      flex: 1;
      min-width: 240px;
      max-width: 420px;
    }

    .cmp-label {
      font-size: 11px;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      color: var(--text-muted);
    }

    .cmp-id-input {
      height: 34px;
      padding: 0 12px;
      background: var(--bg-elevated);
      border: 1px solid var(--border);
      border-radius: 6px;
      color: var(--text-primary);
      font-size: 12px;
      font-family: var(--font-ui);
      outline: none;
      box-sizing: border-box;

      &::placeholder { color: var(--text-muted); }
      &:focus { border-color: var(--info); }
    }

    .cmp-vs {
      font-size: 13px;
      font-weight: 700;
      color: var(--text-muted);
      padding-bottom: 6px;
      flex-shrink: 0;
    }

    .cmp-btn {
      display: flex;
      align-items: center;
      gap: 6px;
      height: 34px;
      padding: 0 18px;
      background: var(--accent);
      border: none;
      border-radius: 6px;
      color: #000;
      font-size: 13px;
      font-weight: 600;
      cursor: pointer;
      flex-shrink: 0;
      transition: opacity .15s;

      &:hover:not(:disabled) { opacity: .85; }
      &:disabled { opacity: .4; cursor: default; }
    }

    .cmp-error {
      display: flex;
      align-items: center;
      gap: 6px;
      padding: 8px 16px;
      font-size: 12px;
      color: var(--error);
      flex-shrink: 0;
    }

    .cmp-summary {
      display: flex;
      gap: 0;
      border-bottom: 1px solid var(--border);
      flex-shrink: 0;
    }

    .cmp-sum-item {
      flex: 1;
      display: flex;
      flex-direction: column;
      gap: 2px;
      padding: 10px 16px;
      border-right: 1px solid var(--border);

      &:last-child { border-right: none; }
    }

    .cmp-sum-label {
      font-size: 10px;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      color: var(--text-muted);
    }

    .cmp-sum-val {
      font-size: 16px;
      font-weight: 700;
      color: var(--text-primary);
      font-family: var(--font-ui);

      &.faster { color: var(--success); }
      &.slower { color: var(--error); }
    }

    .cmp-empty {
      flex: 1;
      display: flex;
      align-items: center;
      justify-content: center;
      gap: 8px;
      color: var(--text-muted);
      font-size: 13px;
    }

    .cmp-table-wrap {
      flex: 1;
      display: flex;
      flex-direction: column;
      min-height: 0;
      overflow: hidden;
    }

    .cmp-head {
      display: grid;
      grid-template-columns: 1fr 80px 80px 90px 1fr;
      gap: 0;
      background: var(--bg-elevated);
      border-bottom: 1px solid var(--border);
      font-size: 10px;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      color: var(--text-muted);
      padding: 6px 16px;
      flex-shrink: 0;
    }

    .cmp-body {
      flex: 1;
      overflow-y: auto;
    }

    .cmp-row {
      display: grid;
      grid-template-columns: 1fr 80px 80px 90px 1fr;
      align-items: center;
      padding: 7px 16px;
      border-bottom: 1px solid var(--border);
      gap: 0;

      &:last-child { border-bottom: none; }
      &:hover { background: var(--bg-elevated); }
      &.only-a { background: color-mix(in srgb, var(--info) 4%, transparent); }
      &.only-b { background: color-mix(in srgb, var(--accent) 4%, transparent); }
    }

    .cmp-col-name {
      display: flex;
      align-items: center;
      gap: 7px;
      overflow: hidden;
    }

    .cmp-dot {
      width: 7px;
      height: 7px;
      border-radius: 50%;
      flex-shrink: 0;
    }

    .cmp-names {
      display: flex;
      flex-direction: column;
      gap: 1px;
      overflow: hidden;
    }

    .cmp-op {
      font-size: 12px;
      color: var(--text-primary);
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }

    .cmp-svc {
      font-size: 10px;
      color: var(--text-muted);
      white-space: nowrap;
    }

    .cmp-col-a, .cmp-col-b {
      font-size: 12px;
      color: var(--text-secondary);
      text-align: right;
      padding-right: 12px;
    }

    .cmp-col-diff {
      font-size: 12px;
      font-weight: 600;
      text-align: right;
      padding-right: 12px;

      &.faster { color: var(--success); }
      &.slower { color: var(--error); }
    }

    .cmp-col-bar { padding: 0 4px; }

    .diff-bars {
      display: flex;
      flex-direction: column;
      gap: 3px;
    }

    .diff-bar {
      height: 8px;
      border-radius: 2px;
      min-width: 2px;
      transition: width .2s;
    }
    .diff-bar--a { background: var(--info); opacity: .7; }
    .diff-bar--b { background: var(--accent); opacity: .7; }

    .badge-missing {
      font-size: 9px;
      font-weight: 700;
      padding: 1px 4px;
      border-radius: 3px;
      background: color-mix(in srgb, var(--text-muted) 15%, transparent);
      color: var(--text-muted);
      margin-left: 4px;
      font-style: normal;
    }

    .mono { font-family: var(--font-ui); }

    @keyframes spin { to { transform: rotate(360deg); } }
    .spin { animation: spin 1s linear infinite; }
  `],
})
export class CompareTraceComponent {
  private api = inject(ApiService);
  private cdr = inject(ChangeDetectorRef);

  protected readonly Math = Math;

  traceAInput = '';
  traceBInput = '';

  loading = signal(false);
  error   = signal('');
  spansA  = signal<SpanDto[]>([]);
  spansB  = signal<SpanDto[]>([]);

  canCompare(): boolean {
    return this.traceAInput.trim().length > 0 && this.traceBInput.trim().length > 0;
  }

  totalA = computed(() => this.rootDurMs(this.spansA()));
  totalB = computed(() => this.rootDurMs(this.spansB()));

  rows = computed<AlignedRow[]>(() => {
    const a = this.spansA();
    const b = this.spansB();
    if (!a.length && !b.length) return [];

    // Group by operation name within each trace (take first occurrence)
    const aMap = new Map<string, SpanDto>();
    for (const s of a) if (!aMap.has(s.name)) aMap.set(s.name, s);
    const bMap = new Map<string, SpanDto>();
    for (const s of b) if (!bMap.has(s.name)) bMap.set(s.name, s);

    const names = new Set([...aMap.keys(), ...bMap.keys()]);
    const result: AlignedRow[] = [];

    for (const name of names) {
      const aSpan = aMap.get(name) ?? null;
      const bSpan = bMap.get(name) ?? null;
      const diffMs = (aSpan && bSpan)
        ? (bSpan.durationNanos - aSpan.durationNanos) / 1_000_000
        : null;
      result.push({ name, aSpan, bSpan, diffMs });
    }

    // Sort: shared rows first by abs diff desc, then only-in-A, then only-in-B
    return result.sort((x, y) => {
      const xBoth = x.aSpan && x.bSpan ? 1 : 0;
      const yBoth = y.aSpan && y.bSpan ? 1 : 0;
      if (xBoth !== yBoth) return yBoth - xBoth;
      return Math.abs(y.diffMs ?? 0) - Math.abs(x.diffMs ?? 0);
    });
  });

  compare() {
    const a = this.traceAInput.trim();
    const b = this.traceBInput.trim();
    if (!a || !b) return;
    this.loading.set(true);
    this.error.set('');
    this.spansA.set([]);
    this.spansB.set([]);
    this.api.compareTraces(a, b).subscribe({
      next: result => {
        this.spansA.set(result.traceA);
        this.spansB.set(result.traceB);
        this.loading.set(false);
        this.cdr.markForCheck();
      },
      error: err => {
        this.error.set(err?.error?.message ?? err?.message ?? 'Compare failed');
        this.loading.set(false);
        this.cdr.markForCheck();
      },
    });
  }

  diffLabel(ms: number): string {
    if (Math.abs(ms) < 0.01) return '±0';
    const sign = ms > 0 ? '+' : '';
    return sign + this.fmtMs(Math.abs(ms));
  }

  fmtMs(ms: number): string {
    if (ms < 0.001) return `${(ms * 1_000_000).toFixed(0)}ns`;
    if (ms < 1)     return `${(ms * 1_000).toFixed(0)}µs`;
    if (ms < 1_000) return `${ms.toFixed(2)}ms`;
    return `${(ms / 1_000).toFixed(3)}s`;
  }

  private rootDurMs(spans: SpanDto[]): number {
    if (!spans.length) return 0;
    const root = spans.find(s => !s.parentSpanId || /^0+$/.test(s.parentSpanId)) ?? spans[0];
    return root.durationNanos / 1_000_000;
  }

  private readonly PALETTE = [
    '#38bdf8','#f59e0b','#22c55e','#a78bfa',
    '#f97316','#ec4899','#06b6d4','#84cc16',
  ];

  svcColor(name: string): string {
    let h = 0;
    for (const c of name) h = (h * 31 + c.charCodeAt(0)) & 0x7fff_ffff;
    return this.PALETTE[h % this.PALETTE.length];
  }
}
