import {
  Component, computed, signal, inject, HostListener,
  input, output, ChangeDetectionStrategy, effect, untracked,
  viewChild, TemplateRef, ViewContainerRef,
} from '@angular/core';
import { DomSanitizer } from '@angular/platform-browser';
import { NgTemplateOutlet, DatePipe } from '@angular/common';
import { Overlay, OverlayRef } from '@angular/cdk/overlay';
import { TemplatePortal } from '@angular/cdk/portal';
import { LucideAngularModule } from 'lucide-angular';

import { EventDto } from '../../../core/models/event.model';
import { renderMessageHtml, escapeHtmlExport } from '../../../shared/utils/clef-renderer';

const LEVEL_SHORT: Record<string, string> = {
  verbose: 'VRB', debug: 'DBG', information: 'INF',
  warning: 'WRN', error: 'ERR', fatal: 'FTL',
};

export interface PropEntry {
  /** Full dot-path usable in filter expressions, e.g. "Payload.Customer.Id" */
  path: string;
  /** Last segment, shown in the table */
  label: string;
  /** Stringified value; empty for group rows */
  value: string;
  /** Nesting depth (0 = top-level) */
  depth: number;
  /** True for object-header rows (non-clickable, no value column) */
  isGroup: boolean;
}

function isPlainObject(v: unknown): v is Record<string, unknown> {
  return v !== null && typeof v === 'object' && !Array.isArray(v);
}

function formatInline(v: unknown): string {
  if (v === null || v === undefined) return 'null';
  if (Array.isArray(v))               return JSON.stringify(v);
  if (typeof v === 'object')          return JSON.stringify(v);
  return String(v);
}

/** Safe identifier as accepted by the server-side lexer (letter/digit/_/@). */
const IDENT_RE = /^[A-Za-z_@][A-Za-z0-9_@]*$/;

/** Numeric literal accepted by the filter lexer (int/decimal, optional sign). */
const NUMBER_RE = /^-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?$/;

/**
 * Convert the stringified property value (as produced by {@link formatInline})
 * back into a filter-language literal:
 *   - `null` / `true` / `false`           → as-is keyword
 *   - numeric                              → as-is
 *   - valid JSON object/array              → as-is (server may not support, but preserved)
 *   - anything else                        → single-quoted string with `'` escaped
 */
function toFilterLiteral(v: string): string {
  if (v === 'null' || v === 'true' || v === 'false') return v;
  if (NUMBER_RE.test(v)) return v;
  if ((v.startsWith('{') && v.endsWith('}')) || (v.startsWith('[') && v.endsWith(']'))) {
    try { JSON.parse(v); return v; } catch { /* fall through to string */ }
  }
  return `'${v.replace(/'/g, "''")}'`;
}

/** Append an array-index segment, e.g. `Tags` + 0 → `Tags[0]`. */
function joinIndex(prefix: string, index: number): string {
  return `${prefix}[${index}]`;
}

/**
 * Append a child segment to a property path, using bracket notation when the
 * segment contains characters the filter lexer cannot consume as an identifier
 * (hyphens, spaces, slashes, etc.).
 *   joinPath('Headers', 'Foo')               → 'Headers.Foo'
 *   joinPath('Headers', 'Api-Request-Id')    → "Headers['Api-Request-Id']"
 *   joinPath('',        'My-Top')            → "['My-Top']"
 */
function joinPath(prefix: string, key: string): string {
  if (IDENT_RE.test(key))
    return prefix ? `${prefix}.${key}` : key;
  const escaped = key.replace(/'/g, "\\'");
  return `${prefix}['${escaped}']`;
}

function flattenProps(obj: Record<string, unknown>, prefix = '', depth = 0, out: PropEntry[] = []): PropEntry[] {
  for (const [k, v] of Object.entries(obj)) {
    const path = joinPath(prefix, k);
    appendValue(path, k, v, depth, out);
  }
  return out;
}

/**
 * Recursively appends a single value at <paramref name="path"/> to the flattened
 * list. Plain objects become a group header plus children; arrays become a
 * group header plus indexed children (<c>Tags[0]</c>, <c>Tags[1]</c>, …); leaves
 * become a single row.
 */
function appendValue(path: string, label: string, v: unknown, depth: number, out: PropEntry[]): void {
  if (isPlainObject(v)) {
    out.push({ path, label, value: '', depth, isGroup: true });
    for (const [ck, cv] of Object.entries(v)) {
      appendValue(joinPath(path, ck), ck, cv, depth + 1, out);
    }
    return;
  }
  if (Array.isArray(v)) {
    out.push({ path, label, value: `[${v.length}]`, depth, isGroup: true });
    for (let i = 0; i < v.length; i++) {
      appendValue(joinIndex(path, i), String(i), v[i], depth + 1, out);
    }
    return;
  }
  out.push({ path, label, value: formatInline(v), depth, isGroup: false });
}

@Component({
  selector: 'app-event-row',
  imports: [LucideAngularModule, NgTemplateOutlet, DatePipe],
  providers: [DatePipe],
  templateUrl: './event-row.html',
  styleUrl: './event-row.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
  host: {
    '[class.ev-verbose]':     'levelKey() === "verbose"',
    '[class.ev-debug]':       'levelKey() === "debug"',
    '[class.ev-information]': 'levelKey() === "information"',
    '[class.ev-warning]':     'levelKey() === "warning"',
    '[class.ev-error]':       'levelKey() === "error"',
    '[class.ev-fatal]':       'levelKey() === "fatal"',
    '[class.ev-expanded]':    'expanded()',
    '[class.ev-wrap-mode]':   'wrap()',
  },
})
export class EventRowComponent {
  // ── Inputs / outputs ──────────────────────────────────────────────────
  event = input.required<EventDto>();
  wrap  = input<boolean>(false);
  filterSelected = output<string>();
  seekRequested  = output<{ from: Date; to: Date }>();

  // ── Local state ───────────────────────────────────────────────────────
  expanded  = signal(false);
  detailTab = signal<'props' | 'raw' | 'exception'>('props');
  menuType  = signal<'message' | 'level' | 'export' | 'prop' | null>(null);
  menuPos   = signal({ x: 0, y: 0 });
  selectedProp = signal<{ k: string; v: string; isGroup: boolean } | null>(null);
  /** Set of group paths currently collapsed (children hidden). */
  collapsed = signal<ReadonlySet<string>>(new Set());

  private sanitizer = inject(DomSanitizer);
  private datePipe  = inject(DatePipe);
  private overlay = inject(Overlay);
  private vcr = inject(ViewContainerRef);
  private overlayRef: OverlayRef | null = null;
  ctxMenuTpl = viewChild<TemplateRef<unknown>>('ctxMenuTpl');

  constructor() {
    // CDK virtual scroll recycles row instances across different events.
    // Reset local UI state whenever the bound event changes so an expanded /
    // menu-open state from a previous row does not leak onto a new one.
    effect(() => {
      this.event(); // track input identity
      untracked(() => {
        this.expanded.set(false);
        this.menuType.set(null);
        this.selectedProp.set(null);
        this.collapsed.set(new Set());
        this.detailTab.set('props');
      });
    });

    // Render the context menu through a CDK overlay so it escapes the
    // transformed virtual-scroll container (where `position: fixed` would
    // otherwise be positioned relative to the transformed ancestor).
    effect(() => {
      const type = this.menuType();
      const pos  = this.menuPos();
      const tpl  = this.ctxMenuTpl();
      untracked(() => this.syncOverlay(type, pos, tpl));
    });
  }

  private syncOverlay(
    type: 'message' | 'level' | 'export' | 'prop' | null,
    pos: { x: number; y: number },
    tpl: TemplateRef<unknown> | undefined,
  ): void {
    if (!type || !tpl) {
      this.overlayRef?.detach();
      return;
    }
    const positionStrategy = this.overlay.position()
      .global()
      .left(`${pos.x}px`)
      .top(`${pos.y}px`);
    if (!this.overlayRef) {
      this.overlayRef = this.overlay.create({
        positionStrategy,
        scrollStrategy: this.overlay.scrollStrategies.reposition(),
        panelClass: 'ctx-menu-overlay',
      });
    } else {
      this.overlayRef.updatePositionStrategy(positionStrategy);
    }
    if (!this.overlayRef.hasAttached()) {
      this.overlayRef.attach(new TemplatePortal(tpl, this.vcr));
    }
  }

  ngOnDestroy(): void {
    this.overlayRef?.dispose();
    this.overlayRef = null;
  }

  // ── Derived ───────────────────────────────────────────────────────────
  levelKey = computed(() => (this.event()['@l'] ?? 'information').toLowerCase());

  levelShort = computed(() => LEVEL_SHORT[this.levelKey()] ?? this.levelKey().slice(0, 3).toUpperCase());

  service = computed(() => (this.event().props?.['ApplicationContext'] as string) ?? '');

  // timestamp = computed(() => {
  //   const raw = this.event()['@t'];
  //   try {
  //     const d = new Date(raw);
  //     const datePart = this.datePipe.transform(d, 'MM-dd HH:mm:ss') ?? raw;
  //     const fracMatch = raw.match(/\.(\d+)/);
  //     const ms = fracMatch ? fracMatch[1].slice(0, 3).padEnd(3, '0') : '000';
  //     return `${datePart}.${ms}`;
  //   } catch { return raw; }
  // });

  renderedHtml = computed(() =>
    this.sanitizer.bypassSecurityTrustHtml(
      renderMessageHtml(this.event()['@mt'], this.event().props)
    )
  );

  rawJson = computed(() => JSON.stringify(this.event(), null, 2));

  topProps = computed(() =>
    Object.entries(this.event().props ?? {})
      .slice(0, 3)
      .map(([k, v]) => ({ k, v: formatInline(v) }))
  );

  /**
   * Flattened property tree:
   *  - Plain values   → leaf row { path:'A.B', label:'B', value:'42', depth, isGroup:false }
   *  - Plain objects  → group header row + recursive children
   *  - Arrays / null  → leaf row (arrays serialized as JSON; array search not yet supported server-side)
   * Children of collapsed group rows are hidden.
   */
  allProps = computed(() => {
    const all = flattenProps(this.event().props ?? {});
    const col = this.collapsed();
    if (col.size === 0) return all;
    return all.filter(e => {
      for (const c of col) {
        if (e.path === c) continue;
        // Children appear under `Parent.Child`, `Parent['Child']`, or `Parent[0]`.
        if (e.path.startsWith(c + '.') ||
            e.path.startsWith(c + '[')) return false;
      }
      return true;
    });
  });

  isCollapsed(path: string): boolean { return this.collapsed().has(path); }

  toggleGroup(e: MouseEvent, path: string): void {
    e.stopPropagation();
    this.collapsed.update(s => {
      const ns = new Set(s);
      if (ns.has(path)) ns.delete(path); else ns.add(path);
      return ns;
    });
  }

  propsCount   = computed(() => Object.keys(this.event().props ?? {}).length);
  hasException  = computed(() => !!this.event()['@x']);

  // ── Interaction ───────────────────────────────────────────────────────
  toggleExpand(e: Event): void {
    e.stopPropagation();
    if (this.menuType()) { this.menuType.set(null); return; }
    const opening = !this.expanded();
    this.expanded.update(v => !v);
    if (opening && this.hasException()) this.detailTab.set('exception');
    else if (opening)                   this.detailTab.set('props');
  }

  openPropMenu(e: MouseEvent, k: string, v: string, isGroup = false): void {
    e.stopPropagation();
    e.preventDefault();
    this.selectedProp.set({ k, v, isGroup });
    let x = e.clientX;
    let y = e.clientY + 6;
    if (x + 210 > window.innerWidth)  x = e.clientX - 210;
    if (y + 180 > window.innerHeight) y = e.clientY - 180;
    this.menuType.set('prop');
    this.menuPos.set({ x, y });
  }

  openMenu(e: MouseEvent, type: 'message' | 'level' | 'export'): void {
    e.stopPropagation();
    e.preventDefault();
    let x = e.clientX;
    let y = e.clientY + 6;
    if (x + 210 > window.innerWidth)  x = e.clientX - 210;
    if (y + 260 > window.innerHeight) y = e.clientY - 260;
    this.menuType.set(type);
    this.menuPos.set({ x, y });
  }

  @HostListener('document:click')
  @HostListener('document:keydown')
  onCloseMenu(): void {
    if (this.menuType()) this.menuType.set(null);
  }

  // ── Context menu actions ──────────────────────────────────────────────
  findSimilar(): void {
    const tmpl = this.event()['@mt'] ?? '';
    this.filterSelected.emit(`@mt = '${tmpl.replace(/'/g, "''")}'`);
    this.menuType.set(null);
  }

  findFrom(): void {
    this.seekRequested.emit({ from: new Date(this.event()['@t']), to: new Date() });
    this.menuType.set(null);
  }

  findTo(): void {
    this.seekRequested.emit({ from: new Date(0), to: new Date(this.event()['@t']) });
    this.menuType.set(null);
  }

  seek(seconds: number): void {
    const ms = new Date(this.event()['@t']).getTime();
    this.seekRequested.emit({
      from: new Date(ms - seconds * 1000),
      to:   new Date(ms + seconds * 1000),
    });
    this.menuType.set(null);
  }

  filterLevel(): void {
    this.filterSelected.emit(`@l = '${this.event()['@l']}'`);
    this.menuType.set(null);
  }

  excludeLevel(): void {
    this.filterSelected.emit(`@l <> '${this.event()['@l']}'`);
    this.menuType.set(null);
  }

  async copyMessage(): Promise<void> {
    const tmp = document.createElement('div');
    tmp.innerHTML = this.renderedHtml() as string;
    await navigator.clipboard.writeText(tmp.textContent ?? '');
    this.menuType.set(null);
  }

  async copyRaw(): Promise<void> {
    await navigator.clipboard.writeText(JSON.stringify(this.event()));
    this.menuType.set(null);
  }

  copyLink(): void {
    const url = `${window.location.origin}/events?id=${encodeURIComponent(this.event().id ?? '')}`;
    navigator.clipboard.writeText(url);
    this.menuType.set(null);
  }

  propFind(): void {
    const p = this.selectedProp();
    if (!p) return;
    if (p.isGroup) {
      this.filterSelected.emit(`has(${p.k})`);
    } else {
      this.filterSelected.emit(`${p.k} = ${toFilterLiteral(p.v)}`);
    }
    this.menuType.set(null);
  }

  async propCopy(): Promise<void> {
    const p = this.selectedProp();
    if (!p) return;
    await navigator.clipboard.writeText(p.isGroup ? p.k : p.v);
    this.menuType.set(null);
  }

  propExclude(): void {
    const p = this.selectedProp();
    if (!p) return;
    if (p.isGroup) {
      this.filterSelected.emit(`not has(${p.k})`);
    } else {
      this.filterSelected.emit(`${p.k} <> ${toFilterLiteral(p.v)}`);
    }
    this.menuType.set(null);
  }

  protected readonly escapeHtmlExport = escapeHtmlExport;
}
