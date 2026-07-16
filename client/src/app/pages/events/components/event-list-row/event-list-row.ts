import {
  Component, ChangeDetectionStrategy, computed, effect, inject, input, output,
  untracked, viewChild, TemplateRef, ViewContainerRef,
} from '@angular/core';
import { DatePipe } from '@angular/common';
import { DomSanitizer } from '@angular/platform-browser';
import { LucideAngularModule } from 'lucide-angular';

import { EventDto } from '../../../../core/models/event.model';
import { renderMessageHtml } from '../../../../shared/utils/clef-renderer';
import { ContextMenuService, OverlayPanelRef } from '../../../../shared/services/overlay';

const LEVEL_SHORT: Record<string, string> = {
  verbose: 'VRB', debug: 'DBG', information: 'INF',
  warning: 'WRN', error: 'ERR', fatal: 'FTL',
};

/** Render a top-level prop value to a compact single-line string for the row hint. */
function formatInline(v: unknown): string {
  if (v === null || v === undefined) return 'null';
  if (Array.isArray(v))               return JSON.stringify(v);
  if (typeof v === 'object')          return JSON.stringify(v);
  return String(v);
}

/**
 * Fixed-height, single-line collapsed log row for the Events virtual list.
 *
 * Purely presentational + selection: a left click asks the parent to open the
 * detail drawer ({@link selectRequested}); right-clicking the message offers a
 * lightweight "find similar" filter ({@link filterSelected}). All inline detail,
 * trace/metrics/JSON tabs and heavyweight menus live in the drawer, not here.
 *
 * By default the host renders at a fixed 29px and clips overflow so heights stay
 * uniform for virtual scrolling. In {@link wrap} mode the host grows to fit the
 * full message across multiple lines (the parent renders wrapped rows
 * non-virtualized, since variable heights can't be virtualized at fixed size).
 */
@Component({
  selector: 'app-event-list-row',
  imports: [LucideAngularModule, DatePipe],
  templateUrl: './event-list-row.html',
  styleUrl: './event-list-row.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
  host: {
    '[class.ev-verbose]':     'levelKey() === "verbose"',
    '[class.ev-debug]':       'levelKey() === "debug"',
    '[class.ev-information]': 'levelKey() === "information"',
    '[class.ev-warning]':     'levelKey() === "warning"',
    '[class.ev-error]':       'levelKey() === "error"',
    '[class.ev-fatal]':       'levelKey() === "fatal"',
    '[class.selected]':       'selected()',
    '[class.ev-wrap-mode]':   'wrap()',
    '[class.is-new]':         'isNew()',
  },
})
export class EventListRowComponent {
  // ── Inputs ────────────────────────────────────────────────────────────
  readonly event = input.required<EventDto>();
  /** Highlight when this row is the one currently open in the detail drawer. */
  readonly selected = input<boolean>(false);
  /** Wrap the message across multiple lines (the row grows to fit) instead of truncating. */
  readonly wrap = input<boolean>(false);
  /** True for ~1s after the event arrives on the live tail — plays a highlight flash. */
  readonly isNew = input<boolean>(false);

  // ── Outputs ───────────────────────────────────────────────────────────
  /** Row clicked — parent opens the detail drawer for this event. */
  readonly selectRequested = output<void>();
  /** Quick filter picked from the message context menu (a CLEF filter expression). */
  readonly filterSelected = output<string>();

  private readonly sanitizer = inject(DomSanitizer);
  private readonly contextMenu = inject(ContextMenuService);
  private readonly vcr = inject(ViewContainerRef);

  private readonly ctxMenuTpl = viewChild<TemplateRef<unknown>>('ctxMenuTpl');
  private menuRef?: OverlayPanelRef;

  constructor() {
    // CDK virtual scroll recycles this row instance across events — close any
    // stale menu when the bound event changes so it can't leak onto a new row.
    effect(() => {
      this.event();
      untracked(() => this.closeMenu());
    });
  }

  ngOnDestroy(): void {
    this.closeMenu();
  }

  // ── Derived view state ────────────────────────────────────────────────
  readonly levelKey = computed(() => (this.event()['@l'] ?? 'information').toLowerCase());

  readonly levelShort = computed(() =>
    LEVEL_SHORT[this.levelKey()] ?? this.levelKey().slice(0, 3).toUpperCase());

  readonly service = computed(() =>
    (this.event()['service.name'] as string | undefined) ?? '');

  readonly renderedHtml = computed(() =>
    this.sanitizer.bypassSecurityTrustHtml(
      renderMessageHtml(this.event()['@mt'], this.event().props),
    ));

  readonly topProps = computed(() =>
    Object.entries(this.event().props ?? {})
      .slice(0, 3)
      .map(([k, v]) => ({ k, v: formatInline(v) })));

  readonly propsCount = computed(() => Object.keys(this.event().props ?? {}).length);

  // ── Interaction ───────────────────────────────────────────────────────
  onRowClick(): void {
    this.selectRequested.emit();
  }

  /** Right-click on the message → open the lightweight quick-filter menu. */
  openMessageMenu(e: MouseEvent): void {
    e.preventDefault();
    e.stopPropagation();
    const tpl = this.ctxMenuTpl();
    if (!tpl) return;

    let x = e.clientX;
    let y = e.clientY + 6;
    if (x + 220 > window.innerWidth)  x = e.clientX - 220;
    if (y + 140 > window.innerHeight) y = e.clientY - 140;

    this.menuRef = this.contextMenu.open({
      template: tpl,
      viewContainerRef: this.vcr,
      x, y,
      panelClass: 'ctx-menu-overlay',
    });
  }

  findSimilar(): void {
    this.emitFilter(`@mt = '${this.messageTemplate()}'`);
  }

  excludeSimilar(): void {
    this.emitFilter(`@mt <> '${this.messageTemplate()}'`);
  }

  private messageTemplate(): string {
    return (this.event()['@mt'] ?? '').replace(/'/g, "''");
  }

  private emitFilter(expr: string): void {
    this.filterSelected.emit(expr);
    this.closeMenu();
  }

  private closeMenu(): void {
    this.menuRef?.close();
    this.menuRef = undefined;
  }
}
