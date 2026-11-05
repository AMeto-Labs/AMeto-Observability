import {
  Component, ChangeDetectionStrategy, computed, effect, inject, input, output,
  untracked, viewChild, TemplateRef, ViewContainerRef,
} from '@angular/core';
import { DatePipe } from '@angular/common';
import { DomSanitizer } from '@angular/platform-browser';
import { LucideAngularModule } from 'lucide-angular';

import { EventDto, levelShort } from '../../../../core/models/event.model';
import { renderMessageHtml } from '../../../../shared/utils/clef-renderer';
import { LongPress } from '../../../../shared/utils/long-press';
import { ContextMenuService, OverlayPanelRef } from '../../../../shared/services/overlay';
import { excludeLikeFilter, findLikeFilter } from '../../store/events-filter.util';

/**
 * Fixed-height, single-line collapsed log row for the Events virtual list.
 *
 * Purely presentational + selection: a left click asks the parent to open the
 * detail drawer ({@link selectRequested}); right-clicking the message offers a
 * lightweight "find similar" filter ({@link filterSelected}). All inline detail,
 * trace/metrics/JSON tabs and heavyweight menus live in the drawer, not here.
 *
 * By default the host renders at a fixed --log-row-h (24px) and clips overflow so
 * heights stay uniform for virtual scrolling. In {@link wrap} mode the host grows to
 * fit the full message across multiple lines (the parent measures each wrapped row
 * instead of assuming a height).
 *
 * On a phone ({@link phone}) the row is two lines — the level badge, the time and the service
 * over the message clamped to two lines — and is measured like a wrapped row; the date leaves
 * the row for the list's day separators. On a touch screen ({@link touch}) a long press on the
 * row asks the parent for the message actions ({@link messageActionsRequested}) in place of the
 * right-click menu, which has no cursor to open at.
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
    '[class.ev-phone]':       'phone()',
    '[class.ev-dateless]':    'dateless()',
    '[class.ev-touch]':       'touch()',
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
  /** The phone's two-line row (design canvas, Logs on a phone). The parent measures it. */
  readonly phone = input<boolean>(false);
  /** The tablet's table: the TIME column holds the time alone, the date stays in its title. */
  readonly dateless = input<boolean>(false);
  /** A touch screen: a long press opens the message actions; the right-click menu is not offered. */
  readonly touch = input<boolean>(false);

  // ── Outputs ───────────────────────────────────────────────────────────
  /** Row clicked — parent opens the detail drawer for this event. */
  readonly selectRequested = output<void>();
  /** Quick filter picked from the message context menu (a CLEF filter expression). */
  readonly filterSelected = output<string>();
  /** A long press on a touch screen: the parent shows the message action sheet for this event. */
  readonly messageActionsRequested = output<void>();

  private readonly sanitizer = inject(DomSanitizer);
  private readonly contextMenu = inject(ContextMenuService);
  private readonly vcr = inject(ViewContainerRef);

  private readonly ctxMenuTpl = viewChild<TemplateRef<unknown>>('ctxMenuTpl');
  private menuRef?: OverlayPanelRef;

  /** The long press, on a touch screen. A press that fires swallows the click its release makes. */
  private readonly press = new LongPress(() => this.messageActionsRequested.emit());

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
    this.press.end();
  }

  // ── Derived view state ────────────────────────────────────────────────
  readonly levelKey = computed(() => (this.event()['@l'] ?? 'information').toLowerCase());

  readonly levelShort = computed(() => levelShort(this.event()['@l']));

  readonly service = computed(() => this.event()['@service'] ?? '');

  readonly renderedHtml = computed(() =>
    this.sanitizer.bypassSecurityTrustHtml(
      renderMessageHtml(this.event()['@mt'], this.event().props),
    ));

  // ── Interaction ───────────────────────────────────────────────────────
  onRowClick(): void {
    // The release after a long press clicks too; that press has already been answered.
    if (this.press.consumeFired()) return;
    this.selectRequested.emit();
  }

  onPointerDown(e: PointerEvent): void {
    if (!this.touch() || !e.isPrimary) return;
    this.press.start(e.clientX, e.clientY);
  }

  onPointerMove(e: PointerEvent): void {
    if (this.touch()) this.press.move(e.clientX, e.clientY);
  }

  onPointerEnd(): void {
    this.press.end();
  }

  /**
   * A long press on Android raises `contextmenu` as well; on a touch screen that menu is the
   * browser's text-selection one, and the sheet the press opens replaces it.
   */
  onRowContextMenu(e: MouseEvent): void {
    if (this.touch()) e.preventDefault();
  }

  /** Right-click on the message → open the lightweight quick-filter menu. */
  openMessageMenu(e: MouseEvent): void {
    e.preventDefault();
    e.stopPropagation();
    // No cursor to anchor to: the long press has opened, or is about to open, the sheet.
    if (this.touch()) return;
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

  /** The sheet's expressions (events-filter.util): the template quoted as the lexer reads it. */
  findSimilar(): void {
    this.emitFilter(findLikeFilter(this.event()['@mt']));
  }

  /** In place of the current filter, as this menu always did; the phone's sheet narrows it instead. */
  excludeSimilar(): void {
    this.emitFilter(excludeLikeFilter(this.event()['@mt'], ''));
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
