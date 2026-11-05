import {
  Component, signal, computed, inject, effect, viewChild, ElementRef,
  ChangeDetectionStrategy,
} from '@angular/core';
import { NgTemplateOutlet } from '@angular/common';
import { LucideAngularModule } from 'lucide-angular';
import { DomSanitizer, SafeHtml } from '@angular/platform-browser';

import { highlightFilterExpression } from '../../../../shared/utils/filter-highlight';
import { SearchHistoryService } from '../../../../core/services/search-history.service';
import { ViewportService } from '../../../../core/services/viewport.service';
import { EventsStore } from '../../store/events.store';
import { BUILTIN_SUGGESTIONS, LEVEL_ITEMS, currentPrefix } from '../../store/events-filter.util';

/** The phone's suggestion strip holds this many chips; the rest is a letter away. */
const PHONE_SUGGEST_MAX = 12;
/** The phone's recent-searches list under the field: pinned first, five rows in all. */
const PHONE_RECENT_MAX = 5;

/**
 * Top toolbar: the syntax-highlighted filter textarea with autocomplete, plus the
 * Apply / Clear / Live / Signals actions. Reads and writes the shared {@link EventsStore};
 * owns only the DOM-bound autocomplete state.
 *
 * <p>On a phone (design canvas, "Логи на телефоне") the same component renders the page's top
 * instead: a 48px header — "Logs", the range button that opens the filter sheet, the Live
 * switch, the Signals bell — then the search row with the same textarea and a "Filters · N"
 * button, then the strip of level chips that the filter bar shows wider. While the field has
 * focus the strip gives way to suggestion chips, and the recent searches open under them.</p>
 */
@Component({
  selector: 'app-events-toolbar',
  imports: [LucideAngularModule, NgTemplateOutlet],
  templateUrl: './events-toolbar.html',
  styleUrls: ['./events-toolbar.scss', './events-toolbar.responsive.scss'],
  host: { style: 'display: contents' },
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class EventsToolbarComponent {
  readonly store = inject(EventsStore);
  readonly viewport = inject(ViewportService);
  private sanitizer = inject(DomSanitizer);
  private readonly history = inject(SearchHistoryService).forScope('logs');

  private filterRef = viewChild<ElementRef<HTMLTextAreaElement>>('filterEl');

  readonly phone = this.viewport.phone;
  readonly levelItems = LEVEL_ITEMS;

  suggestOpen  = signal(false);
  suggestIndex = signal(0);
  private suggestPrefix = signal('');

  /** Items shown in the suggestion popup, filtered by the current prefix. */
  suggestItems = computed<string[]>(() => {
    if (!this.suggestOpen()) return [];
    const all = [...BUILTIN_SUGGESTIONS, ...this.store.knownPropPaths()];
    const q = this.suggestPrefix().toLowerCase();
    return (q ? all.filter(s => s.toLowerCase().includes(q)) : all).slice(0, 40);
  });

  /** The phone's strip: the popup's items, as many as a strip can take. */
  readonly phoneSuggestItems = computed(() => this.suggestItems().slice(0, PHONE_SUGGEST_MAX));

  /** Syntax-highlighted HTML mirrored under the filter textarea. */
  filterHighlight = computed<SafeHtml>(() =>
    this.sanitizer.bypassSecurityTrustHtml(highlightFilterExpression(this.store.filterInput())),
  );
  /** Auto-grow the textarea with the number of newlines — on a phone to three lines at most. */
  filterRows = computed(() => {
    const lines = Math.max(1, this.store.filterInput().split('\n').length);
    return this.phone() ? Math.min(3, lines) : lines;
  });

  /** The phone's field has focus: the suggestion strip and the recent searches are out. */
  readonly phoneFocused = signal(false);

  /** The phone's range button: the preset's short name, or Custom (the Traces header's rule). */
  readonly phoneRangeLabel = computed(() => {
    const p = this.store.timePreset();
    return p === 'custom' ? 'Custom' : p;
  });

  /** The recent searches under the phone's field: pinned first, then recent, five in all. */
  readonly phoneRecent = computed(() => {
    const pinned = this.history.pinned().map(q => ({ q, pinned: true }));
    const recent = this.history.recent().map(q => ({ q, pinned: false }));
    return [...pinned, ...recent].slice(0, PHONE_RECENT_MAX);
  });

  readonly phoneRecentSummary = computed(() => {
    const p = this.history.pinned().length;
    const r = this.history.recent().length;
    return `${p} pinned · ${r} recent`;
  });

  constructor() {
    // One-way sync signal → textarea. Only write on an actual diff, otherwise
    // we'd reset the caret on every keystroke.
    effect(() => {
      const v  = this.store.filterInput();
      const el = this.filterRef()?.nativeElement;
      if (el && el.value !== v) el.value = v;
    });
  }

  /** Enter → search (or accept suggestion); Ctrl+Space → open popup; ↑/↓/Tab/Esc → navigate. */
  onFilterKeydown(e: KeyboardEvent): void {
    if (e.ctrlKey && e.code === 'Space') { e.preventDefault(); this.openSuggest(); return; }

    // The phone's keyboard has a Search key and no arrows: Enter runs the search whatever the
    // strip is showing (a chip is a tap away), and Escape puts the keyboard down.
    if (this.phone()) {
      if (e.key === 'Enter' && !e.ctrlKey && !e.shiftKey && !e.altKey && !e.metaKey) {
        e.preventDefault();
        this.store.search();
        this.filterRef()?.nativeElement.blur();
      } else if (e.key === 'Escape') {
        e.preventDefault();
        this.filterRef()?.nativeElement.blur();
      }
      return;
    }

    if (this.suggestOpen()) {
      const items = this.suggestItems();
      if (e.key === 'ArrowDown') {
        e.preventDefault();
        if (items.length) this.suggestIndex.update(i => (i + 1) % items.length);
        return;
      }
      if (e.key === 'ArrowUp') {
        e.preventDefault();
        if (items.length) this.suggestIndex.update(i => (i - 1 + items.length) % items.length);
        return;
      }
      if (e.key === 'Escape') { e.preventDefault(); this.closeSuggest(); return; }
      if (e.key === 'Enter' || e.key === 'Tab') {
        const pick = items[this.suggestIndex()];
        if (pick) { e.preventDefault(); this.applySuggestion(pick); return; }
      }
    }

    if (e.key === 'Enter' && !e.ctrlKey && !e.shiftKey && !e.altKey && !e.metaKey) {
      e.preventDefault();
      this.store.search();
    }
  }

  onFilterInput(value: string): void {
    this.store.setFilterInput(value);
    const el = this.filterRef()?.nativeElement;
    const caret = el?.selectionStart ?? value.length;
    if (this.phone()) {
      // The strip opens by itself while an identifier is being typed, and goes when it is done.
      const prefix = currentPrefix(value, caret);
      this.suggestPrefix.set(prefix);
      this.suggestIndex.set(0);
      this.suggestOpen.set(prefix.length > 0);
      return;
    }
    if (this.suggestOpen()) {
      this.suggestPrefix.set(currentPrefix(value, caret));
      this.suggestIndex.set(0);
    }
  }

  onFilterFocus(): void {
    if (!this.phone()) return;
    this.phoneFocused.set(true);
    this.history.load();
  }

  onFilterBlur(): void {
    this.closeSuggest();
    this.phoneFocused.set(false);
  }

  selectSuggestion(item: string): void {
    this.applySuggestion(item);
  }

  /** The phone's keyword chips and the ↵ chip: text at the caret, the field kept in focus. */
  insertText(text: string): void {
    const el = this.filterRef()?.nativeElement;
    if (!el) return;
    const start = el.selectionStart ?? el.value.length;
    const end = el.selectionEnd ?? start;
    const next = el.value.slice(0, start) + text + el.value.slice(end);
    el.value = next;
    const caret = start + text.length;
    el.setSelectionRange(caret, caret);
    this.store.setFilterInput(next);
    this.suggestOpen.set(false);
    el.focus();
  }

  /** A recent search tapped on the phone: it runs, and the keyboard goes. */
  applyRecent(query: string): void {
    this.store.applyFilter(query);
    this.filterRef()?.nativeElement.blur();
  }

  pinRecent(query: string, pinned: boolean, e: Event): void {
    e.stopPropagation();
    this.history.setPinned(query, pinned);
  }

  removeRecent(query: string, e: Event): void {
    e.stopPropagation();
    this.history.remove(query);
  }

  /** The phone field's × : the same as the toolbar's Clear filter. */
  clearFilter(): void {
    this.store.applyFilter('');
  }

  closeSuggest(): void {
    if (this.suggestOpen()) this.suggestOpen.set(false);
  }

  fmtCount(n: number | undefined): string {
    if (!n) return '0';
    return n >= 1000 ? `${(n / 1000).toFixed(1)}K` : String(n);
  }

  isLevelActive(level: string): boolean {
    return this.store.activeLevels().has(level);
  }

  private openSuggest(): void {
    const el = this.filterRef()?.nativeElement;
    if (!el) return;
    const caret = el.selectionStart ?? el.value.length;
    this.suggestPrefix.set(currentPrefix(el.value, caret));
    this.suggestIndex.set(0);
    this.suggestOpen.set(true);
  }

  private applySuggestion(item: string): void {
    const el = this.filterRef()?.nativeElement;
    if (!el) { this.closeSuggest(); return; }
    const caret = el.selectionStart ?? el.value.length;
    const prefix = currentPrefix(el.value, caret);
    const start  = caret - prefix.length;
    const next   = el.value.slice(0, start) + item + el.value.slice(caret);
    el.value = next;
    const newCaret = start + item.length;
    el.setSelectionRange(newCaret, newCaret);
    this.store.setFilterInput(next);
    this.closeSuggest();
    el.focus();
  }
}
