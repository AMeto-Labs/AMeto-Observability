import {
  Component, signal, computed, inject, effect, viewChild, ElementRef,
  ChangeDetectionStrategy,
} from '@angular/core';
import { LucideAngularModule } from 'lucide-angular';
import { DomSanitizer, SafeHtml } from '@angular/platform-browser';

import { highlightFilterExpression } from '../../../../shared/utils/filter-highlight';
import { EventsStore } from '../../store/events.store';
import { BUILTIN_SUGGESTIONS, currentPrefix } from '../../store/events-filter.util';

/**
 * Top toolbar: the syntax-highlighted filter textarea with autocomplete, plus the
 * Apply / Clear / Live / Signals actions. Reads and writes the shared {@link EventsStore};
 * owns only the DOM-bound autocomplete state.
 */
@Component({
  selector: 'app-events-toolbar',
  imports: [LucideAngularModule],
  templateUrl: './events-toolbar.html',
  styleUrl: './events-toolbar.scss',
  host: { style: 'display: contents' },
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class EventsToolbarComponent {
  readonly store = inject(EventsStore);
  private sanitizer = inject(DomSanitizer);

  private filterRef = viewChild<ElementRef<HTMLTextAreaElement>>('filterEl');

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

  /** Syntax-highlighted HTML mirrored under the filter textarea. */
  filterHighlight = computed<SafeHtml>(() =>
    this.sanitizer.bypassSecurityTrustHtml(highlightFilterExpression(this.store.filterInput())),
  );
  /** Auto-grow the textarea with the number of newlines. */
  filterRows = computed(() => Math.max(1, this.store.filterInput().split('\n').length));

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
    if (this.suggestOpen()) {
      const el = this.filterRef()?.nativeElement;
      const caret = el?.selectionStart ?? value.length;
      this.suggestPrefix.set(currentPrefix(value, caret));
      this.suggestIndex.set(0);
    }
  }

  selectSuggestion(item: string): void {
    this.applySuggestion(item);
  }

  closeSuggest(): void {
    if (this.suggestOpen()) this.suggestOpen.set(false);
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
