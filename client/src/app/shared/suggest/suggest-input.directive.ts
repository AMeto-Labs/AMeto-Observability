import {
  ComponentRef, DestroyRef, Directive, ElementRef, HostListener, Renderer2,
  ViewContainerRef, computed, effect, inject, input, output, signal,
} from '@angular/core';
import { SuggestPopupComponent } from './suggest-popup';

/** Token under the caret that the popup completes: identifiers, dotted paths and a leading `.`/`@`. */
const DEFAULT_PREFIX_RE = /[.@A-Za-z_][A-Za-z0-9_.]*$/;

/**
 * Ctrl+Space autocomplete for any text `<input>` / `<textarea>`, mirroring the Events
 * filter: press Ctrl+Space to open a popup of candidates, filtered by the token under the
 * caret; ↑/↓ navigate, Enter/Tab accept, Esc closes. Accepting splices the pick over the
 * current token and syncs the bound model via a native `input` event.
 *
 * The directive fully owns Enter while the popup is open, and otherwise emits
 * {@link suggestSubmit} — so hosts wire their "run" action to `(suggestSubmit)` instead of
 * their own Enter binding, avoiding a same-element keydown race between accept-and-run.
 */
@Directive({
  selector: '[appSuggestInput]',
})
export class SuggestInputDirective {
  private readonly host = inject<ElementRef<HTMLInputElement | HTMLTextAreaElement>>(ElementRef);
  private readonly vcr = inject(ViewContainerRef);
  private readonly renderer = inject(Renderer2);

  /** Full candidate list; the directive filters it by the token under the caret. */
  readonly items = input<string[]>([], { alias: 'appSuggestInput' });
  /** Token matcher — override for grammars whose tokens differ from the default. */
  readonly prefixRe = input<RegExp>(DEFAULT_PREFIX_RE, { alias: 'suggestPrefixRe' });
  /** Fired on Enter when no suggestion is accepted (host runs its query/search here). */
  readonly suggestSubmit = output<void>();

  private readonly open = signal(false);
  private readonly index = signal(0);
  private readonly prefix = signal('');

  /** Candidates for the current prefix (case-insensitive substring), capped for the popup. */
  private readonly shown = computed<string[]>(() => {
    if (!this.open()) return [];
    const q = this.prefix().toLowerCase();
    const all = this.items();
    return (q ? all.filter(s => s.toLowerCase().includes(q)) : all).slice(0, 40);
  });

  private popup?: ComponentRef<SuggestPopupComponent>;

  constructor() {
    // Reflect state into the popup: create/destroy with visibility, push items + active
    // index, and reposition under the input on every change while open.
    effect(() => {
      const items = this.shown();
      if (this.open() && items.length) {
        this.ensurePopup();
        this.popup!.setInput('items', items);
        this.popup!.setInput('activeIndex', this.index());
        this.position();
      } else {
        this.destroyPopup();
      }
    });
    inject(DestroyRef).onDestroy(() => this.destroyPopup());
  }

  @HostListener('keydown', ['$event'])
  onKeydown(e: KeyboardEvent): void {
    if (e.ctrlKey && e.code === 'Space') { e.preventDefault(); this.openAtCaret(); return; }

    if (e.key === 'Enter') {
      // The directive owns Enter so the host's run-on-Enter can't fire mid-accept.
      const pick = this.open() ? this.shown()[this.index()] : undefined;
      e.preventDefault();
      if (pick) { this.apply(pick); }
      else { this.close(); this.suggestSubmit.emit(); }
      return;
    }

    if (!this.open()) return;
    const items = this.shown();
    switch (e.key) {
      case 'ArrowDown':
        e.preventDefault();
        if (items.length) this.index.update(i => (i + 1) % items.length);
        return;
      case 'ArrowUp':
        e.preventDefault();
        if (items.length) this.index.update(i => (i - 1 + items.length) % items.length);
        return;
      case 'Escape':
        e.preventDefault();
        this.close();
        return;
      case 'Tab': {
        const pick = items[this.index()];
        if (pick) { e.preventDefault(); this.apply(pick); }
        return;
      }
    }
  }

  @HostListener('input')
  onInput(): void {
    if (!this.open()) return;
    const el = this.host.nativeElement;
    const caret = el.selectionStart ?? el.value.length;
    this.prefix.set(this.currentPrefix(el.value, caret));
    this.index.set(0);
  }

  @HostListener('blur')
  onBlur(): void {
    this.close();
  }

  // ── Internals ─────────────────────────────────────────────────────────────
  private openAtCaret(): void {
    const el = this.host.nativeElement;
    const caret = el.selectionStart ?? el.value.length;
    this.prefix.set(this.currentPrefix(el.value, caret));
    this.index.set(0);
    this.open.set(true);
  }

  private close(): void {
    if (this.open()) this.open.set(false);
  }

  private apply(item: string): void {
    const el = this.host.nativeElement;
    const caret = el.selectionStart ?? el.value.length;
    const prefix = this.currentPrefix(el.value, caret);
    const start = caret - prefix.length;
    el.value = el.value.slice(0, start) + item + el.value.slice(caret);
    const newCaret = start + item.length;
    el.setSelectionRange(newCaret, newCaret);
    // Sync the bound model (ngModel / signal) — DefaultValueAccessor listens to `input`.
    el.dispatchEvent(new Event('input', { bubbles: true }));
    this.close();
    el.focus();
  }

  private currentPrefix(value: string, caret: number): string {
    const m = value.slice(0, caret).match(this.prefixRe());
    return m ? m[0] : '';
  }

  private ensurePopup(): void {
    if (this.popup) return;
    this.popup = this.vcr.createComponent(SuggestPopupComponent);
    this.popup.instance.pick.subscribe((i: number) => {
      const item = this.shown()[i];
      if (item) this.apply(item);
    });
    this.popup.instance.hover.subscribe((i: number) => this.index.set(i));
  }

  private destroyPopup(): void {
    this.popup?.destroy();
    this.popup = undefined;
  }

  private position(): void {
    if (!this.popup) return;
    const r = this.host.nativeElement.getBoundingClientRect();
    const root = this.popup.location.nativeElement as HTMLElement;
    this.renderer.setStyle(root, 'left', `${r.left}px`);
    this.renderer.setStyle(root, 'top', `${r.bottom + 4}px`);
    this.renderer.setStyle(root, 'min-width', `${r.width}px`);
  }
}
