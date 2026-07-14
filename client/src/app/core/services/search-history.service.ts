import { Injectable, inject, signal } from '@angular/core';
import { ApiService } from './api.service';

const RECENT_MAX = 10;
const PINNED_MAX = 5;

/**
 * Per-user saved-search state (pinned + recent), backed by the server
 * (`/api/search-history`). Mutations update the local signals optimistically and
 * fire-and-forget the request; the panel calls {@link load} on open to reconcile.
 */
@Injectable({ providedIn: 'root' })
export class SearchHistoryService {
  private api = inject(ApiService);

  private readonly _pinned = signal<string[]>([]);
  private readonly _recent = signal<string[]>([]);
  readonly pinned = this._pinned.asReadonly();
  readonly recent = this._recent.asReadonly();

  load(): void {
    this.api.getSearchHistory().subscribe({
      next: h => { this._pinned.set(h.pinned ?? []); this._recent.set(h.recent ?? []); },
      error: () => { /* keep whatever we have */ },
    });
  }

  /** Records a deliberately-run query (skips blanks; pinned queries stay pinned). */
  record(query: string): void {
    const q = query.trim();
    if (!q) return;
    if (!this._pinned().includes(q)) {
      this._recent.update(r => [q, ...r.filter(x => x !== q)].slice(0, RECENT_MAX));
    }
    this.api.recordSearch(q).subscribe({ error: () => { /* best-effort */ } });
  }

  setPinned(query: string, pinned: boolean): void {
    const q = query.trim();
    if (!q) return;
    if (pinned) {
      this._recent.update(r => r.filter(x => x !== q));
      this._pinned.update(p => [q, ...p.filter(x => x !== q)].slice(0, PINNED_MAX));
    } else {
      this._pinned.update(p => p.filter(x => x !== q));
      this._recent.update(r => [q, ...r.filter(x => x !== q)].slice(0, RECENT_MAX));
    }
    this.api.pinSearch(q, pinned).subscribe({ error: () => this.load() });
  }

  remove(query: string): void {
    const q = query.trim();
    if (!q) return;
    this._pinned.update(p => p.filter(x => x !== q));
    this._recent.update(r => r.filter(x => x !== q));
    this.api.deleteSearch(q).subscribe({ error: () => this.load() });
  }
}
