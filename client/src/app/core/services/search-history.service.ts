import { Injectable, Signal, inject, signal } from '@angular/core';
import { SearchScope } from '../models/search-history.model';
import { ApiService } from './api.service';

const RECENT_MAX = 10;
const PINNED_MAX = 5;

/** One page's view of the history: the whole surface a host component needs. */
export interface ScopedSearchHistory {
  readonly pinned: Signal<string[]>;
  readonly recent: Signal<string[]>;
  load(): void;
  record(query: string): void;
  setPinned(query: string, pinned: boolean): void;
  remove(query: string): void;
}

/**
 * Per-user saved-search state (pinned + recent), backed by the server
 * (`/api/search-history`). Mutations update the local signals optimistically and
 * fire-and-forget the request; the panel calls {@link ScopedSearchHistory.load} on
 * open to reconcile.
 *
 * State is kept **per scope** because this is one root singleton serving three
 * pages: with a single pair of signals, a Traces panel opening would overwrite the
 * signals the Events panel was showing, and a Metrics `record` would push a PromQL
 * expression into the logs list — the histories silently merged in the UI even
 * though the server keeps them apart. {@link forScope} hands each page its own
 * cached view instead.
 */
@Injectable({ providedIn: 'root' })
export class SearchHistoryService {
  private api = inject(ApiService);

  private readonly views = new Map<SearchScope, ScopedSearchHistory>();

  /** The (cached) view for one page; repeat calls for a scope return the same object. */
  forScope(scope: SearchScope): ScopedSearchHistory {
    let view = this.views.get(scope);
    if (!view) {
      view = this.createView(scope);
      this.views.set(scope, view);
    }
    return view;
  }

  private createView(scope: SearchScope): ScopedSearchHistory {
    const api = this.api;
    const _pinned = signal<string[]>([]);
    const _recent = signal<string[]>([]);

    const load = (): void => {
      api.getSearchHistory(scope).subscribe({
        next: h => { _pinned.set(h.pinned ?? []); _recent.set(h.recent ?? []); },
        error: () => { /* keep whatever we have */ },
      });
    };

    return {
      pinned: _pinned.asReadonly(),
      recent: _recent.asReadonly(),

      load,

      /** Records a deliberately-run query (skips blanks; pinned queries stay pinned). */
      record(query: string): void {
        const q = query.trim();
        if (!q) return;
        if (!_pinned().includes(q)) {
          _recent.update(r => [q, ...r.filter(x => x !== q)].slice(0, RECENT_MAX));
        }
        api.recordSearch(q, scope).subscribe({ error: () => { /* best-effort */ } });
      },

      setPinned(query: string, pinned: boolean): void {
        const q = query.trim();
        if (!q) return;
        if (pinned) {
          _recent.update(r => r.filter(x => x !== q));
          _pinned.update(p => [q, ...p.filter(x => x !== q)].slice(0, PINNED_MAX));
        } else {
          _pinned.update(p => p.filter(x => x !== q));
          _recent.update(r => [q, ...r.filter(x => x !== q)].slice(0, RECENT_MAX));
        }
        api.pinSearch(q, pinned, scope).subscribe({ error: () => load() });
      },

      remove(query: string): void {
        const q = query.trim();
        if (!q) return;
        _pinned.update(p => p.filter(x => x !== q));
        _recent.update(r => r.filter(x => x !== q));
        api.deleteSearch(q, scope).subscribe({ error: () => load() });
      },
    };
  }
}
