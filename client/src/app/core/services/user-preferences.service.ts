import { Injectable, computed, signal } from '@angular/core';

const OVERVIEW_PROPS_KEY = 'Ameto-events-overview-custom-props';

const DRAWER_WIDTH_KEY = 'Ameto-events-drawer-width';
const DRAWER_MIN = 400;
const DRAWER_MAX = 1200;
const DRAWER_DEFAULT = 560;

@Injectable({ providedIn: 'root' })
export class UserPreferencesService {
  private _overviewCustomProps = signal<string[]>(this.loadOverviewProps());

  overviewCustomProps = computed(() => this._overviewCustomProps());
  overviewCustomPropsCsv = computed(() => this._overviewCustomProps().join(', '));

  /** Persisted width (px) of the events detail drawer — restored on every visit. */
  private _drawerWidth = signal<number>(this.loadDrawerWidth());
  drawerWidth = computed(() => this._drawerWidth());

  setOverviewCustomPropsFromCsv(csv: string): void {
    const parsed = Array.from(new Set(
      csv
        .split(',')
        .map(v => v.trim())
        .filter(v => v.length > 0)
    ));
    this._overviewCustomProps.set(parsed);
    localStorage.setItem(OVERVIEW_PROPS_KEY, JSON.stringify(parsed));
  }

  /**
   * Updates the drawer width (clamped to sane bounds). Pass `persist: false`
   * for live drag updates, then `true` once on release so localStorage is only
   * written on commit rather than on every mouse-move.
   */
  setDrawerWidth(px: number, persist = true): void {
    const clamped = Math.round(Math.max(DRAWER_MIN, Math.min(DRAWER_MAX, px)));
    this._drawerWidth.set(clamped);
    if (persist) {
      try { localStorage.setItem(DRAWER_WIDTH_KEY, String(clamped)); } catch { /* quota / private mode */ }
    }
  }

  private loadOverviewProps(): string[] {
    try {
      const raw = localStorage.getItem(OVERVIEW_PROPS_KEY);
      if (!raw) return [];
      const parsed = JSON.parse(raw);
      if (!Array.isArray(parsed)) return [];
      return parsed.filter((v): v is string => typeof v === 'string' && v.trim().length > 0);
    } catch {
      return [];
    }
  }

  private loadDrawerWidth(): number {
    try {
      const raw = localStorage.getItem(DRAWER_WIDTH_KEY);
      const n = raw ? Number(raw) : NaN;
      if (Number.isFinite(n) && n >= DRAWER_MIN && n <= DRAWER_MAX) return n;
    } catch { /* ignore */ }
    return DRAWER_DEFAULT;
  }
}
