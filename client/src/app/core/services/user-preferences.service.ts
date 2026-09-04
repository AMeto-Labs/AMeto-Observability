import { Injectable, computed, signal } from '@angular/core';

const OVERVIEW_PROPS_KEY = 'Ameto-events-overview-custom-props';

const DRAWER_WIDTH_KEY = 'Ameto-events-drawer-width';
const DRAWER_MIN = 400;
const DRAWER_MAX = 1200;
const DRAWER_DEFAULT = 560;

const SPAN_DETAIL_HEIGHT_KEY = 'Ameto-traces-span-detail-height';
/** Below this the two-column body stops showing a usable line of either column. */
const SPAN_DETAIL_MIN = 140;
/** The waterfall above it has to stay readable; past this the panel IS the tab. The drag is
 *  additionally capped against the live viewport in the component, which this cannot see. */
const SPAN_DETAIL_MAX = 900;
/** The height the panel had when it was fixed (min 180 / max 240 in SCSS), so an untouched
 *  install looks exactly as it did. */
const SPAN_DETAIL_DEFAULT = 220;

@Injectable({ providedIn: 'root' })
export class UserPreferencesService {
  private _overviewCustomProps = signal<string[]>(this.loadOverviewProps());

  overviewCustomProps = computed(() => this._overviewCustomProps());
  overviewCustomPropsCsv = computed(() => this._overviewCustomProps().join(', '));

  /** Persisted width (px) of the events detail drawer — restored on every visit. */
  private _drawerWidth = signal<number>(this.loadDrawerWidth());
  drawerWidth = computed(() => this._drawerWidth());

  /** Persisted height (px) of the trace span-detail bottom panel — restored on every visit. */
  private _spanDetailHeight = signal<number>(this.loadSpanDetailHeight());
  spanDetailHeight = computed(() => this._spanDetailHeight());

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

  /**
   * Updates the span-detail panel height. Same two-phase contract as
   * {@link setDrawerWidth}: `persist: false` while dragging, `true` once on release.
   */
  setSpanDetailHeight(px: number, persist = true): void {
    const clamped = Math.round(Math.max(SPAN_DETAIL_MIN, Math.min(SPAN_DETAIL_MAX, px)));
    this._spanDetailHeight.set(clamped);
    if (persist) {
      try { localStorage.setItem(SPAN_DETAIL_HEIGHT_KEY, String(clamped)); } catch { /* quota / private mode */ }
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

  private loadSpanDetailHeight(): number {
    try {
      const raw = localStorage.getItem(SPAN_DETAIL_HEIGHT_KEY);
      const n = raw ? Number(raw) : NaN;
      if (Number.isFinite(n) && n >= SPAN_DETAIL_MIN && n <= SPAN_DETAIL_MAX) return n;
    } catch { /* ignore */ }
    return SPAN_DETAIL_DEFAULT;
  }
}
