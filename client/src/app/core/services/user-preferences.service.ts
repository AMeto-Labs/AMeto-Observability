import { Injectable, computed, signal } from '@angular/core';

const OVERVIEW_PROPS_KEY = 'Ameto-events-overview-custom-props';

@Injectable({ providedIn: 'root' })
export class UserPreferencesService {
  private _overviewCustomProps = signal<string[]>(this.loadOverviewProps());

  overviewCustomProps = computed(() => this._overviewCustomProps());
  overviewCustomPropsCsv = computed(() => this._overviewCustomProps().join(', '));

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
}
