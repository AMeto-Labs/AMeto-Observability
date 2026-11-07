import { computed, inject } from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';
import { patchState, signalStore, withComputed, withHooks, withMethods, withState } from '@ngrx/signals';

import {
  DEFAULT_PRESET, QUICK_RANGE_LABELS, TimePreset, customRangeLabel, parseCustomDate,
} from '../../shared/utils/time-range';
import {
  StatsSignal, StatsWindow, parseStatsParams, resolveWindow, windowCaption, windowLabel,
} from './stats-range.util';

interface StatsState {
  preset: TimePreset;
  /** The custom bounds as `yyyy-MM-dd HH:mm`; `customTo` '' for "up to now". */
  customFrom: string;
  customTo: string;
  /** The band a phone shows; a desktop shows all three. */
  tab: StatsSignal;
  /** Bumped by Refresh and by every range change: the window re-resolves against the clock. */
  tick: number;
  sheetOpen: boolean;
}

/**
 * The Statistics page's one time range, shared by its three bands, with the URL as its memory
 * (`?preset=7d`, `?preset=custom&from=…&to=…`, `?tab=traces` on a phone). The window itself is
 * computed: a preset is resolved against the clock whenever the range changes or Refresh is
 * pressed, so a page left open does not quietly keep yesterday's "now".
 */
export const StatsStore = signalStore(
  withState<StatsState>({
    preset: DEFAULT_PRESET, customFrom: '', customTo: '', tab: 'logs', tick: 0, sheetOpen: false,
  }),
  withComputed(store => {
    const window = computed<StatsWindow | null>(() => {
      store.tick();   // a dependency only: the clock moved, the window moves with it
      return resolveWindow(store.preset(), store.customFrom(), store.customTo());
    });
    /** How the range reads on the Custom button: the quick range's name, or the custom bounds. */
    const rangeLabel = computed(() => {
      const preset = store.preset();
      return preset === 'custom'
        ? customRangeLabel(parseCustomDate(store.customFrom()), parseCustomDate(store.customTo()))
        : { text: QUICK_RANGE_LABELS[preset], extra: '' };
    });
    const subtitle = computed(() => {
      const w = window();
      return w ? windowLabel(w) : 'Pick a custom range: a start from 1970 on, an end no later than tomorrow';
    });
    const caption = computed(() => {
      const w = window();
      return w ? windowCaption(w) : 'Custom range';
    });
    return { window, rangeLabel, subtitle, caption };
  }),
  withMethods(store => {
    const router = inject(Router);
    const route = inject(ActivatedRoute);

    function syncUrl(): void {
      const custom = store.preset() === 'custom';
      router.navigate([], {
        relativeTo: route,
        queryParams: {
          preset: store.preset() === DEFAULT_PRESET ? null : store.preset(),
          from: custom && store.customFrom() ? store.customFrom() : null,
          to: custom && store.customTo() ? store.customTo() : null,
          tab: store.tab() === 'logs' ? null : store.tab(),
        },
        queryParamsHandling: 'merge',
        replaceUrl: true,
      });
    }

    return {
      setPreset(preset: TimePreset): void {
        patchState(store, { preset, tick: store.tick() + 1 });
        syncUrl();
      },
      applyRange(from: string, to: string): void {
        patchState(store, { preset: 'custom', customFrom: from, customTo: to, tick: store.tick() + 1 });
        syncUrl();
      },
      setTab(tab: StatsSignal): void {
        patchState(store, { tab });
        syncUrl();
      },
      refresh(): void {
        patchState(store, { tick: store.tick() + 1 });
      },
      openSheet(): void { patchState(store, { sheetOpen: true }); },
      closeSheet(): void { patchState(store, { sheetOpen: false }); },
      restoreFromUrl(): void {
        const q = route.snapshot.queryParamMap;
        patchState(store, parseStatsParams(q.get('preset'), q.get('from'), q.get('to'), q.get('tab')));
      },
    };
  }),
  withHooks({
    onInit(store) { store.restoreFromUrl(); },
  }),
);

export type StatsStoreInstance = InstanceType<typeof StatsStore>;
