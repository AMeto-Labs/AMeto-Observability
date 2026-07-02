import { Injectable, computed, signal } from '@angular/core';
import { SettingsTabId } from './settings-tab';

/**
 * Component-scoped service shared between the Settings container and its
 * section components. Each section reports its own "dirty" (unsaved) state
 * here so the container can drive the F5 `beforeunload` warning and the
 * `canDeactivate` route guard from a single aggregated signal.
 *
 * Provided at the SettingsComponent level (not root) so every section created
 * inside it shares the same instance.
 */
@Injectable()
export class SettingsDirtyService {
  private readonly dirtyTabs = signal<ReadonlySet<SettingsTabId>>(new Set());

  /** True when ANY section has unsaved changes. */
  readonly dirty = computed(() => this.dirtyTabs().size > 0);

  /** Human-readable list of dirty tab ids (for confirm dialogs). */
  readonly dirtyTabsList = computed(() => [...this.dirtyTabs()]);

  mark(tab: SettingsTabId, isDirty: boolean): void {
    this.dirtyTabs.update(current => {
      const next = new Set(current);
      if (isDirty) next.add(tab);
      else next.delete(tab);
      return next;
    });
  }

  clear(tab: SettingsTabId): void {
    this.mark(tab, false);
  }
}
