import { CanDeactivateFn } from '@angular/router';
import { SettingsComponent } from '../../pages/settings/settings';

/**
 * Prevents leaving the Settings route via in-app navigation (e.g. clicking a
 * nav item) when any section has unsaved changes, after a confirm prompt.
 *
 * Browser refresh / close / F5 is handled separately by the SettingsComponent
 * `beforeunload` host listener.
 */
export const settingsDeactivateGuard: CanDeactivateFn<SettingsComponent> = component => {
  if (component.isDirty()) {
    const tabs = component.dirtyTabsList().join(', ');
    return confirm(`You have unsaved changes in Settings (${tabs}). Leave anyway?`);
  }
  return true;
};
