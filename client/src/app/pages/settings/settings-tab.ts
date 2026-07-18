export type SettingsTabId = 'dashboards' | 'events' | 'retention' | 'users' | 'api-keys' | 'updates';

export interface SettingsTab {
  id: SettingsTabId;
  label: string;
  icon: string;
  adminOnly?: boolean;
}

/** Ordered definition of all Settings tabs. Admin-only tabs are filtered out
 *  for non-admin users by the container. */
export const SETTINGS_TABS: readonly SettingsTab[] = [
  { id: 'dashboards', label: 'Dashboards', icon: 'layout-dashboard' },
  { id: 'events',     label: 'Events',     icon: 'list' },
  { id: 'retention',  label: 'Retention',  icon: 'database' },
  { id: 'users',      label: 'Users',      icon: 'users', adminOnly: true },
  { id: 'api-keys',   label: 'API Keys',   icon: 'key' },
  { id: 'updates',    label: 'Updates',    icon: 'download', adminOnly: true },
];
