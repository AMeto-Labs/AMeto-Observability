import { Routes } from '@angular/router';
import { authGuard } from './core/guards/auth.guard';
import { settingsDeactivateGuard } from './core/guards/settings-deactivate.guard';

export const routes: Routes = [
  {
    path: 'login',
    loadComponent: () => import('./pages/login/login').then(m => m.LoginComponent),
  },
  {
    // OAuth redirect lands here with ?token=...&expiresIn=...&role=...
    path: 'oauth-callback',
    loadComponent: () => import('./pages/oauth-callback/oauth-callback').then(m => m.OauthCallbackComponent),
  },
  {
    path: '',
    canActivate: [authGuard],
    loadComponent: () => import('./layout/shell/shell').then(m => m.ShellComponent),
    children: [
      { path: '', redirectTo: 'events', pathMatch: 'full' },
      {
        path: 'events',
        loadComponent: () => import('./pages/events/events').then(m => m.EventsComponent),
      },
      {
        path: 'live',
        loadComponent: () => import('./pages/live/live').then(m => m.LiveComponent),
      },
      {
        path: 'signals',
        loadComponent: () => import('./pages/signals/signals').then(m => m.SignalsPageComponent),
      },
      {
        path: 'nodes',
        loadComponent: () => import('./pages/nodes/nodes').then(m => m.NodesComponent),
      },
      {
        path: 'settings',
        children: [
          {
            path: '',
            canDeactivate: [settingsDeactivateGuard],
            loadComponent: () => import('./pages/settings/settings').then(m => m.SettingsComponent),
          },
          {
            path: 'users/:id',
            loadComponent: () => import('./pages/settings/user-detail/user-detail').then(m => m.UserDetailComponent),
          },
        ],
      },
      {
        path: 'diagnostics',
        loadComponent: () => import('./pages/diagnostics/diagnostics').then(m => m.DiagnosticsComponent),
      },
      {
        path: 'traces',
        loadComponent: () => import('./pages/traces/traces').then(m => m.TracesComponent),
      },
      {
        path: 'metrics',
        loadComponent: () => import('./pages/metrics/metrics').then(m => m.MetricsComponent),
      },
    ],
  },
  {
    path: '**',
    loadComponent: () => import('./pages/not-found/not-found').then(m => m.NotFoundComponent),
  },
];
