import { Routes } from '@angular/router';
import { authGuard } from './core/guards/auth.guard';
import { permGuard } from './core/guards/perm.guard';
import { settingsDeactivateGuard } from './core/guards/settings-deactivate.guard';
import { ViewPermission } from './core/models/auth.model';

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
        canActivate: [permGuard(ViewPermission.Logs)],
        loadComponent: () => import('./pages/events/events').then(m => m.EventsComponent),
      },
      {
        path: 'stats',
        canActivate: [permGuard(ViewPermission.Stats)],
        loadComponent: () => import('./pages/stats/stats').then(m => m.StatsComponent),
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
        path: 'traces',
        canActivate: [permGuard(ViewPermission.Traces)],
        loadComponent: () => import('./pages/traces/traces').then(m => m.TracesComponent),
      },
      {
        path: 'metrics',
        canActivate: [permGuard(ViewPermission.Metrics)],
        loadComponent: () => import('./pages/metrics/metrics').then(m => m.MetricsComponent),
      },
      {
        path: 'reference',
        loadComponent: () => import('./pages/reference/reference').then(m => m.ReferenceComponent),
      },
    ],
  },
  {
    path: '**',
    loadComponent: () => import('./pages/not-found/not-found').then(m => m.NotFoundComponent),
  },
];
