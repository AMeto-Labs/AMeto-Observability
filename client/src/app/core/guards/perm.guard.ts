import { CanActivateFn, Router } from '@angular/router';
import { inject } from '@angular/core';
import { AuthService } from '../services/auth.service';
import { ViewPermission } from '../models/auth.model';

/** Ordered fallbacks: a blocked user is redirected to the first page they *can* see. */
const FALLBACKS: { perm: ViewPermission; path: string }[] = [
  { perm: ViewPermission.Logs,    path: '/events' },
  { perm: ViewPermission.Stats,   path: '/stats' },
  { perm: ViewPermission.Traces,  path: '/traces' },
  { perm: ViewPermission.Metrics, path: '/metrics' },
];

/**
 * Route guard for a per-view scope. Allows navigation when the user holds the
 * scope (admin always does); otherwise redirects to the first scoped page they
 * can access, falling back to Alerts (which is never scope-gated).
 */
export function permGuard(scope: ViewPermission): CanActivateFn {
  return () => {
    const auth   = inject(AuthService);
    const router = inject(Router);
    if (auth.can(scope)) return true;
    const fallback = FALLBACKS.find(f => auth.can(f.perm));
    return router.createUrlTree([fallback?.path ?? '/signals']);
  };
}
