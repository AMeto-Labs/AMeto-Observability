import { CanActivateFn, Router } from '@angular/router';
import { inject } from '@angular/core';
import { catchError, map, of } from 'rxjs';
import { AuthService, isUnauthorized } from '../services/auth.service';

/**
 * Lets a signed-in user through. An expired access token is not a reason to show the login page —
 * the session cookie usually outlives it by weeks — so the guard refreshes first and sends the
 * user to sign in only when the server refuses (401). Any other failure (the server is down)
 * lets the page load and report it the way it reports everything else.
 */
export const authGuard: CanActivateFn = (_route, state) => {
  const auth   = inject(AuthService);
  const router = inject(Router);
  const toLogin = () => router.createUrlTree(['/login'], {
    queryParams: state.url && state.url !== '/' ? { returnUrl: state.url } : {},
  });

  if (auth.isAuthenticated()) return true;
  if (!auth.hasSession()) return toLogin();
  return auth.refresh().pipe(
    map(() => true),
    catchError(err => of(isUnauthorized(err) ? toLogin() : true)),
  );
};
