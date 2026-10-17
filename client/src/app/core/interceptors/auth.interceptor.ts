import { HttpInterceptorFn, HttpRequest, HttpHandlerFn } from '@angular/common/http';
import { inject } from '@angular/core';
import { catchError, switchMap, throwError } from 'rxjs';
import { AuthService, isUnauthorized } from '../services/auth.service';

/**
 * The endpoints that manage the session itself. They carry no bearer token — the session
 * cookie is their credential, and a stale token would only add a failed validation to the
 * server's log — and a 401 from them is an answer, not something to refresh and retry.
 */
const SESSION_ENDPOINTS = ['/api/auth/login', '/api/auth/refresh', '/api/auth/logout', '/api/auth/providers'];

function isSessionEndpoint(url: string): boolean {
  const path = url.split('?')[0];
  // endsWith: the deployment prefix is already on the URL (baseHrefInterceptor runs first).
  return SESSION_ENDPOINTS.some(p => path.endsWith(p));
}

function withToken(req: HttpRequest<unknown>, token: string | null): HttpRequest<unknown> {
  return token ? req.clone({ setHeaders: { Authorization: `Bearer ${token}` } }) : req;
}

/**
 * Attaches the access token, and survives its expiry: a 401 is answered with one refresh — shared
 * with every other request that hit the same expiry — and one retry under the new token. Only
 * when the refresh itself is refused (the session is over) does the user get signed out.
 */
export const authInterceptor: HttpInterceptorFn = (
  req: HttpRequest<unknown>,
  next: HttpHandlerFn,
) => {
  if (isSessionEndpoint(req.url)) return next(req);

  const auth = inject(AuthService);
  return next(withToken(req, auth.token)).pipe(
    catchError(err => {
      if (!isUnauthorized(err)) return throwError(() => err);
      if (!auth.hasSession()) { auth.logout(); return throwError(() => err); }

      return auth.refresh().pipe(
        catchError(refreshErr => {
          // Refused: the session is over. Anything else (offline, server restarting) leaves the
          // session for the next attempt — but this request still failed, with its own error.
          if (isUnauthorized(refreshErr)) auth.logout();
          return throwError(() => err);
        }),
        switchMap(() => next(withToken(req, auth.token)).pipe(
          catchError(retryErr => {
            // A fresh token refused straight away: the session ended between the two calls.
            if (isUnauthorized(retryErr)) auth.logout();
            return throwError(() => retryErr);
          }),
        )),
      );
    }),
  );
};
