import { TestBed } from '@angular/core/testing';
import { HttpClient, HttpErrorResponse, provideHttpClient, withInterceptors } from '@angular/common/http';
import { HttpTestingController, provideHttpClientTesting } from '@angular/common/http/testing';
import { Router, UrlTree, provideRouter } from '@angular/router';
import { firstValueFrom, isObservable, Observable } from 'rxjs';
import { authInterceptor } from './auth.interceptor';
import { authGuard } from '../guards/auth.guard';
import { AuthService } from '../services/auth.service';

/**
 * The access token lives 15 minutes; the sign-in lives as long as its session cookie. These pin
 * the client half of that: an expired token is renewed, not treated as a sign-out — and only the
 * server refusing the refresh signs the user out.
 */

const fresh = (token: string) => ({ token, expiresIn: 900, role: 'viewer', permissions: 15 });

/** Puts a token in storage the way a previous page load left it, before AuthService reads it. */
function seedSession(token: string, expiresInMs: number): void {
  localStorage.setItem('rd_token', token);
  localStorage.setItem('rd_token_exp', String(Date.now() + expiresInMs));
  localStorage.setItem('rd_role', 'viewer');
  localStorage.setItem('rd_perms', '15');
}

function setUp(): { http: HttpClient; mock: HttpTestingController; auth: AuthService; router: Router } {
  TestBed.configureTestingModule({
    providers: [
      provideRouter([{ path: 'login', children: [] }, { path: 'events', children: [] }]),
      provideHttpClient(withInterceptors([authInterceptor])),
      provideHttpClientTesting(),
    ],
  });
  return {
    http:   TestBed.inject(HttpClient),
    mock:   TestBed.inject(HttpTestingController),
    auth:   TestBed.inject(AuthService),
    router: TestBed.inject(Router),
  };
}

const unauthorized = { status: 401, statusText: 'Unauthorized' };

describe('authInterceptor: an expired access token is a refresh away, not a sign-out', () => {
  beforeEach(() => localStorage.clear());
  afterEach(() => TestBed.inject(HttpTestingController).verify());

  it('answers a 401 with one refresh and retries the request under the new token', () => {
    seedSession('old-token', -1_000);
    const { http, mock, auth, router } = setUp();
    const navigate = vi.spyOn(router, 'navigate');
    let result: unknown;
    http.get('/api/stats').subscribe(r => (result = r));

    const first = mock.expectOne('/api/stats');
    expect(first.request.headers.get('Authorization')).toBe('Bearer old-token');
    first.flush(null, unauthorized);

    const refresh = mock.expectOne('/api/auth/refresh');
    // The cookie is the credential; a stale bearer token would only add a failed validation.
    expect(refresh.request.headers.has('Authorization')).toBe(false);
    refresh.flush(fresh('new-token'));

    const retry = mock.expectOne('/api/stats');
    expect(retry.request.headers.get('Authorization')).toBe('Bearer new-token');
    retry.flush({ ok: true });

    expect(result).toEqual({ ok: true });
    expect(auth.token).toBe('new-token');
    expect(navigate).not.toHaveBeenCalled();
  });

  it('shares one refresh among every request that hit the same expiry', () => {
    seedSession('old-token', -1_000);
    const { http, mock } = setUp();
    http.get('/api/a').subscribe();
    http.get('/api/b').subscribe();

    mock.expectOne('/api/a').flush(null, unauthorized);
    mock.expectOne('/api/b').flush(null, unauthorized);

    // One refresh: each one rotates the session cookie.
    mock.expectOne('/api/auth/refresh').flush(fresh('new-token'));
    mock.expectOne('/api/a').flush({});
    mock.expectOne('/api/b').flush({});
  });

  it('signs out when the server refuses the refresh', () => {
    seedSession('old-token', -1_000);
    const { http, mock, auth, router } = setUp();
    const navigate = vi.spyOn(router, 'navigate');
    let failure: HttpErrorResponse | undefined;
    http.get('/api/stats').subscribe({ error: e => (failure = e) });

    mock.expectOne('/api/stats').flush(null, unauthorized);
    mock.expectOne('/api/auth/refresh').flush(null, unauthorized);
    // And nothing after it — no retry, and no sign-out call: the refusal already cleared the
    // cookie on the server's side (verify() in afterEach holds this).

    expect(failure?.status).toBe(401);
    expect(auth.hasSession()).toBe(false);
    expect(navigate).toHaveBeenCalledWith(['/login']);
  });

  it('keeps the session when the refresh fails for any other reason', () => {
    seedSession('old-token', -1_000);
    const { http, mock, auth, router } = setUp();
    const navigate = vi.spyOn(router, 'navigate');
    let failure: HttpErrorResponse | undefined;
    http.get('/api/stats').subscribe({ error: e => (failure = e) });

    mock.expectOne('/api/stats').flush(null, unauthorized);
    mock.expectOne('/api/auth/refresh').error(new ProgressEvent('error'));   // offline

    expect(failure?.status).toBe(401);
    expect(auth.hasSession()).toBe(true);
    expect(navigate).not.toHaveBeenCalled();
  });

  it('leaves a 401 from the sign-in itself to the login page', () => {
    const { http, mock } = setUp();
    let failure: HttpErrorResponse | undefined;
    http.post('/api/auth/login', { username: 'u', password: 'wrong' }).subscribe({ error: e => (failure = e) });

    mock.expectOne('/api/auth/login').flush(null, unauthorized);

    expect(failure?.status).toBe(401);   // no refresh, no retry
  });
});

describe('authGuard: the login page only when the session is really over', () => {
  beforeEach(() => localStorage.clear());
  afterEach(() => TestBed.inject(HttpTestingController).verify());

  const run = () => TestBed.runInInjectionContext(
    () => authGuard({} as never, { url: '/traces' } as never)) as boolean | UrlTree | Observable<boolean | UrlTree>;

  const settle = (result: boolean | UrlTree | Observable<boolean | UrlTree>) =>
    isObservable(result) ? firstValueFrom(result) : Promise.resolve(result);

  it('lets a valid token straight through', async () => {
    seedSession('token', 10 * 60_000);
    setUp();
    expect(await settle(run())).toBe(true);
  });

  it('sends a browser that never signed in to the login page, keeping where it was going', async () => {
    const { router } = setUp();
    const result = await settle(run());
    expect(router.serializeUrl(result as UrlTree)).toBe('/login?returnUrl=%2Ftraces');
  });

  it('renews an expired token instead of showing the login page', async () => {
    seedSession('old-token', -60 * 60_000);   // an hour stale: the laptop slept
    const { mock } = setUp();
    const pending = settle(run());
    mock.expectOne('/api/auth/refresh').flush(fresh('new-token'));
    expect(await pending).toBe(true);
  });

  it('shows the login page when the session has ended', async () => {
    seedSession('old-token', -60 * 60_000);
    const { mock, router } = setUp();
    const pending = settle(run());
    mock.expectOne('/api/auth/refresh').flush(null, unauthorized);
    expect(router.serializeUrl(await pending as UrlTree)).toBe('/login?returnUrl=%2Ftraces');
  });

  it('lets the page load when the server cannot be reached, rather than signing out', async () => {
    seedSession('old-token', -60 * 60_000);
    const { mock } = setUp();
    const pending = settle(run());
    mock.expectOne('/api/auth/refresh').flush(null, { status: 503, statusText: 'Service Unavailable' });
    expect(await pending).toBe(true);
  });
});
