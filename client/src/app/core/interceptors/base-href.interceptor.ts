import { HttpInterceptorFn, HttpRequest, HttpHandlerFn } from '@angular/common/http';

import { appPath } from '../../shared/utils/app-url';

/**
 * Rewrites app-relative request URLs so they land under the deployment prefix.
 *
 * Every call in `ApiService` is written as a root-absolute literal (`/api/events`), which is
 * the right thing to write — but a root-absolute URL deliberately ignores `<base href>`, so
 * under `BasePath: /ameto` all of them would miss the server by exactly one path segment.
 * Rewriting once here beats prefixing several hundred call sites, and keeps the prefix a
 * deployment concern rather than something every service has to remember.
 *
 * Deciding what is app-relative belongs in {@link appPath}, not here: a URL that names its own
 * destination comes back unchanged, and `req.clone` with an identical URL is inert.
 *
 * Must run before {@link authInterceptor}: that one only reads the URL, but an interceptor
 * that ever decides *whether* to attach the token by looking at the path should see the final
 * one.
 */
export const baseHrefInterceptor: HttpInterceptorFn = (
  req: HttpRequest<unknown>,
  next: HttpHandlerFn,
) => next(req.clone({ url: appPath(req.url) }));
