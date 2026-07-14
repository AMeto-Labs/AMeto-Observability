import { Injectable } from '@angular/core';
import { PreloadingStrategy, Route } from '@angular/router';
import { Observable, of, timer } from 'rxjs';
import { mergeMap } from 'rxjs/operators';

/**
 * Preloads lazy route chunks a couple of seconds after bootstrap — once the
 * initial route has rendered — so navigating between dashboard pages is instant
 * without the preload competing with the first paint / initial data fetch.
 * Opt a route out with `data: { preload: false }`.
 */
@Injectable({ providedIn: 'root' })
export class IdlePreloadStrategy implements PreloadingStrategy {
  preload(route: Route, load: () => Observable<unknown>): Observable<unknown> {
    if (route.data?.['preload'] === false) return of(null);
    return timer(2000).pipe(mergeMap(() => load()));
  }
}
