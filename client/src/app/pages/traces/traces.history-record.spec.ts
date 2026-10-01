import { TestBed } from '@angular/core/testing';
import { provideRouter } from '@angular/router';
import { LUCIDE_ICONS, LucideIconProvider, icons, AlertCircle } from 'lucide-angular';
import { NEVER, of } from 'rxjs';
import { TracesComponent } from './traces';
import { ApiService } from '../../core/services/api.service';

/**
 * A filter-bar search with an HTTP status IS recorded in the search history (PR #107 review,
 * pre-existing). `synthesizeTraceql` refuses to record a status it cannot spell as one value —
 * the '2xx' / '4xx' / '5xx' bucket picks — and tested that with `/^d+$/`, which matches a run of
 * the LETTER d: every status, '500' included, was refused, so no search touching the field ever
 * reached the history. Reverted (`/^d+$/`): nothing is recorded for '500'.
 */

class NoopResizeObserver {
  observe(): void { /* no layout in jsdom */ }
  unobserve(): void { /* no-op */ }
  disconnect(): void { /* no-op */ }
}
(globalThis as any).ResizeObserver ??= NoopResizeObserver;

describe('traces filter bar — what a search records in the history', () => {
  let recorded: { query: string; scope: string }[];

  function boot() {
    recorded = [];
    const api: Partial<ApiService> = {
      streamTraceList:  () => NEVER as any,
      streamTraceQuery: () => NEVER as any,
      getTraceStats:    () => NEVER as any,
      getServiceNames:  () => of([]) as any,
      getSearchHistory: () => NEVER as any,
      recordSearch:     ((query: string, scope: string) => { recorded.push({ query, scope }); return NEVER; }) as any,
      getTrace:         () => NEVER as any,
      getTraceLogs:     () => NEVER as any,
    };

    TestBed.configureTestingModule({
      imports: [TracesComponent],
      providers: [
        provideRouter([]),
        { provide: ApiService, useValue: api },
        { provide: LUCIDE_ICONS, multi: true, useValue: new LucideIconProvider({ ...icons, AlertCircle }) },
      ],
    });
    const fixture = TestBed.createComponent(TracesComponent);
    fixture.detectChanges();
    return fixture.componentInstance as any;
  }

  it('records a single status code as its TraceQL predicate', () => {
    const c = boot();
    c.filterHttpStatus = '500';
    c.applyFilters();
    expect(recorded).toEqual([{ query: '{ .http.status_code = 500 }', scope: 'traces' }]);
  });

  it('records it beside the other fields, in the bar\'s order', () => {
    const c = boot();
    c.filterService    = 'checkout';
    c.filterHttpStatus = '404';
    c.applyFilters();
    expect(recorded.map(r => r.query)).toEqual(['{ service = "checkout" && .http.status_code = 404 }']);
  });

  it('still records nothing for a status bucket, which has no single-value form', () => {
    const c = boot();
    c.filterService    = 'checkout';
    c.filterHttpStatus = '5xx';
    c.applyFilters();
    expect(recorded).toEqual([]);
  });
});
