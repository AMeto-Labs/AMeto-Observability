import { TestBed } from '@angular/core/testing';
import { HttpErrorResponse } from '@angular/common/http';
import { provideRouter } from '@angular/router';
import { LUCIDE_ICONS, LucideIconProvider, icons } from 'lucide-angular';
import { Observable, of, throwError } from 'rxjs';
import { MetricsComponent } from './metrics';
import { ApiService } from '../../core/services/api.service';
import { HeatmapDto, MetricCatalogDto } from '../../core/models/metric.model';

/**
 * A 503 IN HEATMAP MODE IS ONE SENTENCE, NOT TWO (#95).
 *
 * <p>The metric store refusing the heatmap query answers with the server's sentence. The page used
 * to show it ABOVE the heatmap component, which, handed no data, still rendered its own "No
 * histogram data" — the very "no data" the 503 exists to replace, beside the sentence that
 * contradicts it. The sentence now takes the heatmap's place.</p>
 */
describe('MetricsComponent — a 503 in heatmap mode', () => {
  const sentence = 'The metric store has shut down and cannot answer. Metrics are available again once the server has restarted.';
  const unavailable = () => throwError(() => new HttpErrorResponse({ status: 503, error: { error: sentence } }));

  const histogram: MetricCatalogDto = {
    name: 'http.server.request.duration', type: 'Histogram', unit: 's',
    labelKeys: [], cardinality: 1, lastSeenMs: Date.now(),
  } as MetricCatalogDto;

  function render(heatmapAnswer: () => Observable<HeatmapDto | null>) {
    const api = {
      getMetricCatalog:   () => of([histogram]),
      getMetricHeatmap:   heatmapAnswer,
      getMetricExemplars: () => of([]),
      queryMetricAgg:     () => of([]),
      queryMetricExpr:    () => of(null),
      getSearchHistory:   () => of([]),
    };
    TestBed.configureTestingModule({
      imports: [MetricsComponent],
      providers: [
        provideRouter([]),
        { provide: ApiService, useValue: api },
        { provide: LUCIDE_ICONS, multi: true, useValue: new LucideIconProvider(icons) },
      ],
    });
    const fixture = TestBed.createComponent(MetricsComponent);
    fixture.componentInstance.viewMode.set('heatmap');   // before init: the canvas chart never mounts
    fixture.detectChanges();                             // ngOnInit: catalog → the histogram → the query
    fixture.detectChanges();
    return fixture;
  }

  afterEach(() => TestBed.resetTestingModule());

  it("shows the server's sentence in place of the heatmap", () => {
    const fixture = render(unavailable);
    const text: string = fixture.nativeElement.textContent;

    expect(text).toContain(sentence);
    expect(text).not.toContain('No histogram data');
    expect(fixture.nativeElement.querySelector('app-heatmap')).toBeNull();
    fixture.destroy();
  });

  it('still shows the heatmap (and its own empty state) when the store answered', () => {
    const fixture = render(() => of(null));
    const text: string = fixture.nativeElement.textContent;

    expect(text).not.toContain(sentence);
    expect(fixture.nativeElement.querySelector('app-heatmap')).not.toBeNull();
    expect(text).toContain('No histogram data');
    fixture.destroy();
  });
});
