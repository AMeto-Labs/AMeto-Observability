import { TestBed } from '@angular/core/testing';
import { MetricSparkComponent } from './metric-spark';
import { MetricSeriesDto } from '../../../core/models/metric.model';

/**
 * A NULL VALUE IS LEFT OUT, NOT DRAWN AS ZERO (#92).
 *
 * <p>The server answers a NaN or ±Infinity point value as `null`. The spark used to keep such a point:
 * `null < minV` is `0 < minV`, so the null became the minimum, and the line was drawn down to it — a
 * drop to zero that never happened. The spark now leaves value-less points out of its scale, its line
 * and its "value at event", and JOINS the points either side of it — a trend glyph; the Metrics chart,
 * unlike the spark, draws a gap there.</p>
 */
function series(values: (number | null)[]): MetricSeriesDto {
  return {
    name: 'm', kind: 'Gauge', unit: '', labels: {},
    points: values.map((v, i) => ({ ts: (i + 1) * 1e9, value: v, count: 0, sum: 0 })),
  };
}

function spark(values: (number | null)[], eventTsMs: number): MetricSparkComponent {
  const fixture = TestBed.createComponent(MetricSparkComponent);
  fixture.componentRef.setInput('series', series(values));
  fixture.componentRef.setInput('eventTsMs', eventTsMs);
  fixture.detectChanges();
  return fixture.componentInstance;
}

describe('metric spark — a null value is left out, not drawn as zero', () => {
  it('scales and draws only the points that carry a value', () => {
    const c = spark([1, null, 3], 2000);
    // min 1 at the bottom (y 40), max 3 at the top (y 4); the null point is not on the line.
    expect(c.linePath()).toBe('M2.0,40.0 L218.0,4.0');
    expect(c.areaPath()).not.toContain('NaN');
  });

  it('reads the value at the event from the nearest point that has one', () => {
    expect(spark([1, null, 3], 2000).valueAtEvent()).toBe('1');
  });

  it('shows no data when no point has a value', () => {
    const fixture = TestBed.createComponent(MetricSparkComponent);
    fixture.componentRef.setInput('series', series([null, null]));
    fixture.componentRef.setInput('eventTsMs', 1000);
    fixture.detectChanges();
    expect((fixture.nativeElement as HTMLElement).textContent).toContain('no data');
  });
});
