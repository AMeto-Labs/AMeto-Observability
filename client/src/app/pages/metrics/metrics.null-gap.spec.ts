import { scaledValue } from './metrics';

/**
 * A NULL POINT IS PLOTTED AS A GAP, NOT AS ZERO (#92).
 *
 * <p>The server answers a NaN or ±Infinity point value — and an aggregated timestamp with no finite
 * member — as `null`. The chart plotted `y: p.value * scale`, and `null * scale` is 0: every such
 * point was a drop to zero on the chart. Chart.js draws a `null` y as a gap; `scaledValue` hands it one.
 * The exemplar dots go through the same function.</p>
 */
describe('metrics chart — a null value', () => {
  it('stays null, where multiplying it made 0', () => {
    expect((null as unknown as number) * 1000).toBe(0);   // the hazard
    expect(scaledValue(null, 1000)).toBeNull();
  });

  it('leaves every real value scaled as before, zero included', () => {
    expect(scaledValue(0, 1000)).toBe(0);
    expect(scaledValue(2.5, 1000)).toBe(2500);
    expect(scaledValue(-1, 1)).toBe(-1);
  });
});
