import { boundLabel, bucketRange } from './heatmap';

/**
 * A NULL BUCKET BOUND IS MARKED, NOT LABELLED "0" (#92).
 *
 * <p>The server answers a bound it cannot represent — NaN, or an explicit ±Infinity an exporter sent —
 * as `null`. The y-axis read it through `?? 0` and labelled the bucket "0"; the tooltip multiplied it
 * (`null * scale` is 0) into a range that was never the bucket's.</p>
 */
describe('heatmap — a null bucket bound', () => {
  const bounds = [0.5, null, 2];

  it('is labelled "—" on the axis, and every real bound as before', () => {
    expect(boundLabel(bounds, 0, 1000)).toBe('0');
    expect(boundLabel(bounds, 1, 1000)).toBe('500');
    expect(boundLabel(bounds, 2, 1000)).toBe('—');
    expect(boundLabel(bounds, 3, 1000)).toBe('2.0k');
    expect(boundLabel(bounds, 4, 1000)).toBe('∞');
  });

  it('makes the tooltip range of both buckets it bounds "—"', () => {
    expect(bucketRange(bounds, 0, 1000)).toBe('0.00–500');
    expect(bucketRange(bounds, 1, 1000)).toBe('—');
    expect(bucketRange(bounds, 2, 1000)).toBe('—');
    expect(bucketRange(bounds, 3, 1000)).toBe('> 2.0k');
  });
});
