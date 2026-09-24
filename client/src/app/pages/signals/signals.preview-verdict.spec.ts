import { previewVerdict } from './signals';

/**
 * A PREVIEW WITH NO VALUE CLAIMS NO VERDICT (#92).
 *
 * <p>POST /api/alerts/preview answers `value: null, wouldFire: false` for a window whose every point
 * is NaN or infinite — the evaluator leaves such a rule's state alone. The editor printed "→ ok" for
 * it, which reads as "this rule would resolve".</p>
 */
describe('alert preview verdict', () => {
  it('is "no data" when the preview has no value', () => {
    expect(previewVerdict({ value: null, threshold: 5, wouldFire: false })).toBe('no data');
  });

  it('is the server verdict for a value, as before', () => {
    expect(previewVerdict({ value: 3, threshold: 5, wouldFire: true })).toBe('WOULD FIRE');
    expect(previewVerdict({ value: 7, threshold: 5, wouldFire: false })).toBe('ok');
    expect(previewVerdict({ value: 0, threshold: 5, wouldFire: false })).toBe('ok');   // 0 is a value
  });
});
