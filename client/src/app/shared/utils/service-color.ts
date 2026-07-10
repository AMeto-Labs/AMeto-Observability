/**
 * Deterministic per-service colour. The same service name always maps to the same
 * palette entry (stable across sessions and views), so a service reads as "one colour"
 * everywhere it appears — list rows, the detail drawer, dropdowns, trace waterfalls.
 *
 * Palette matches the one users already see in the Services filter dropdown.
 */
const SERVICE_PALETTE = [
  '#4DA3FF', '#38BDF8', '#34D399', '#A78BFA',
  '#FB923C', '#F472B6', '#22D3EE', '#818CF8',
  '#E879F9', '#4ADE80', '#FACC15', '#F87171',
] as const;

/** Neutral fallback for an empty/unknown service. */
const NO_SERVICE = 'var(--txt-muted)';

/** FNV-ish rolling hash → palette index. Cheap and stable. */
export function serviceColor(name: string | null | undefined): string {
  if (!name) return NO_SERVICE;
  let h = 0;
  for (let i = 0; i < name.length; i++) h = (h * 31 + name.charCodeAt(i)) >>> 0;
  return SERVICE_PALETTE[h % SERVICE_PALETTE.length];
}
