import { subDays, startOfDay, format } from 'date-fns';
import { LEVELS } from '../../../core/models/event.model';

export type TimePreset = '5m' | '15m' | '30m' | '1d' | '7d' | '2w' | '1mo' | 'custom';

/** Safe identifier as accepted by the server-side lexer (letter/digit/_/@). */
const IDENT_RE = /^[A-Za-z_@][A-Za-z0-9_@]*$/;

/** Matches the trailing token under the caret that the autocomplete popup completes. */
export const PREFIX_RE = /[@A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*$/;

/** Matches an `@l = …` / `@l <> …` / `@l in […]` / `@l not in […]` clause for splicing. */
const LEVEL_CLAUSE_RE =
  /@l\s+not\s+in\s*\[[^\]]*\]|@l\s+in\s*\[[^\]]*\]|@l\s*(?:<>|!=|=)\s*'[^']*'/gi;

/** Matches a `['service.name']` clause (and the legacy OR-form) for splicing. */
const SERVICE_CLAUSE_RE =
  /\['service\.name'\]\s*=\s*'[^']*'|\['service\.name'\]\s*in\s*\[[^\]]*\]|\(service\.name\s*=\s*'[^']*'\s*or\s*ApplicationContext\s*=\s*'[^']*'\)/g;

/** Milliseconds between .NET DateTime min (0001-01-01 UTC) and Unix epoch (1970-01-01 UTC). */
const DOTNET_TICKS_UNIX_EPOCH_MS = 62_135_596_800_000;

/** Built-in tokens always offered by the filter autocomplete popup. */
export const BUILTIN_SUGGESTIONS = [
  '@l', '@mt', '@t', '@x', '@x.Type', '@x.Message', '@x.StackTrace',
  '@i', '@r', '@sp', '@tr',
  'and', 'or', 'not', 'in', 'like',
  'true', 'false', 'null',
  'has(', 'isDefined(', 'startsWith(', 'contains(', 'endsWith(',
  'ci_startsWith(', 'ci_contains(',
  'length(', 'coalesce(', 'fromJson(', 'toJson(',
  'toLower(', 'toUpper(', 'toNumber(',
  'substring(', 'indexOf(', 'lastIndexOf(', 'replace(', 'concat(',
  'ci_endsWith(', 'typeOf(', 'elementAt(', 'keys(', 'values(',
  'round(', 'now(', 'dateTime(', 'toIsoString(',
  'datePart(', 'timeOfDay(', 'timeSpan(', 'totalMilliseconds(',
  'toTimeString(', 'toHexString(', 'bucket(', 'offsetIn(', 'arrived(',
  'fromXml(', 'fromBase64(', 'toBase64(', 'regexMatch(', 'regexExtract(',
];

const ALL_LEVELS = () => new Set(LEVELS as readonly string[]);

/**
 * Walks a property bag and yields every reachable path in filter-language form
 * (`Foo.Bar`, `Foo['weird-key']`, `Tags[0]`). Arrays are traversed but only the
 * first element contributes suggestions — the goal is to expose shape, not values.
 */
export function collectPropPaths(obj: unknown, prefix: string, out: Set<string>, depth: number): void {
  if (depth > 4 || obj === null || obj === undefined) return;
  if (Array.isArray(obj)) {
    if (prefix) out.add(prefix);
    if (obj.length > 0) collectPropPaths(obj[0], `${prefix}[0]`, out, depth + 1);
    return;
  }
  if (typeof obj !== 'object') return;
  for (const [k, v] of Object.entries(obj as Record<string, unknown>)) {
    const seg = IDENT_RE.test(k) ? (prefix ? `${prefix}.${k}` : k) : `${prefix}['${k.replace(/'/g, "\\'")}']`;
    out.add(seg);
    collectPropPaths(v, seg, out, depth + 1);
  }
}

/** Extracts the identifier-like token that ends at `caret`. */
export function currentPrefix(value: string, caret: number): string {
  const m = value.slice(0, caret).match(PREFIX_RE);
  return m ? m[0] : '';
}

/** Converts JS milliseconds-since-Unix-epoch into .NET UTC ticks (100ns since 0001-01-01). */
export function msToDotNetUtcTicks(ms: number): number {
  return (ms + DOTNET_TICKS_UNIX_EPOCH_MS) * 10_000;
}

/**
 * Removes all matches of `pattern` from a filter expression, then cleans up
 * dangling `and`/`or` connectives and extra whitespace — used to splice out a
 * previous clause before inserting a new one, without erasing the user's own text.
 */
export function stripFilterClause(expr: string, pattern: RegExp): string {
  return expr
    .replace(pattern, '')
    .replace(/\s+and\s+and\s+/gi, ' and ')
    .replace(/^\s*(and|or)\s+/gi, '')
    .replace(/\s+(and|or)\s*$/gi, '')
    .trim();
}

/** Active log levels in a filter expression. Full set when there's no `@l` clause. */
export function parseLevelsFromFilter(expr: string): Set<string> {
  // `@l not in ['A', 'B']` ⇒ every level except the listed ones.
  const notInMatch = expr.match(/@l\s+not\s+in\s*\[([^\]]+)\]/i);
  if (notInMatch) {
    const levels = ALL_LEVELS();
    for (const m of notInMatch[1].matchAll(/'([^']+)'/g)) levels.delete(m[1]);
    return levels;
  }
  const inMatch = expr.match(/@l\s+in\s*\[([^\]]+)\]/i);
  if (inMatch) {
    const levels = new Set<string>();
    for (const m of inMatch[1].matchAll(/'([^']+)'/g)) levels.add(m[1]);
    return levels.size > 0 ? levels : ALL_LEVELS();
  }
  // `@l <> 'A'` / `@l != 'A'` ⇒ every level except A.
  const neqMatch = expr.match(/@l\s*(?:<>|!=)\s*'([^']+)'/i);
  if (neqMatch) {
    const levels = ALL_LEVELS();
    levels.delete(neqMatch[1]);
    return levels;
  }
  const eqMatch = expr.match(/@l\s*=\s*'([^']+)'/i);
  if (eqMatch) return new Set([eqMatch[1]]);
  return ALL_LEVELS();
}

/** Selected service names in a filter expression. */
export function parseServicesFromFilter(expr: string): Set<string> {
  const inMatch = expr.match(/\['service\.name'\]\s+in\s*\[([^\]]+)\]/i);
  if (inMatch) {
    const svcs = new Set<string>();
    for (const m of inMatch[1].matchAll(/'([^']+)'/g)) svcs.add(m[1]);
    return svcs;
  }
  const eqMatch = expr.match(/\['service\.name'\]\s*=\s*'([^']+)'/i);
  if (eqMatch) return new Set([eqMatch[1]]);
  return new Set<string>();
}

/** Rewrites the `@l` clause of `expr` for the given active `levels` (empty clause = all levels). */
export function setLevelsClause(expr: string, levels: Set<string>): string {
  const stripped = stripFilterClause(expr, LEVEL_CLAUSE_RE);
  if (levels.size === LEVELS.length) return stripped;
  let clause: string;
  if (levels.size === 1) {
    clause = `@l = '${[...levels][0]}'`;
  } else if (levels.size === LEVELS.length - 1) {
    const excluded = (LEVELS as readonly string[]).find((l) => !levels.has(l))!;
    clause = `@l <> '${excluded}'`;
  } else {
    clause = `@l in [${[...levels].map((l) => `'${l}'`).join(', ')}]`;
  }
  return stripped.trim() ? `${clause} and ${stripped.trim()}` : clause;
}

/**
 * Rewrites the `['service.name']` clause of `expr`. Bracket notation keeps the
 * parser treating it as one segment (matches the backend ServiceName fast-path).
 * Placed after any `@l` clause, before the rest of the user's expression.
 */
export function setServicesClause(expr: string, svcs: Set<string>): string {
  const stripped = stripFilterClause(expr, SERVICE_CLAUSE_RE);
  if (svcs.size === 0) return stripped;
  const clause = svcs.size === 1
    ? `['service.name'] = '${[...svcs][0]}'`
    : `['service.name'] in [${[...svcs].map(s => `'${s}'`).join(', ')}]`;
  const lvlMatch = stripped.match(/^(@l\s+(?:not\s+in|in)\s*\[[^\]]+\]|@l\s*(?:<>|!=|=)\s*'[^']*')(\s+and\s+|$)/i);
  if (lvlMatch) {
    const rest = stripped.slice(lvlMatch[0].length).trim();
    return rest ? `${lvlMatch[1]} and ${clause} and ${rest}` : `${lvlMatch[1]} and ${clause}`;
  }
  return stripped.trim() ? `${clause} and ${stripped.trim()}` : clause;
}

/** Comma-separated active levels for the `levels=` query param (undefined = all). */
export function levelsParam(levels: Set<string>): string | undefined {
  return levels.size === LEVELS.length ? undefined : [...levels].join(',');
}

export function fmtDateInput(d: Date): string {
  return format(d, 'yyyy-MM-dd HH:mm');
}

/** Parses `yyyy-MM-dd [HH:mm]` (also accepts legacy `dd/MM/yyyy [HH:mm]`). */
export function parseCustomDate(val: string): Date | null {
  if (!val) return null;
  const iso = val.match(/^(\d{4})-(\d{2})-(\d{2})(?:[\s,T]+(\d{1,2}):(\d{2}))?$/);
  if (iso) {
    const [, y, mo, d, h = '0', min = '0'] = iso;
    const dt = new Date(+y, +mo - 1, +d, +h, +min);
    return isNaN(dt.getTime()) ? null : dt;
  }
  const leg = val.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})(?:[\s,T]+(\d{1,2}):(\d{2}))?$/);
  if (leg) {
    const [, d, mo, y, h = '0', min = '0'] = leg;
    const dt = new Date(+y, +mo - 1, +d, +h, +min);
    return isNaN(dt.getTime()) ? null : dt;
  }
  return null;
}

/** The `from` timestamp for a preset (empty string for `custom`); `to` is always open (''). */
export function presetFrom(preset: TimePreset): string {
  if (preset === 'custom') return '';
  const now = new Date();
  const msMap: Partial<Record<TimePreset, number>> = { '5m': 5 * 60_000, '15m': 15 * 60_000, '30m': 30 * 60_000 };
  const daysMap: Partial<Record<TimePreset, number>> = { '1d': 1, '7d': 7, '2w': 14, '1mo': 30 };
  if (msMap[preset] !== undefined) return fmtDateInput(new Date(now.getTime() - msMap[preset]!));
  if (daysMap[preset] !== undefined) return fmtDateInput(startOfDay(subDays(now, daysMap[preset]!)));
  return '';
}
