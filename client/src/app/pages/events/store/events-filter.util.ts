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

// ── The services picker's clause ────────────────────────────────────────────
//
// The picker owns ONE clause: a top-level AND conjunct `@service = '…'` / `@service in […]` —
// under any name the server answers for the field. Only such a conjunct is a SELECTION: it
// constrains every row. The same text under a `not`, or beside a top-level `or`, does not, and
// reading it as one hid rows the server had rightly returned. The field names are matched
// ORDINALLY, as the server matches its aliases (`@SERVICE` is a user property there); the `in`
// keyword, like every keyword, in any case.

/** PropertyPath.Separator: what the server's parser joins a property path's segments with. */
const PATH_SEP = '\u0001';

/**
 * The KEYS the server resolves to the field (BuiltinFields, the ServiceName row): `@service`;
 * `ServiceName`; `service.name` as one key, which only a bracket writes (`['service.name']`, the
 * spelling the picker wrote before `@service` — saved searches, history and shared URLs still
 * carry it); and the path `service`/`name`, which bare `service.name` becomes, and
 * `service['name']`, `['service'].name` and `['service']['name']` too.
 *
 * The picker used to match SPELLINGS, with a regex, and every spelling it missed kept its clause
 * as the user's own text while a pick ANDed `@service = 'new'` in front of it: two service
 * clauses that contradict, and an empty page. So a conjunct is now read the way the server reads
 * it ({@link lex}, {@link readPath}) and the KEY that comes out is what is compared — ordinally,
 * as the server compares it, so `service.namespace`, `servicename` and `service['name']['x']`
 * stay properties here as they are there.
 */
const SERVICE_KEYS: ReadonlySet<string> =
  new Set(['@service', 'ServiceName', 'service.name', `service${PATH_SEP}name`]);

/** A token of the server's filter lexer, as far as the picker's clause needs one. */
interface Token {
  kind: 'ident' | 'string' | 'number' | 'punct' | 'op';
  /** An identifier as written; a string's value, its escapes undone; otherwise the symbol. */
  text: string;
}

/** `char.IsLetter` / `char.IsDigit`, the lexer's tests — one UTF-16 unit at a time, as there. */
const isLetter = (ch: string) => /\p{L}/u.test(ch);
const isDigit = (ch: string | undefined) => ch !== undefined && /\p{Nd}/u.test(ch);
const isIdentPart = (ch: string) =>
  ch === '@' || ch === '_' || ch === '.' || isLetter(ch) || isDigit(ch);

const isPunct = (t: Token | undefined, ch: string) => t?.kind === 'punct' && t.text === ch;

/**
 * `src` in the server lexer's tokens (Lexer.Tokenise), rule for rule: whitespace separates
 * tokens; `'…'` is a string in which `\x` is x and `''` is a quote; `( ) [ ] ,` stand alone; a
 * `.` not before a digit is a dot; `= != <> < <= > >=` are operators; a number starts with `-`,
 * a digit or `.digit`; an identifier starts with `@`, `_` or a letter and runs on through those,
 * digits AND DOTS; any other character is skipped, as the lexer skips it.
 *
 * One departure: a string still open at the end — which the lexer reads to the end — makes this
 * null. That is a draft being typed (`@service = 'pay`), and the picker reads nothing from it.
 */
function lex(src: string): Token[] | null {
  const out: Token[] = [];
  let i = 0;
  while (i < src.length) {
    const c = src[i];
    const two = src.slice(i, i + 2);
    if (/\s/.test(c)) {
      i++;
    } else if (c === "'") {
      let text = '';
      for (i++; ; ) {
        if (i >= src.length) return null;
        if (src[i] === '\\' && i + 1 < src.length) { text += src[i + 1]; i += 2; }
        else if (src[i] === "'" && src[i + 1] === "'") { text += "'"; i += 2; }
        else if (src[i] === "'") { i++; break; }
        else { text += src[i]; i++; }
      }
      out.push({ kind: 'string', text });
    } else if ('()[],'.includes(c) || (c === '.' && !isDigit(src[i + 1]))) {
      out.push({ kind: 'punct', text: c });
      i++;
    } else if (two === '!=' || two === '<=' || two === '<>' || two === '>=') {
      out.push({ kind: 'op', text: two });
      i += 2;
    } else if (c === '=' || c === '<' || c === '>') {
      out.push({ kind: 'op', text: c });
      i++;
    } else if (c === '-' || c === '.' || isDigit(c)) {        // a `.` here has a digit after it
      let j = c === '-' ? i + 1 : i;
      while (j < src.length && (src[j] === '.' || isDigit(src[j]))) j++;
      out.push({ kind: 'number', text: src.slice(i, j) });
      i = j;
    } else if (c === '@' || c === '_' || isLetter(c)) {
      let j = i + 1;
      while (j < src.length && isIdentPart(src[j])) j++;
      out.push({ kind: 'ident', text: src.slice(i, j) });
      i = j;
    } else {
      i++;
    }
  }
  return out;
}

/**
 * The key the server's parser makes of the property path starting at `t[p]`
 * (FilterParser.ReadPropertyPath) — an identifier split at its dots, a bracketed string kept whole
 * as one segment, then any run of `.identifier` and `['segment']`, all joined with U+0001 — and
 * the position after it; null when no path starts there. A bracketed number, an array index, is
 * not read: no key of the field has one, so a clause holding one is not the picker's.
 */
function readPath(t: readonly Token[], p: number): { key: string; end: number } | null {
  const bracketed = (k: number): string | null =>
    isPunct(t[k], '[') && t[k + 1]?.kind === 'string' && isPunct(t[k + 2], ']')
      ? t[k + 1].text
      : null;

  const segs: string[] = [];
  const first = bracketed(p);
  if (first !== null) { segs.push(first); p += 3; }
  else if (t[p]?.kind === 'ident') { segs.push(...t[p].text.split('.')); p++; }
  else return null;

  for (;;) {
    const seg = bracketed(p);
    if (seg !== null) {
      segs.push(seg);
      p += 3;
    } else if (isPunct(t[p], '.') && t[p + 1]?.kind === 'ident') {
      segs.push(...t[p + 1].text.split('.'));
      p += 2;
    } else {
      return { key: segs.join(PATH_SEP), end: p };
    }
  }
}

/**
 * The services a conjunct SELECTS — `field = 'x'` or `field in ['x', …]`, `field` being any
 * spelling the server resolves to the built-in service — or null for anything else: an exclusion
 * (`<>`, `not in`), another property, a comparison with something that is not a name, a list
 * holding one, or text after the clause. A name is never empty.
 */
function serviceSelection(conjunct: string): string[] | null {
  const t = lex(conjunct);
  const path = t && readPath(t, 0);
  if (!t || !path || !SERVICE_KEYS.has(path.key)) return null;

  const isName = (k: number) => t[k]?.kind === 'string' && t[k].text.length > 0;
  let p = path.end;
  if (t[p]?.kind === 'op' && t[p].text === '=')
    return isName(p + 1) && p + 2 === t.length ? [t[p + 1].text] : null;

  if (t[p]?.kind !== 'ident' || t[p].text.toLowerCase() !== 'in' || !isPunct(t[p + 1], '['))
    return null;
  const names: string[] = [];
  for (p += 2; isName(p); ) {
    names.push(t[p++].text);
    if (!isPunct(t[p], ',')) break;
    p++;                                                      // the parser takes a trailing comma
  }
  return names.length > 0 && isPunct(t[p], ']') && p + 1 === t.length ? names : null;
}

/** The picker's oldest clause, `(service.name = 'x' or ApplicationContext = 'x')`: replaced, never read. */
const LEGACY_SERVICE_OR_CLAUSE =
  /^\(service\.name\s*=\s*'[^']*'\s*or\s*ApplicationContext\s*=\s*'[^']*'\)$/;

/** A top-level connective at the scan position: whitespace, `and`/`or`, whitespace. */
const CONNECTIVE_AT = /^\s+(and|or)\s+/i;

/**
 * The top-level AND conjuncts of `expr` — split outside quotes, brackets and parentheses — or
 * null when `expr` has a top-level `or`, where no single conjunct constrains every row.
 */
function topLevelConjuncts(expr: string): string[] | null {
  const parts: string[] = [];
  let depth = 0;
  let quoted = false;
  let start = 0;
  for (let i = 0; i < expr.length; i++) {
    const c = expr[i];
    if (quoted) {
      if (c === '\\') i++;                                   // \' escapes a quote
      else if (c === "'") {
        if (expr[i + 1] === "'") i++;                        // '' is a quote too
        else quoted = false;
      }
      continue;
    }
    if (c === "'") { quoted = true; continue; }
    if (c === '(' || c === '[') { depth++; continue; }
    if (c === ')' || c === ']') { depth--; continue; }
    if (depth !== 0 || !/\s/.test(c)) continue;
    const m = CONNECTIVE_AT.exec(expr.slice(i));
    if (!m) continue;
    if (m[1].toLowerCase() === 'or') return null;
    parts.push(expr.slice(start, i).trim());
    i += m[0].length - 1;
    start = i + 1;
  }
  parts.push(expr.slice(start).trim());
  return parts.filter(p => p.length > 0);
}

/**
 * True for a conjunct that selects services, under any name of the field, or for the picker's
 * oldest clause.
 */
function isServiceClause(conjunct: string): boolean {
  return serviceSelection(conjunct) !== null || LEGACY_SERVICE_OR_CLAUSE.test(conjunct);
}

/** Milliseconds between .NET DateTime min (0001-01-01 UTC) and Unix epoch (1970-01-01 UTC). */
const DOTNET_TICKS_UNIX_EPOCH_MS = 62_135_596_800_000;

/** Built-in tokens always offered by the filter autocomplete popup. */
export const BUILTIN_SUGGESTIONS = [
  '@l', '@mt', '@t', '@x', '@x.Type', '@x.Message', '@x.StackTrace',
  '@i', '@r', '@sp', '@tr', '@service',
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
 * Converts an ISO-8601 timestamp into .NET UTC ticks (100 ns since 0001-01-01) as a
 * decimal string, preserving the full 7-digit fractional second.
 *
 * `new Date(iso).getTime()` floors to milliseconds, and the server's keyset cursor
 * compares the timestamp strictly first — so every event whose ticks fell between the
 * floored cursor and the boundary event's true ticks was silently skipped at each
 * infinite-scroll page boundary (at high ingest rates that is dozens of events per
 * millisecond). Ticks also exceed Number.MAX_SAFE_INTEGER (6.4e17 > 2^53), so the math
 * runs in BigInt and the result travels as a string.
 */
export function isoToDotNetUtcTicksString(iso: string): string {
  const m = /^(\d{4}-\d{2}-\d{2}[Tt ]\d{2}:\d{2}:\d{2})(?:\.(\d+))?(.*)$/.exec(iso);
  if (m) {
    const baseMs = Date.parse(m[1] + m[3]); // seconds-precision part keeps its own tz suffix
    if (!Number.isNaN(baseMs)) {
      const fracTicks = (m[2] ?? '').padEnd(7, '0').slice(0, 7);
      return ((BigInt(baseMs) + BigInt(DOTNET_TICKS_UNIX_EPOCH_MS)) * 10_000n + BigInt(fracTicks)).toString();
    }
  }
  // Unrecognised shape — fall back to millisecond precision rather than fail the query.
  return String(msToDotNetUtcTicks(new Date(iso).getTime()));
}

/**
 * Removes all matches of `pattern` from a filter expression, then cleans up
 * dangling `and`/`or` connectives and extra whitespace — used to splice out a
 * previous clause before inserting a new one, without erasing the user's own text.
 */
export function stripFilterClause(expr: string, pattern: RegExp): string {
  let out = expr
    .replace(pattern, '')
    // Two connectives left facing each other where the clause used to be. Only `and and`
    // was collapsed, so a query joined with `or` — or with a mix — left `or  or` behind,
    // which the server now refuses outright instead of quietly mis-reading. The user never
    // typed the broken string: a checkbox click built it.
    .replace(/\s+(?:and|or)(?:\s+(?:and|or))+\s+/gi, (m) => (/\bor\b/i.test(m) ? ' or ' : ' and '));

  // Until it settles. One pass was not enough: stripping the level clause out of
  // `not @l = 'Error' and X` leaves `not  and X`, where the collapse above does not fire — it
  // wants two connectives side by side — so removing the leading `not` left `and X`, a filter
  // opening with a dangling connective, patched straight in and sent. A checkbox click built
  // that string; the user never typed it.
  let previous: string;
  do {
    previous = out;
    out = out
      .replace(/^\s*(?:and|or|not)\s+/i, '')
      .replace(/\s+(?:and|or|not)\s*$/i, '')
      .trim();
  } while (out !== previous);

  return out;
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

/**
 * The services the filter SELECTS: those named by the picker's clause when it is a top-level AND
 * conjunct (any spelling of the field). Empty — "all services" — when there is none, including
 * when the clause sits under a `not` or beside a top-level `or`, where it selects nothing.
 */
export function parseServicesFromFilter(expr: string): Set<string> {
  for (const conjunct of topLevelConjuncts(expr) ?? []) {
    const names = serviceSelection(conjunct);
    if (names) return new Set(names);
  }
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
 * `s` as a string literal the server's lexer reads back as `s`. It ends a string at a lone quote
 * and takes a backslash as an escape of the character after it, so `'O'Brien'` was the name `O`
 * followed by text it could not use, and `'DOMAIN\svc'` the name `DOMAINsvc`. The quote is
 * doubled, as `jvLiteral` writes it; the backslash is escaped.
 */
function quoted(s: string): string {
  return `'${s.replace(/\\/g, '\\\\').replace(/'/g, "''")}'`;
}

/**
 * Rewrites the picker's service clause of `expr` as `@service = …` / `@service in […]` — the
 * built-in field's own name, which the server answers from the event header and its index. Only
 * the picker's own clause (a top-level AND conjunct, in any spelling of the field) is replaced —
 * never duplicated — and a service test the user wrote under a `not` or inside an `or` is left
 * as written. When `expr` has a top-level `or`, it is parenthesised so the selection applies to
 * all of it. Placed after any `@l` clause, before the rest of the user's expression.
 */
export function setServicesClause(expr: string, svcs: Set<string>): string {
  const conjuncts = topLevelConjuncts(expr);
  const stripped = conjuncts
    ? conjuncts.filter(c => !isServiceClause(c)).join(' and ')
    : `(${expr.trim()})`;
  if (svcs.size === 0) return conjuncts ? stripped : expr.trim();
  const clause = svcs.size === 1
    ? `@service = ${quoted([...svcs][0])}`
    : `@service in [${[...svcs].map(quoted).join(', ')}]`;
  const lvlMatch = stripped.match(/^(@l\s+(?:not\s+in|in)\s*\[[^\]]+\]|@l\s*(?:<>|!=|=)\s*'[^']*')(\s+and\s+|$)/i);
  if (lvlMatch) {
    const rest = stripped.slice(lvlMatch[0].length).trim();
    return rest ? `${lvlMatch[1]} and ${clause} and ${rest}` : `${lvlMatch[1]} and ${clause}`;
  }
  return stripped.trim() ? `${clause} and ${stripped.trim()}` : clause;
}

/**
 * Does this query LOOK like it asks for an aggregation table rather than a list of events?
 *
 * A conservative hint, not the definition. The server decides, by tokenising: its lexer drops
 * any character it has no meaning for, so `select "count"(*)` — quoting an identifier out of
 * SQL habit — is an aggregation to it and not to this regex. The divergence is deliberately
 * ONE-WAY. Under-detecting costs a wasted round trip that `loadEvents` then corrects, because
 * the search endpoint answers such a query by naming the aggregate endpoint. Over-detecting
 * would send an ordinary search to the wrong endpoint and turn it into an error — and `select`
 * is not a reserved word, so `select the cheapest plan` has to stay a text search.
 *
 * Keep this narrower than the server, never wider.
 */
export function isAggregationQuery(expr: string): boolean {
  return /^\s*select\s+(count|sum|min|max|avg)\s*\(/i.test(expr);
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
