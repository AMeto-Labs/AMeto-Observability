import { ChangeDetectionStrategy, Component, signal } from '@angular/core';
import { LucideAngularModule } from 'lucide-angular';

interface Row { name: string; desc: string; ex?: string; }
interface FnGroup { title: string; icon: string; fns: Row[]; }
interface Section { id: string; label: string; icon: string; }

/**
 * In-app observability handbook: what Ameto stores (logs / traces / metrics /
 * alerts) and the full query-language reference for each. Content is static and
 * mirrors the server-side lexers/parsers (Ameto.Query.Filtering, TraceQL,
 * Ameto.Metrics) so it stays accurate against the running build.
 */
@Component({
  selector: 'app-reference',
  standalone: true,
  imports: [LucideAngularModule],
  templateUrl: './reference.html',
  styleUrl: './reference.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class ReferenceComponent {
  readonly active = signal('overview');

  readonly sections: Section[] = [
    { id: 'overview',   label: 'Overview',        icon: 'info' },
    { id: 'logs',       label: 'Logs',            icon: 'list' },
    { id: 'fields',     label: 'Built-in fields', icon: 'hash' },
    { id: 'operators',  label: 'Operators & keywords', icon: 'equal' },
    { id: 'functions',  label: 'Filter functions', icon: 'terminal' },
    { id: 'traces',     label: 'Traces / TraceQL', icon: 'activity' },
    { id: 'metrics',    label: 'Metrics',         icon: 'chart-bar' },
    { id: 'alerts',     label: 'Alerts',          icon: 'bell' },
  ];

  select(id: string): void {
    this.active.set(id);
    document.getElementById(id)?.scrollIntoView({ behavior: 'smooth', block: 'start' });
  }

  // ── Logs: CLEF built-in fields ─────────────────────────────────────────────
  readonly levels = ['Verbose', 'Debug', 'Information', 'Warning', 'Error', 'Fatal'];

  readonly fields: Row[] = [
    { name: '@t',  desc: 'Timestamp (ISO-8601, UTC).', ex: `@t > '2026-01-01'` },
    { name: '@l',  desc: 'Level: Verbose/Debug/Information/Warning/Error/Fatal.', ex: `@l = 'Error'` },
    { name: '@mt / @m', desc: 'Message template (raw, before rendering).', ex: `@mt like '%timeout%'` },
    { name: '@x',  desc: 'Exception — bare @x compares the exception type.', ex: `has(@x)` },
    { name: '@x.Type / @x.Message / @x.StackTrace', desc: 'Exception detail fields.', ex: `@x.Type = 'SqlException'` },
    { name: '@x.Inner.Type / @x.Inner.Message', desc: 'Inner (nested) exception.', ex: `has(@x.Inner.Type)` },
    { name: '@tr', desc: 'Trace id (W3C hex).', ex: `@tr = '4bf92f...'` },
    { name: '@sp', desc: 'Span id (hex).', ex: `@sp = 'a3ce9f...'` },
    { name: '@id', desc: 'Event id.', ex: `@id = '01H...'` },
    { name: `['service.name']`, desc: 'Service name (bracket form — dotted key).', ex: `['service.name'] = 'api'` },
  ];

  // ── Operators & keywords ───────────────────────────────────────────────────
  readonly operators: Row[] = [
    { name: '=  !=', desc: 'Equal / not equal. Strings compare case-insensitively; also works on numbers & bools.', ex: `@l != 'Debug'` },
    { name: '<  <=  >  >=', desc: 'Ordered comparison (numeric, string or coerced).', ex: `Elapsed > 500` },
    { name: '<>', desc: 'Alias for != (not equal).', ex: `@l <> 'Information'` },
  ];

  readonly keywords: Row[] = [
    { name: 'and · or · not', desc: 'Boolean logic. Group with parentheses.', ex: `@l = 'Error' and not has(@tr)` },
    { name: '( … )', desc: 'Grouping / precedence override.', ex: `(A = 1 or B = 2) and @l = 'Warning'` },
    { name: 'in [ … ]', desc: 'Membership test (case-insensitive). Bare list applies to @l.', ex: `@l in ['Error', 'Fatal']` },
    { name: 'not in [ … ]', desc: 'Negated membership.', ex: `@l not in ['Verbose', 'Debug']` },
    { name: 'like', desc: 'SQL-style pattern: % = any run, _ = one char, case-insensitive.', ex: `RequestPath like '/api/%'` },
    { name: 'free text', desc: 'Bare words → case-insensitive substring across message, exception type/message & string props. All terms must match.', ex: `timeout payment` },
    { name: 'bare level', desc: 'A lone level word is a level filter.', ex: `Error` },
    { name: 'array match', desc: 'Any comparison against an array property matches if ANY element matches.', ex: `Tags = 'urgent'` },
  ];

  // ── Filter functions (mirror of Ameto.Query.Filtering) ─────────────────────
  readonly fnGroups: FnGroup[] = [
    {
      title: 'Existence & type', icon: 'check-circle',
      fns: [
        { name: 'has(Prop)', desc: 'True when the property exists and is non-null.', ex: `has(UserId)` },
        { name: 'isDefined(Prop)', desc: 'Alias of has() — property is present.', ex: `isDefined(Headers.Auth)` },
        { name: `typeOf(Prop) op 'type'`, desc: `Value type: null / object / array / string / bool / number / undefined.`, ex: `typeOf(Payload) = 'object'` },
        { name: 'length(Prop) op n', desc: 'String length, or element count for an array.', ex: `length(Items) > 3` },
      ],
    },
    {
      title: 'Strings', icon: 'type',
      fns: [
        { name: `startsWith(Prop, 'x')`, desc: 'Prefix test (ordinal). ci_startsWith = case-insensitive.', ex: `startsWith(Path, '/api')` },
        { name: `contains(Prop, 'x')`, desc: 'Substring test. ci_contains = case-insensitive.', ex: `ci_contains(Msg, 'error')` },
        { name: `endsWith(Prop, 'x')`, desc: 'Suffix test. ci_endsWith = case-insensitive.', ex: `endsWith(File, '.json')` },
        { name: `toLower(Prop) op 'x'`, desc: 'Lower-case then compare. toUpper() for upper.', ex: `toLower(Env) = 'prod'` },
        { name: `substring(Prop, start[, len]) op 'x'`, desc: 'Compare a substring slice.', ex: `substring(Code, 0, 3) = 'ERR'` },
        { name: 'indexOf(Prop, \'x\') op n', desc: 'Ordinal index of substring (-1 if absent). lastIndexOf() from the end.', ex: `indexOf(Url, '?') >= 0` },
        { name: `replace(Prop, 'a', 'b') op 'x'`, desc: 'Replace all occurrences, then compare.', ex: `replace(Ver, 'v', '') = '2'` },
        { name: `concat(a, b, …) op 'x'`, desc: 'Concatenate ≥2 string props/literals, then compare.', ex: `concat(First, ' ', Last) = 'Jane Doe'` },
        { name: 'toNumber(Prop) op n', desc: 'Parse the value to a number, then compare.', ex: `toNumber(CountStr) >= 10` },
      ],
    },
    {
      title: 'JSON & XML', icon: 'braces',
      fns: [
        { name: 'fromJson(Prop) op value', desc: 'Parse a JSON string then compare the whole value.', ex: `fromJson(Body) = 42` },
        { name: 'fromJson(Prop).a.b op value', desc: 'Navigate parsed JSON (.field, [i], [\'key\']). Also supports like / in / has() and startsWith/contains/endsWith.', ex: `fromJson(Body).user.id = 7` },
        { name: `toJson(Prop) op 'json'`, desc: 'Serialize a value to a JSON string, then compare.', ex: `toJson(Tags) = '["a","b"]'` },
        { name: `fromXml(Prop, 'xpath') op v`, desc: 'Parse XML and extract via XPath, then compare.', ex: `fromXml(Soap, '//code') = '200'` },
      ],
    },
    {
      title: 'Collections', icon: 'brackets',
      fns: [
        { name: 'elementAt(Prop, i|key) op v', desc: 'Array element by index, or dict value by key.', ex: `elementAt(Items, 0) = 'first'` },
        { name: 'keys(Prop) op value', desc: 'Dict keys as a JSON array (= / != only).', ex: `keys(Headers) = '["A","B"]'` },
        { name: 'values(Prop) op value', desc: 'Dict values as a JSON array (= / != only).', ex: `values(Meta) != '[]'` },
      ],
    },
    {
      title: 'Numbers & math', icon: 'sigma',
      fns: [
        { name: 'round(Prop, places) op n', desc: 'Round (away from zero) then compare.', ex: `round(Ratio, 2) = 0.33` },
        { name: 'bucket(Prop, error) op n', desc: 'Snap a number to a log-scale bucket by relative error.', ex: `bucket(Latency, 0.1) = 500` },
        { name: `toHexString(Prop) op '0x…'`, desc: 'Integer to lower-case hex (0x-prefixed).', ex: `toHexString(Flags) = '0xff'` },
        { name: `toBase64(Prop) / fromBase64(Prop)`, desc: 'UTF-8 base64 encode / decode, then compare.', ex: `fromBase64(Token) = 'admin'` },
      ],
    },
    {
      title: 'Date & time', icon: 'clock',
      fns: [
        { name: 'now() op ticks', desc: 'Current UTC time in .NET ticks.', ex: `arrived(@t) < now()` },
        { name: 'dateTime(Prop) op value', desc: 'Parse a date/time string; compares as UTC ticks.', ex: `dateTime(Due) < now()` },
        { name: `toIsoString(Prop[, tzHours]) op 'iso'`, desc: 'Format an instant as ISO-8601 (optional UTC offset).', ex: `toIsoString(@t, 3) like '2026-%'` },
        { name: `datePart(Prop, 'part'[, tzHours]) op n`, desc: 'Extract year/month/day/hour/minute/second/weekday.', ex: `datePart(@t, 'hour') >= 9` },
        { name: 'timeOfDay(Prop, tzHours) op ticks', desc: 'Time-of-day (since midnight) in ticks at the given offset.', ex: `timeOfDay(@t, 0) > 0` },
        { name: 'timeSpan(Prop) op ticks', desc: 'Parse a TimeSpan string; compare in ticks.', ex: `timeSpan(Took) > 0` },
        { name: 'totalMilliseconds(Prop) op ms', desc: 'A TimeSpan (or ticks) as total milliseconds.', ex: `totalMilliseconds(Took) > 500` },
        { name: `toTimeString(Prop) op 'hh:mm:ss'`, desc: 'Ticks → TimeSpan text (constant "c" format).', ex: `toTimeString(Dur) = '00:00:05'` },
        { name: `offsetIn('TZ', Prop) op ticks`, desc: 'UTC offset of a time zone at that instant.', ex: `offsetIn('UTC', @t) = 0` },
        { name: 'arrived(Prop) op value', desc: 'Server ingest/arrival time of the event.', ex: `arrived(@t) > now()` },
      ],
    },
    {
      title: 'Regex', icon: 'regex',
      fns: [
        { name: `regexMatch(Prop, 'pattern')`, desc: 'Boolean .NET regex match (culture-invariant).', ex: `regexMatch(Ip, '^10\\.')` },
        { name: `regexExtract(Prop, 'pat'[, group]) op v`, desc: 'Extract a capture group (default 1) then compare.', ex: `regexExtract(Url, 'id=(\\d+)', 1) = '42'` },
      ],
    },
  ];

  // ── TraceQL (mirror of Ameto.Tracing.TraceQL) ──────────────────────────────
  readonly traceIntrinsics: Row[] = [
    { name: 'duration', desc: 'Span duration. Compare with a duration literal.', ex: `{ duration > 200ms }` },
    { name: 'status', desc: 'error / ok / unset (= and != only).', ex: `{ status = error }` },
    { name: 'service · service.name', desc: 'Span service name (= / !=).', ex: `{ service = 'api' }` },
    { name: 'name · span.name', desc: 'Span operation name (= / !=).', ex: `{ name = 'GET /users' }` },
    { name: 'kind · span.kind', desc: 'server / client / producer / consumer / internal.', ex: `{ kind = server }` },
    { name: 'http.status_code', desc: 'Promoted HTTP status (fast path; all operators).', ex: `{ http.status_code >= 500 }` },
    { name: '.any.attribute', desc: 'Any span attribute — leading dot required.', ex: `{ .db.system = 'mssql' }` },
  ];

  readonly traceExamples = [
    `{ .db.system = 'mssql' && duration > 200ms }`,
    `{ service = 'checkout' && status = error }`,
    `{ .http.request.method = 'POST' && http.status_code >= 500 }`,
    `{ name = 'GET /orders' || duration > 1s }`,
  ];

  // ── Metrics (mirror of Ameto.Metrics) ──────────────────────────────────────
  readonly metricAggs: Row[] = [
    { name: 'none', desc: 'Raw values (downsample only).' },
    { name: 'rate', desc: 'Per-second rate of a cumulative counter (reset-aware).' },
    { name: 'increase', desc: 'Total increase of a counter over each step.' },
    { name: 'avg / min / max / sum / last', desc: 'Scalar reduction across the step window.' },
    { name: 'quantile', desc: 'Histogram percentile (needs quantile 0–1, e.g. 0.95).' },
  ];

  readonly metricConcepts: Row[] = [
    { name: 'step', desc: 'Resolution — bucket width (auto-fit to chart width, or explicit e.g. 30s).' },
    { name: 'groupBy [labels]', desc: 'Split into one series per label combination.', ex: `groupBy: ['service']` },
    { name: 'filters {label: value}', desc: 'Keep only series matching label equalities.', ex: `{ service: 'api' }` },
    { name: 'topK', desc: 'Keep only the K highest series (by area).', ex: `topK: 5` },
    { name: 'expr (A op B)', desc: 'Binary of two queries: div / mul / add / sub, × scale — e.g. error ratio.', ex: `errors / total` },
    { name: 'exemplars', desc: 'Sampled points linked to a trace — click to jump into Traces.' },
    { name: 'heatmap', desc: 'Histogram distribution over time (bucket density).' },
  ];
}
