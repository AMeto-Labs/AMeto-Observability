import { Injectable } from '@angular/core';
import { Subject } from 'rxjs';

export interface JvMenuRequest {
  /** Full filter-language path, e.g. "items[0].Id" or "Payload.Customer". */
  path: string;
  /** Raw value at the path — used to build equality literals for leaves. */
  rawValue: unknown;
  /** True for object/array nodes (→ `has(...)`); false for leaves (→ equality). */
  isContainer: boolean;
  /** Viewport coordinates of the click, for positioning the menu. */
  x: number;
  y: number;
}

/**
 * Bridges the recursive `<app-json-viewer>` tree to its host component's
 * context menu. Provided *per host* (e.g. each event-row) so every row owns its
 * own channel; nested viewers resolve the same instance through the element
 * injector. Optional — a viewer used without a provider simply hides its menu.
 */
@Injectable()
export class JsonViewerActions {
  readonly menu = new Subject<JvMenuRequest>();
  open(req: JvMenuRequest): void { this.menu.next(req); }
}

/** Safe identifier as accepted by the server-side filter lexer (letter/digit/_/@). */
const IDENT_RE = /^[A-Za-z_@][A-Za-z0-9_@]*$/;

/**
 * Appends an object key to a path, using bracket notation when the key is not a
 * bare identifier: `jvJoinKey('Headers', 'Api-Id')` → `Headers['Api-Id']`.
 */
export function jvJoinKey(prefix: string, key: string): string {
  if (IDENT_RE.test(key)) return prefix ? `${prefix}.${key}` : key;
  return `${prefix}['${key.replace(/'/g, "\\'")}']`;
}

/** Appends an array index: `jvJoinIndex('items', 0)` → `items[0]`. */
export function jvJoinIndex(prefix: string, index: number): string {
  return `${prefix}[${index}]`;
}

/** Converts a raw leaf value into a filter-language literal. */
export function jvLiteral(v: unknown): string {
  if (v === null || v === undefined) return 'null';
  if (typeof v === 'number' || typeof v === 'boolean') return String(v);
  return `'${String(v).replace(/'/g, "''")}'`;
}

/**
 * If `path` contains concrete array indices, returns the wildcard form that
 * matches *any* element: `items[0].id` → `items[%].id`. Returns `null` when the
 * path has no array index (so callers can hide the "any element" action).
 */
export function jvWildcard(path: string): string | null {
  return /\[\d+\]/.test(path) ? path.replace(/\[\d+\]/g, '[%]') : null;
}
