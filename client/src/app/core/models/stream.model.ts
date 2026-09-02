/**
 * The payload of an SSE stream's terminal `done` frame — the server's own account of WHY the
 * stream ended, rather than the bare fact that it did.
 *
 * <p>Every field is optional, and that is not defensive typing for its own sake. The two log
 * streams end with a literal `data: {}`, and a server newer than this client may add fields or
 * stop sending one. A reader that assumes any field is present would turn a legacy or future
 * ending into a crash or, worse, into a confident false claim — so nothing here is guaranteed
 * and the component decides what an ending it cannot name is allowed to mean.</p>
 */
export interface StreamEndDto {
  /** True when the whole requested window was read out. Its ABSENCE is not `false`. */
  complete?:    boolean;
  /** Machine-readable ending: `exhausted` (read out) or `max-rows` (the row ceiling stopped it). */
  reason?:      string;
  /**
   * What ELSE went wrong on the way to that ending — `unread-segment` (a storage segment the
   * search had no room left to open) or `unreadable-segment` (one that would not open at all).
   * Omitted when nothing did, which is a positive claim: the stated `reason` is the ONLY reason
   * the list is short.
   */
  truncatedBy?: string;
}

/**
 * What a subscriber to an SSE-backed list receives: the rows, and — for a stream that ends —
 * one final frame carrying the ending itself.
 *
 * <p>The ending travels as a NEXT rather than as a callback beside the Observable because the
 * two facts must arrive in a fixed order. A side channel consulted from the `complete` handler
 * is the same information with the ordering left to whoever wired it up; a terminal value that
 * the stream emits before completing cannot be read early, cannot be missed, and cannot be
 * attributed to the wrong stream.</p>
 */
export type StreamFrame<T> =
  | { kind: 'row'; row: T }
  | { kind: 'end'; end: StreamEndDto };
