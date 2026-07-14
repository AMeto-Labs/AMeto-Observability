/** Per-user saved searches, as returned by GET /api/search-history. */
export interface SearchHistoryDto {
  /** Pinned queries, most-recent first (≤ 5). */
  pinned: string[];
  /** Recent (unpinned) queries, most-recent first (≤ 10). */
  recent: string[];
}
