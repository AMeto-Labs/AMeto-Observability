/**
 * Which page's history a call addresses. The server keys rows by
 * (username, scope, query), so the three signal pages never see each other's queries.
 */
export type SearchScope = 'logs' | 'traces' | 'metrics';

/** Per-user saved searches, as returned by GET /api/search-history. */
export interface SearchHistoryDto {
  /** Pinned queries, most-recent first (≤ 5). */
  pinned: string[];
  /** Recent (unpinned) queries, most-recent first (≤ 10). */
  recent: string[];
}
