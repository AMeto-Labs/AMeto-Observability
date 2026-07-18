/** GET /api/system/update — software-update snapshot (admin only). */
export interface UpdateStatusDto {
  currentVersion: string;
  latestVersion: string | null;
  updateAvailable: boolean;
  releaseUrl: string | null;
  publishedAt: string | null;
  checkedAt: string | null;
  checkError: string | null;
  platform: 'windows' | 'linux' | 'docker';
  /** True on Windows installs — the download/apply endpoints can run the installer. */
  canSelfUpdate: boolean;

  /** Self-update state machine: download (with progress) → explicit install approval. */
  downloadPhase: 'idle' | 'downloading' | 'ready' | 'installing' | 'failed';
  downloadedBytes: number;
  downloadTotalBytes: number;
  downloadedVersion: string | null;
  downloadError: string | null;
}
