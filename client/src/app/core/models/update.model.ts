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
  /** True on Windows installs — the Apply endpoint can run the installer. */
  canSelfUpdate: boolean;
  applyInProgress: boolean;
}
