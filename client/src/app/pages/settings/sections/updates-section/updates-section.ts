import {
  ChangeDetectionStrategy,
  Component,
  DestroyRef,
  OnInit,
  computed,
  inject,
  signal,
} from '@angular/core';
import { DatePipe } from '@angular/common';
import { LucideAngularModule } from 'lucide-angular';
import { SectionComponent } from '../../../../shared/components/ui';
import { ApiService } from '../../../../core/services/api.service';
import { UpdateStatusDto } from '../../../../core/models/update.model';

@Component({
  selector: 'app-updates-section',
  imports: [LucideAngularModule, SectionComponent, DatePipe],
  templateUrl: './updates-section.html',
  styleUrl: './updates-section.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class UpdatesSectionComponent implements OnInit {
  private readonly api = inject(ApiService);
  private readonly destroyRef = inject(DestroyRef);

  readonly loading = signal(true);
  readonly checking = signal(false);
  /** A download/apply POST is in flight (before the server phase reflects it). */
  readonly busy = signal(false);
  readonly status = signal<UpdateStatusDto | null>(null);
  readonly actionError = signal<string | null>(null);
  /** True once the server came back with a NEW version after an install. */
  readonly updated = signal(false);
  /** Set right after the install POST is accepted (server restarts shortly). */
  readonly installing = signal(false);

  /** Server-side self-update phase; 'idle' until a download starts. */
  readonly phase = computed(() => this.status()?.downloadPhase ?? 'idle');

  readonly canDownload = computed(() => {
    const s = this.status();
    return !!s && s.updateAvailable && s.canSelfUpdate && !this.busy()
        && (this.phase() === 'idle' || this.phase() === 'failed');
  });

  /** The verified installer on disk matches the latest release — awaiting approval. */
  readonly readyToInstall = computed(() => {
    const s = this.status();
    return !!s && this.phase() === 'ready' && s.downloadedVersion === s.latestVersion
        && !this.installing();
  });

  readonly progressPct = computed(() => {
    const s = this.status();
    if (!s || s.downloadTotalBytes <= 0) return 0;
    return Math.min(100, Math.round((s.downloadedBytes / s.downloadTotalBytes) * 100));
  });

  /** Platform-specific how-to shown when an update exists but the buttons can't do it. */
  readonly manualHint = computed<string | null>(() => {
    const s = this.status();
    if (!s || !s.updateAvailable || s.canSelfUpdate) return null;
    return s.platform === 'docker'
      ? 'Docker installs update by pulling the new image — point the container at the :latest tag, or add the Watchtower service from install/docker/docker-compose.example.yml for fully automatic updates.'
      : 'Download the new linux-x64 tar.gz from the release and re-run install.sh.';
  });

  private pollTimer: ReturnType<typeof setInterval> | null = null;

  ngOnInit(): void {
    this.refresh(() => this.loading.set(false));
    this.destroyRef.onDestroy(() => this.stopPolling());
  }

  private refresh(done?: () => void): void {
    this.api.getUpdateStatus().subscribe({
      next: s => {
        this.status.set(s);
        // A download already running server-side (e.g. page reload mid-download).
        if (s.downloadPhase === 'downloading') this.pollDownload();
        done?.();
      },
      error: () => done?.(),
    });
  }

  checkNow(): void {
    this.checking.set(true);
    this.api.checkForUpdates().subscribe({
      next: s => { this.status.set(s); this.checking.set(false); },
      error: () => this.checking.set(false),
    });
  }

  /** Phase 1: download + verify. No restart — safe to run any time. */
  download(): void {
    this.busy.set(true);
    this.actionError.set(null);
    this.api.downloadUpdate().subscribe({
      next: () => { this.busy.set(false); this.pollDownload(); },
      error: err => {
        this.busy.set(false);
        this.actionError.set(err?.error?.message ?? 'Download failed to start.');
      },
    });
  }

  /** Phase 2 — the explicit approval: run the installer, server restarts. */
  install(): void {
    const s = this.status();
    if (!s?.downloadedVersion) return;
    if (!confirm(`Install ${s.downloadedVersion} now? The server will restart and be briefly unavailable.`)) return;

    this.busy.set(true);
    this.actionError.set(null);
    this.api.applyUpdate().subscribe({
      next: () => {
        this.busy.set(false);
        this.installing.set(true);
        this.pollUntilRestarted(s.currentVersion);
      },
      error: err => {
        this.busy.set(false);
        this.actionError.set(err?.error?.message ?? 'Install failed to start.');
        this.refresh();
      },
    });
  }

  /** Fast poll while the server downloads — drives the progress bar. */
  private pollDownload(): void {
    this.stopPolling();
    this.pollTimer = setInterval(() => {
      this.api.getUpdateStatus().subscribe({
        next: s => {
          this.status.set(s);
          if (s.downloadPhase !== 'downloading') this.stopPolling();
        },
        error: () => { /* transient — keep polling */ },
      });
    }, 700);
  }

  /**
   * The installer stops the server mid-update, so requests fail for a while —
   * keep polling quietly until it answers with a different version, then flag
   * success (a reload picks up the freshly served SPA).
   */
  private pollUntilRestarted(fromVersion: string): void {
    this.stopPolling();
    const startedAt = Date.now();
    this.pollTimer = setInterval(() => {
      if (Date.now() - startedAt > 10 * 60 * 1000) { this.stopPolling(); this.installing.set(false); return; }
      this.api.getUpdateStatus().subscribe({
        next: s => {
          if (s.currentVersion !== fromVersion) {
            this.stopPolling();
            this.status.set(s);
            this.installing.set(false);
            this.updated.set(true);
          }
        },
        error: () => { /* server restarting — expected */ },
      });
    }, 5000);
  }

  reload(): void {
    window.location.reload();
  }

  mb(bytes: number): string {
    return (bytes / 1048576).toFixed(1);
  }

  private stopPolling(): void {
    if (this.pollTimer !== null) { clearInterval(this.pollTimer); this.pollTimer = null; }
  }
}
