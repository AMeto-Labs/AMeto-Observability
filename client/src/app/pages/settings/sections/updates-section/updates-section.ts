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
  readonly applying = signal(false);
  readonly status = signal<UpdateStatusDto | null>(null);
  /** Message shown after Apply was accepted ("installer started…"). */
  readonly applyMessage = signal<string | null>(null);
  readonly applyError = signal<string | null>(null);
  /** True once the server came back with a NEW version after an update. */
  readonly updated = signal(false);

  readonly canApply = computed(() => {
    const s = this.status();
    return !!s && s.updateAvailable && s.canSelfUpdate && !this.applying() && !s.applyInProgress;
  });

  /** Platform-specific how-to shown when an update exists but the button can't do it. */
  readonly manualHint = computed<string | null>(() => {
    const s = this.status();
    if (!s || !s.updateAvailable || s.canSelfUpdate) return null;
    return s.platform === 'docker'
      ? 'Docker installs update by pulling the new image — point the container at the :latest tag, or add the Watchtower service from install/docker/docker-compose.example.yml for fully automatic updates.'
      : 'Download the new linux-x64 tar.gz from the release and re-run install.sh.';
  });

  private pollTimer: ReturnType<typeof setInterval> | null = null;

  ngOnInit(): void {
    this.api.getUpdateStatus().subscribe({
      next: s => { this.status.set(s); this.loading.set(false); },
      error: () => this.loading.set(false),
    });
    this.destroyRef.onDestroy(() => this.stopPolling());
  }

  checkNow(): void {
    this.checking.set(true);
    this.api.checkForUpdates().subscribe({
      next: s => { this.status.set(s); this.checking.set(false); },
      error: () => this.checking.set(false),
    });
  }

  apply(): void {
    const s = this.status();
    if (!s?.latestVersion) return;
    if (!confirm(`Update to ${s.latestVersion}? The server will restart and be briefly unavailable.`)) return;

    this.applying.set(true);
    this.applyError.set(null);
    this.api.applyUpdate().subscribe({
      next: r => {
        this.applyMessage.set(r.message);
        this.pollUntilRestarted(s.currentVersion);
      },
      error: err => {
        this.applying.set(false);
        this.applyError.set(err?.error?.message ?? 'Update failed to start.');
      },
    });
  }

  /**
   * The installer stops the server mid-update, so requests fail for a while —
   * keep polling quietly until it answers with a different version, then flag
   * success (a reload picks up the freshly served SPA).
   */
  private pollUntilRestarted(fromVersion: string): void {
    const startedAt = Date.now();
    this.pollTimer = setInterval(() => {
      if (Date.now() - startedAt > 10 * 60 * 1000) { this.stopPolling(); this.applying.set(false); return; }
      this.api.getUpdateStatus().subscribe({
        next: s => {
          if (s.currentVersion !== fromVersion) {
            this.stopPolling();
            this.status.set(s);
            this.applying.set(false);
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

  private stopPolling(): void {
    if (this.pollTimer !== null) { clearInterval(this.pollTimer); this.pollTimer = null; }
  }
}
