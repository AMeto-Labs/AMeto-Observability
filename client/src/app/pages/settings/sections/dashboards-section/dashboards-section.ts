import {
  ChangeDetectionStrategy, Component, OnInit, OnDestroy,
  inject, signal, ChangeDetectorRef,
} from '@angular/core';
import { LucideAngularModule } from 'lucide-angular';
import { SectionComponent } from '../../../../shared/components/ui';
import { ApiService } from '../../../../core/services/api.service';
import { DiagnosticsDto } from '../../../../core/models/diagnostics.model';
import { fmtBytes, fmtNum, fmtUptime, fmtStartedAt } from '../../../../shared/utils/format';

/**
 * System dashboard: disk / memory / storage / process vital signs, polled every
 * 10 s. Moved here from the former standalone Diagnostics page.
 */
@Component({
  selector: 'app-dashboards-section',
  imports: [LucideAngularModule, SectionComponent],
  templateUrl: './dashboards-section.html',
  styleUrl: './dashboards-section.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class DashboardsSectionComponent implements OnInit, OnDestroy {
  private readonly api = inject(ApiService);
  private readonly cdr = inject(ChangeDetectorRef);

  readonly data = signal<DiagnosticsDto | null>(null);
  readonly loading = signal(true);
  readonly refreshedAt = signal('');

  private timer?: ReturnType<typeof setInterval>;

  ngOnInit(): void {
    this.load();
    this.timer = setInterval(() => this.load(), 10_000);
  }

  ngOnDestroy(): void {
    if (this.timer) clearInterval(this.timer);
  }

  load(): void {
    this.api.getDiagnostics().subscribe({
      next: d => {
        this.data.set(d);
        this.loading.set(false);
        this.refreshedAt.set(new Date().toLocaleTimeString());
        this.cdr.markForCheck();
      },
      error: () => { this.loading.set(false); this.cdr.markForCheck(); },
    });
  }

  readonly fmtBytes     = fmtBytes;
  readonly fmtNum       = fmtNum;
  readonly fmtUptime    = fmtUptime;
  readonly fmtStartedAt = fmtStartedAt;

  diskPercent(free: number, total: number): number {
    return total === 0 ? 0 : Math.round(((total - free) / total) * 100);
  }

  ramClass(pct: number, target: number): string {
    if (pct >= target)      return 'danger';
    if (pct >= target - 10) return 'warning';
    return 'ok';
  }
}
