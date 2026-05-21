import { Component, signal, inject, OnInit, OnDestroy, ChangeDetectionStrategy, ChangeDetectorRef } from '@angular/core';
import { LucideAngularModule } from 'lucide-angular';
import { ApiService } from '../../core/services/api.service';
import { DiagnosticsDto } from '../../core/models/diagnostics.model';
import { fmtBytes, fmtNum, fmtUptime, fmtStartedAt } from '../../shared/utils/format';
import { PageHeaderComponent, SectionComponent } from '../../shared/components/ui';

@Component({
  selector: 'app-diagnostics',
  imports: [LucideAngularModule, PageHeaderComponent, SectionComponent],
  templateUrl: './diagnostics.html',
  styleUrl: './diagnostics.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class DiagnosticsComponent implements OnInit, OnDestroy {
  private api = inject(ApiService);
  private cdr = inject(ChangeDetectorRef);

  loading    = signal(true);
  data       = signal<DiagnosticsDto | null>(null);
  refreshedAt = signal<string>('');
  private _interval: ReturnType<typeof setInterval> | null = null;

  ngOnInit(): void {
    this.load();
    this._interval = setInterval(() => this.load(), 10_000);
  }

  ngOnDestroy(): void {
    if (this._interval) clearInterval(this._interval);
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

  readonly fmtBytes    = fmtBytes;
  readonly fmtNum      = fmtNum;
  readonly fmtUptime   = fmtUptime;
  readonly fmtStartedAt = fmtStartedAt;

  diskPercent(free: number, total: number): number {
    if (total === 0) return 0;
    return Math.round(((total - free) / total) * 100);
  }

  ramClass(pct: number, target: number): string {
    if (pct >= target)            return 'danger';
    if (pct >= target - 10) return 'warning';
    return 'ok';
  }
}
