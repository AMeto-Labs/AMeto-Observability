import {
  Component, input, output, signal, inject, OnInit,
  ChangeDetectionStrategy, ChangeDetectorRef,
} from '@angular/core';
import { FormsModule } from '@angular/forms';
import { LucideAngularModule } from 'lucide-angular';
import { ApiService } from '../../../core/services/api.service';
import { AlertRule } from '../../../core/models/alert.model';

@Component({
  selector: 'app-signals-panel',
  imports: [FormsModule, LucideAngularModule],
  templateUrl: './signals-panel.html',
  styleUrl: './signals-panel.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class SignalsPanelComponent implements OnInit {
  private api = inject(ApiService);
  private cdr = inject(ChangeDetectorRef);

  readonly currentFilter = input<string>('');
  readonly filterSelected = output<string>();
  readonly closed = output<void>();

  signals  = signal<AlertRule[]>([]);
  loading  = signal(true);
  creating = signal(false);
  saving   = signal(false);
  draftName = '';

  ngOnInit(): void { this.load(); }

  load(): void {
    this.loading.set(true);
    this.api.getAlerts().subscribe({
      next: (list: AlertRule[]) => { this.signals.set(list); this.loading.set(false); this.cdr.markForCheck(); },
      error: ()   => { this.loading.set(false); this.cdr.markForCheck(); },
    });
  }

  startCreate(): void {
    this.draftName = '';
    this.creating.set(true);
  }

  cancelCreate(): void { this.creating.set(false); }

  saveNew(): void {
    if (!this.draftName.trim()) return;
    this.saving.set(true);
    this.api.createAlert({
      name: this.draftName.trim(),
      enabled: true,
      severity: 'Warning',
      source: 'Log',
      comparator: 'GreaterOrEqual',
      threshold: 1,
      windowSeconds: 300,
      forSeconds: 0,
      cooldownSeconds: 900,
      filter: this.currentFilter() || undefined,
    }).subscribe({
        next: () => { this.saving.set(false); this.creating.set(false); this.load(); },
        error: () => { this.saving.set(false); this.cdr.markForCheck(); },
      });
  }

  applyFilter(filter: string | undefined): void {
    if (filter) this.filterSelected.emit(filter);
  }

  toggle(s: AlertRule, event: MouseEvent): void {
    event.stopPropagation();
    this.api.updateAlert(s.id, {
      name: s.name, enabled: !s.enabled, severity: s.severity, source: s.source,
      comparator: s.comparator, threshold: s.threshold,
      windowSeconds: 300, forSeconds: 0, cooldownSeconds: 900,
      filter: s.filter, metric: s.metric, aggregation: s.aggregation, quantile: s.quantile,
      service: s.service, traceMetric: s.traceMetric, channels: s.channels, template: s.template,
    }).subscribe(() => this.load());
  }

  delete(id: string, event: MouseEvent): void {
    event.stopPropagation();
    this.api.deleteAlert(id).subscribe(() => this.load());
  }
}
