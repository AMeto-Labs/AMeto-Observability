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
    this.api.getSignals().subscribe({
      next: list => { this.signals.set(list); this.loading.set(false); this.cdr.markForCheck(); },
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
    this.api.createSignal({ name: this.draftName.trim(), filter: this.currentFilter() || undefined, enabled: true })
      .subscribe({
        next: () => { this.saving.set(false); this.creating.set(false); this.load(); },
        error: () => { this.saving.set(false); this.cdr.markForCheck(); },
      });
  }

  applyFilter(filter: string | undefined): void {
    if (filter) this.filterSelected.emit(filter);
  }

  toggle(s: AlertRule, event: MouseEvent): void {
    event.stopPropagation();
    this.api.updateSignal(s.id, { name: s.name, enabled: !s.enabled })
      .subscribe(() => this.load());
  }

  delete(id: string, event: MouseEvent): void {
    event.stopPropagation();
    this.api.deleteSignal(id).subscribe(() => this.load());
  }
}
