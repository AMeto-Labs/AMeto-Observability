import {
  Component, input, output, signal, inject, OnInit,
  ChangeDetectionStrategy, ChangeDetectorRef,
} from '@angular/core';
import { LucideAngularModule } from 'lucide-angular';
import { forkJoin } from 'rxjs';
import { ApiService } from '../../../core/services/api.service';
import { SearchHistoryService } from '../../../core/services/search-history.service';
import { AlertSeverity } from '../../../core/models/alert.model';

interface FiringAlert {
  id: string;
  name: string;
  severity: AlertSeverity;
  filter?: string;
}

/**
 * Right-hand quick-access panel on the events page:
 *   1. Log-source alert rules that are currently **firing** (top).
 *   2. The user's **search history** — pinned first, then recent.
 * Clicking any entry applies its filter to the events query.
 */
@Component({
  selector: 'app-signals-panel',
  imports: [LucideAngularModule],
  templateUrl: './signals-panel.html',
  styleUrl: './signals-panel.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class SignalsPanelComponent implements OnInit {
  private api = inject(ApiService);
  private cdr = inject(ChangeDetectorRef);
  readonly history = inject(SearchHistoryService);

  /** Current filter — kept for wiring compatibility with the events page. */
  readonly currentFilter = input<string>('');
  readonly filterSelected = output<string>();
  readonly closed = output<void>();

  readonly firing = signal<FiringAlert[]>([]);

  ngOnInit(): void {
    this.history.load();
    this.loadFiring();
  }

  /** Log-source rules whose live state is Firing. */
  private loadFiring(): void {
    forkJoin({ rules: this.api.getAlerts(), states: this.api.getAlertState() }).subscribe({
      next: ({ rules, states }) => {
        const firingIds = new Set(states.filter(s => s.state === 'Firing').map(s => s.ruleId));
        this.firing.set(
          rules
            .filter(r => r.source === 'Log' && firingIds.has(r.id))
            .map(r => ({ id: r.id, name: r.name, severity: r.severity, filter: r.filter })),
        );
        this.cdr.markForCheck();
      },
      error: () => this.cdr.markForCheck(),
    });
  }

  apply(filter: string | undefined): void {
    if (filter) this.filterSelected.emit(filter);
  }

  pin(query: string, pinned: boolean, e: MouseEvent): void {
    e.stopPropagation();
    this.history.setPinned(query, pinned);
  }

  remove(query: string, e: MouseEvent): void {
    e.stopPropagation();
    this.history.remove(query);
  }
}
