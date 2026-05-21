import {
  Component, signal, computed, inject, OnDestroy,
  ChangeDetectionStrategy, ChangeDetectorRef,
} from '@angular/core';
import { LucideAngularModule } from 'lucide-angular';

import { ApiService } from '../../core/services/api.service';
import { EventDto } from '../../core/models/event.model';
import { EventRowComponent } from '../events/event-row/event-row';
import { EmptyStateComponent } from '../../shared/components/ui';

@Component({
  selector: 'app-live',
  imports: [LucideAngularModule, EventRowComponent, EmptyStateComponent],
  templateUrl: './live.html',
  styleUrl: './live.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class LiveComponent implements OnDestroy {
  private api = inject(ApiService);
  private cdr = inject(ChangeDetectorRef);

  events   = signal<EventDto[]>([]);
  running  = signal(false);
  private sub?: import('rxjs').Subscription;

  count = computed(() => this.events().length);

  toggle(): void {
    if (this.running()) {
      this.stop();
    } else {
      this.start();
    }
  }

  private start(): void {
    this.running.set(true);
    this.events.set([]);
    this.sub = this.api.streamLive().subscribe({
      next: ev => {
        this.events.update(es => [ev, ...es.slice(0, 999)]);
        this.cdr.markForCheck();
      },
      error: () => {
        this.running.set(false);
        this.cdr.markForCheck();
      },
    });
  }

  private stop(): void {
    this.running.set(false);
    this.sub?.unsubscribe();
  }

  ngOnDestroy(): void { this.stop(); }
}
