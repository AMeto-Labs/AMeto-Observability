import {
  Component, signal, inject, OnInit,
  ChangeDetectionStrategy, ChangeDetectorRef,
} from '@angular/core';
import { LucideAngularModule } from 'lucide-angular';
import { ApiService } from '../../core/services/api.service';
import { NodeDto } from '../../core/models/node.model';
import { EmptyStateComponent, PageHeaderComponent } from '../../shared/components/ui';

@Component({
  selector: 'app-nodes',
  imports: [LucideAngularModule, EmptyStateComponent, PageHeaderComponent],
  templateUrl: './nodes.html',
  styleUrl: './nodes.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class NodesComponent implements OnInit {
  private api = inject(ApiService);
  private cdr = inject(ChangeDetectorRef);

  nodes   = signal<NodeDto[]>([]);
  loading = signal(false);

  ngOnInit(): void {
    this.loading.set(true);
    this.api.getNodes().subscribe({
      next: list => { this.nodes.set(list); this.loading.set(false); this.cdr.markForCheck(); },
      error: () => { this.loading.set(false); this.cdr.markForCheck(); },
    });
  }
}
