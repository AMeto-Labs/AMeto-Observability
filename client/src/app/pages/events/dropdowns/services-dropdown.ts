import { ChangeDetectionStrategy, Component, computed, inject, signal } from '@angular/core';
import { LucideAngularModule } from 'lucide-angular';

import { OverlayPanelRef } from '../../../shared/services/overlay';

/** Input handed to the services dropdown via {@link OverlayPanelRef.data}. */
export interface ServicesDropdownData {
  /** Services currently selected in the filter. */
  selected: ReadonlySet<string>;
  /** Full available service list (backend-known ∪ services seen in loaded events), sorted. */
  services: string[];
  /** Event counts keyed by service name. */
  counts: Record<string, number>;
  /** Total loaded events (denominator for the per-service percentage). */
  total: number;
}

/** Resolved via {@link OverlayPanelRef.close} on Apply; `undefined` when dismissed. */
export type ServicesDropdownResult = Set<string>;

const SERVICE_COLORS = [
  '#4DA3FF', '#38BDF8', '#34D399', '#A78BFA',
  '#FB923C', '#F472B6', '#22D3EE', '#818CF8',
  '#E879F9', '#4ADE80', '#FACC15', '#F87171',
];

@Component({
  selector: 'app-services-dropdown',
  standalone: true,
  imports: [LucideAngularModule],
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: `
    <div class="dd-panel svc-dd">
      <div class="dd-panel-header">
        <span class="dd-panel-title">Services</span>
        <button class="dd-close-btn" title="Close" (click)="ref.close()">
          <lucide-icon name="x" [size]="13" />
        </button>
      </div>

      <div class="svc-search-wrap">
        <lucide-icon name="search" [size]="12" class="svc-search-icon" />
        <input
          type="text"
          class="svc-search-input"
          placeholder="Search services"
          [value]="search()"
          (input)="search.set($any($event.target).value)" />
      </div>

      <label class="dd-select-all-row">
        <input
          type="checkbox"
          [checked]="allActive()"
          [indeterminate]="!allActive() && pending().size < filtered().length"
          (change)="reset()" />
        <span class="dd-select-all-lbl">Select all</span>
        <span class="dd-total-count">{{ ref.data.services.length }}</span>
      </label>

      <div class="dd-scroll">
        @for (svc of (showMore() ? filtered() : filtered().slice(0, 8)); track svc) {
          <label class="svc-dd-row">
            <input type="checkbox" [checked]="isActive(svc)" (change)="toggle(svc)" />
            <span class="svc-color-dot" [style.background]="serviceColor(svc)"></span>
            <span class="svc-name">{{ svc }}</span>
            <span class="svc-count">{{ fmtCount(ref.data.counts[svc] || 0) }}</span>
            <span class="svc-pct">{{ servicePercent(svc) }}</span>
          </label>
        }
        @if (!showMore() && filtered().length > 8) {
          <button class="svc-show-more" (click)="showMore.set(true)">
            Show {{ filtered().length - 8 }} more
          </button>
        }
        @if (!filtered().length) {
          <div class="svc-empty">No services found</div>
        }
      </div>

      <div class="dd-footer-actions">
        <button class="dd-reset-btn" (click)="reset()">Reset</button>
        <button class="dd-apply-btn" (click)="apply()">Apply</button>
      </div>
    </div>
  `,
})
export class ServicesDropdownComponent {
  readonly ref = inject<OverlayPanelRef<ServicesDropdownResult, ServicesDropdownData>>(OverlayPanelRef);

  readonly pending = signal(new Set(this.ref.data.selected));
  readonly search = signal('');
  readonly showMore = signal(false);

  /** Empty selection means "all services" — matches the filter having no service clause. */
  readonly allActive = computed(() => this.pending().size === 0);

  readonly filtered = computed(() => {
    const q = this.search().toLowerCase();
    const all = this.ref.data.services;
    return q ? all.filter((s) => s.toLowerCase().includes(q)) : all;
  });

  isActive(svc: string): boolean {
    return this.pending().has(svc);
  }

  toggle(svc: string): void {
    this.pending.update((set) => {
      const next = new Set(set);
      next.has(svc) ? next.delete(svc) : next.add(svc);
      return next;
    });
  }

  reset(): void {
    this.pending.set(new Set());
  }

  apply(): void {
    this.ref.close(new Set(this.pending()));
  }

  servicePercent(svc: string): string {
    const total = this.ref.data.total;
    if (!total) return '0%';
    return `${Math.round(((this.ref.data.counts[svc] ?? 0) / total) * 100)}%`;
  }

  serviceColor(name: string): string {
    let h = 0;
    for (let i = 0; i < name.length; i++) h = (h * 31 + name.charCodeAt(i)) >>> 0;
    return SERVICE_COLORS[h % SERVICE_COLORS.length];
  }

  fmtCount(n: number | undefined): string {
    if (!n) return '0';
    return n >= 1000 ? `${(n / 1000).toFixed(1)}K` : String(n);
  }
}
