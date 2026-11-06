import {
  ChangeDetectionStrategy,
  Component,
  computed,
  effect,
  inject,
  signal,
  OnInit,
} from '@angular/core';
import { FormsModule } from '@angular/forms';
import { LucideAngularModule } from 'lucide-angular';
import { SectionComponent } from '../../../../shared/components/ui';
import { ApiService } from '../../../../core/services/api.service';
import { RetentionDto, RetentionRunResult } from '../../../../core/models/retention.model';
import { SettingsDirtyService } from '../../settings-dirty.service';

interface DayField {
  key: keyof RetentionDto;
  label: string;
  dotClass?: string;
  icon?: string;
}

@Component({
  selector: 'app-retention-section',
  imports: [LucideAngularModule, FormsModule, SectionComponent],
  templateUrl: './retention-section.html',
  styleUrl: './retention-section.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class RetentionSectionComponent implements OnInit {
  private readonly api = inject(ApiService);
  private readonly dirtyService = inject(SettingsDirtyService);

  /** `RetentionMath.MaxDays` on the server: 100 years, how "keep forever" is spelled (#130). */
  readonly maxDays = 36_500;

  readonly loading = signal(false);
  readonly saving = signal(false);
  readonly running = signal(false);
  readonly runResult = signal<RetentionRunResult | null>(null);
  /** Why the last save or run was refused — the server's `error` sentence (a window outside 1…36 500 days, for one). */
  readonly actionError = signal<string | null>(null);

  /** Last persisted snapshot — used to detect unsaved edits. */
  private readonly saved = signal<RetentionDto | null>(null);

  readonly verboseDays = signal(3);
  readonly debugDays = signal(3);
  readonly informationDays = signal(90);
  readonly warningDays = signal(90);
  readonly errorDays = signal(90);
  readonly fatalDays = signal(90);
  readonly metricsDays = signal(30);
  readonly tracesDays = signal(14);

  readonly fields: readonly DayField[] = [
    { key: 'verboseDays',     label: 'Verbose',     dotClass: 'dot-v' },
    { key: 'debugDays',       label: 'Debug',       dotClass: 'dot-d' },
    { key: 'informationDays', label: 'Information', dotClass: 'dot-i' },
    { key: 'warningDays',     label: 'Warning',     dotClass: 'dot-w' },
    { key: 'errorDays',       label: 'Error',       dotClass: 'dot-e' },
    { key: 'fatalDays',       label: 'Fatal',       dotClass: 'dot-f' },
    { key: 'metricsDays',     label: 'Metrics',     icon: 'chart-bar' },
    { key: 'tracesDays',      label: 'Traces',      icon: 'git-branch' },
  ];

  /** Map field key → its signal for template binding. */
  readonly signals: Record<keyof RetentionDto, ReturnType<typeof signal<number>>> = {
    verboseDays: this.verboseDays,
    debugDays: this.debugDays,
    informationDays: this.informationDays,
    warningDays: this.warningDays,
    errorDays: this.errorDays,
    fatalDays: this.fatalDays,
    metricsDays: this.metricsDays,
    tracesDays: this.tracesDays,
  };

  readonly dirty = computed(() => {
    const s = this.saved();
    if (!s) return false;
    return (Object.keys(this.signals) as (keyof RetentionDto)[]).some(
      k => this.signals[k]() !== s[k],
    );
  });

  constructor() {
    effect(() => this.dirtyService.mark('retention', this.dirty()));
  }

  ngOnInit(): void {
    this.loading.set(true);
    this.api.getRetention().subscribe({
      next: r => {
        this.verboseDays.set(r.verboseDays);
        this.debugDays.set(r.debugDays);
        this.informationDays.set(r.informationDays);
        this.warningDays.set(r.warningDays);
        this.errorDays.set(r.errorDays);
        this.fatalDays.set(r.fatalDays);
        this.metricsDays.set(r.metricsDays);
        this.tracesDays.set(r.tracesDays);
        this.saved.set({ ...r });
        this.loading.set(false);
      },
      error: () => this.loading.set(false),
    });
  }

  save(): void {
    const dto: RetentionDto = {
      verboseDays: this.verboseDays(),
      debugDays: this.debugDays(),
      informationDays: this.informationDays(),
      warningDays: this.warningDays(),
      errorDays: this.errorDays(),
      fatalDays: this.fatalDays(),
      metricsDays: this.metricsDays(),
      tracesDays: this.tracesDays(),
    };
    this.saving.set(true);
    this.actionError.set(null);
    this.api.putRetention(dto).subscribe({
      next: () => { this.saved.set({ ...dto }); this.saving.set(false); },
      error: e => {
        // The server refuses a window outside 1…36 500 days with 400 { error, field } (#130); the
        // sentence is what the person who typed "forever" as 200 000 needs to see.
        this.actionError.set(e?.error?.error ?? 'Failed to save the retention policy.');
        this.saving.set(false);
      },
    });
  }

  run(): void {
    this.running.set(true);
    this.runResult.set(null);
    this.actionError.set(null);
    this.api.runRetention().subscribe({
      next: r => { this.runResult.set(r); this.running.set(false); },
      error: e => {
        this.actionError.set(e?.error?.error ?? 'Failed to run retention.');
        this.running.set(false);
      },
    });
  }
}
