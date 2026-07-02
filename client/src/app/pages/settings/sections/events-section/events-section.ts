import {
  ChangeDetectionStrategy,
  Component,
  computed,
  effect,
  inject,
  signal,
} from '@angular/core';
import { FormsModule } from '@angular/forms';
import { LucideAngularModule } from 'lucide-angular';
import { SectionComponent } from '../../../../shared/components/ui';
import { UserPreferencesService } from '../../../../core/services/user-preferences.service';
import { SettingsDirtyService } from '../../settings-dirty.service';

@Component({
  selector: 'app-events-section',
  imports: [LucideAngularModule, FormsModule, SectionComponent],
  templateUrl: './events-section.html',
  styleUrl: './events-section.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class EventsSectionComponent {
  private readonly prefs = inject(UserPreferencesService);
  private readonly dirtyService = inject(SettingsDirtyService);

  readonly overviewCustomPropsCsv = signal(this.prefs.overviewCustomPropsCsv());

  /** Dirty while the textarea differs from the persisted value. */
  readonly dirty = computed(() => this.overviewCustomPropsCsv() !== this.prefs.overviewCustomPropsCsv());

  constructor() {
    // Report local dirty state to the shared service so the container can
    // drive the beforeunload / canDeactivate warnings.
    effect(() => this.dirtyService.mark('events', this.dirty()));
  }

  saveOverviewPrefs(): void {
    this.prefs.setOverviewCustomPropsFromCsv(this.overviewCustomPropsCsv());
    this.overviewCustomPropsCsv.set(this.prefs.overviewCustomPropsCsv());
  }
}
