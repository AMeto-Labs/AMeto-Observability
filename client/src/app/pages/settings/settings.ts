import {
  ChangeDetectionStrategy,
  Component,
  DestroyRef,
  HostListener,
  OnInit,
  computed,
  inject,
  signal,
} from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { LucideAngularModule } from 'lucide-angular';
import { PageHeaderComponent } from '../../shared/components/ui';
import { AuthService } from '../../core/services/auth.service';
import { SettingsDirtyService } from './settings-dirty.service';
import { SETTINGS_TABS, SettingsTabId } from './settings-tab';
import { DashboardsSectionComponent } from './sections/dashboards-section/dashboards-section';
import { EventsSectionComponent } from './sections/events-section/events-section';
import { RetentionSectionComponent } from './sections/retention-section/retention-section';
import { UsersSectionComponent } from './sections/users-section/users-section';
import { ApiKeysSectionComponent } from './sections/api-keys-section/api-keys-section';
import { UpdatesSectionComponent } from './sections/updates-section/updates-section';

@Component({
  selector: 'app-settings',
  imports: [
    LucideAngularModule,
    PageHeaderComponent,
    DashboardsSectionComponent,
    EventsSectionComponent,
    RetentionSectionComponent,
    UsersSectionComponent,
    ApiKeysSectionComponent,
    UpdatesSectionComponent,
  ],
  providers: [SettingsDirtyService],
  templateUrl: './settings.html',
  styleUrl: './settings.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class SettingsComponent implements OnInit {
  private readonly route = inject(ActivatedRoute);
  private readonly router = inject(Router);
  private readonly destroyRef = inject(DestroyRef);
  readonly authService = inject(AuthService);
  private readonly dirtyService = inject(SettingsDirtyService);

  /** Tabs visible to the current user (admin-only ones filtered out). */
  readonly availableTabs = computed(() =>
    SETTINGS_TABS.filter(t => !t.adminOnly || this.authService.isAdmin()),
  );

  readonly activeTab = signal<SettingsTabId>(this.availableTabs()[0]?.id ?? 'dashboards');

  /** Aggregated dirty flag, exposed for the canDeactivate route guard. */
  readonly isDirty = this.dirtyService.dirty;
  readonly dirtyTabsList = this.dirtyService.dirtyTabsList;

  ngOnInit(): void {
    // Restore the active tab from the URL (?tab=...) so an F5 / refresh keeps
    // the user on the same tab. Also reacts to back/forward navigation.
    this.route.queryParamMap
      .pipe(takeUntilDestroyed(this.destroyRef))
      .subscribe(params => {
        const requested = params.get('tab') as SettingsTabId | null;
        if (requested && this.isTabAvailable(requested) && requested !== this.activeTab()) {
          this.activeTab.set(requested);
        }
      });
  }

  selectTab(id: SettingsTabId): void {
    if (this.activeTab() === id) return;
    this.activeTab.set(id);
    // Persist the active tab into the URL query string (no reload).
    void this.router.navigate([], {
      relativeTo: this.route,
      queryParams: { tab: id },
      queryParamsHandling: 'merge',
    });
  }

  private isTabAvailable(id: SettingsTabId): boolean {
    return this.availableTabs().some(t => t.id === id);
  }

  /**
   * F5 / refresh / tab-close protection: when any section has unsaved
   * changes the browser shows its native "leave site?" confirmation.
   */
  @HostListener('window:beforeunload', ['$event'])
  onBeforeUnload(event: BeforeUnloadEvent): void {
    if (this.dirtyService.dirty()) {
      event.preventDefault();
      event.returnValue = '';
    }
  }
}
