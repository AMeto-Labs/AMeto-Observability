import { Component, computed, inject } from '@angular/core';
import { RouterLink, RouterLinkActive } from '@angular/router';
import { LucideAngularModule } from 'lucide-angular';
import { AuthService } from '../../core/services/auth.service';
import { ViewPermission } from '../../core/models/auth.model';

interface NavItem {
  path: string;
  icon: string;
  label: string;
  /** View scope required to see this item; omitted items are always shown. */
  perm?: ViewPermission;
}

@Component({
  selector: 'app-nav',
  imports: [RouterLink, RouterLinkActive, LucideAngularModule],
  templateUrl: './nav.html',
  styleUrl: './nav.scss',
})
export class NavComponent {
  auth = inject(AuthService);

  private readonly mainItems: NavItem[] = [
    { path: '/events',  icon: 'list',       label: 'Logs',    perm: ViewPermission.Logs },
    { path: '/stats',   icon: 'chart-pie',  label: 'Stats',   perm: ViewPermission.Stats },
    { path: '/traces',  icon: 'activity',   label: 'Traces',  perm: ViewPermission.Traces },
    { path: '/metrics', icon: 'chart-bar',  label: 'Metrics', perm: ViewPermission.Metrics },
    { path: '/signals', icon: 'bell',       label: 'Alerts'  },
  ];

  /** Nav items the current user is allowed to see (scope-gated; admin sees all). */
  readonly visibleItems = computed(() =>
    this.mainItems.filter(i => i.perm === undefined || this.auth.can(i.perm)));

  readonly referenceItem: NavItem = { path: '/reference', icon: 'book-open', label: 'Reference' };
  readonly settingsItem:  NavItem = { path: '/settings',  icon: 'settings',  label: 'Settings' };
}
