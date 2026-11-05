import { Component, computed, inject, signal } from '@angular/core';
import { toSignal } from '@angular/core/rxjs-interop';
import { NavigationEnd, Router, RouterLink, RouterLinkActive } from '@angular/router';
import { filter, map } from 'rxjs';
import { LucideAngularModule } from 'lucide-angular';
import { AuthService } from '../../core/services/auth.service';
import { ViewPermission } from '../../core/models/auth.model';

interface NavItem {
  path: string;
  icon: string;
  label: string;
  /** View scope required to see this item; omitted items are always shown. */
  perm?: ViewPermission;
  /** Has a place in the phone's five-entry bottom bar; the others are listed under More. */
  bar?: boolean;
}

/** Whether `url` is `path` or a page under it (`/settings/users/1`, `/events?from=…`). */
function isUnder(url: string, path: string): boolean {
  return url === path || url.startsWith(path + '/') || url.startsWith(path + '?');
}

@Component({
  selector: 'app-nav',
  imports: [RouterLink, RouterLinkActive, LucideAngularModule],
  templateUrl: './nav.html',
  styleUrl: './nav.scss',
  host: { '(document:keydown.escape)': 'closeMore()' },
})
export class NavComponent {
  auth = inject(AuthService);
  private readonly router = inject(Router);

  private readonly mainItems: NavItem[] = [
    { path: '/events',  icon: 'list',       label: 'Logs',    perm: ViewPermission.Logs,    bar: true },
    { path: '/stats',   icon: 'chart-pie',  label: 'Stats',   perm: ViewPermission.Stats },
    { path: '/traces',  icon: 'activity',   label: 'Traces',  perm: ViewPermission.Traces,  bar: true },
    { path: '/metrics', icon: 'chart-bar',  label: 'Metrics', perm: ViewPermission.Metrics, bar: true },
    { path: '/signals', icon: 'bell',       label: 'Alerts',  bar: true },
  ];

  /** Nav items the current user is allowed to see (scope-gated; admin sees all). */
  readonly visibleItems = computed(() =>
    this.mainItems.filter(i => i.perm === undefined || this.auth.can(i.perm)));

  readonly referenceItem: NavItem = { path: '/reference', icon: 'book-open', label: 'Reference' };
  readonly settingsItem:  NavItem = { path: '/settings',  icon: 'settings',  label: 'Settings' };
  /** Phone only: the rail has never listed Nodes; the bar's More sheet does (design, phone frames). */
  private readonly nodesItem: NavItem = { path: '/nodes', icon: 'network', label: 'Nodes' };

  /** The More sheet (phone): what the bar has no room for — Stats, then the rail's bottom group. */
  readonly moreItems = computed<NavItem[]>(() => [
    ...this.visibleItems().filter(i => !i.bar),
    this.nodesItem,
    this.settingsItem,
    this.referenceItem,
  ]);

  readonly moreOpen = signal(false);

  /** The current URL, so the More entry lights up for a page it holds as a rail item does for its own. */
  private readonly url = toSignal(
    this.router.events.pipe(
      filter((e): e is NavigationEnd => e instanceof NavigationEnd),
      map(e => e.urlAfterRedirects),
    ),
    { initialValue: this.router.url },
  );

  readonly moreActive = computed(() => {
    const url = this.url();
    return this.moreItems().some(i => isUnder(url, i.path));
  });

  toggleMore(): void {
    this.moreOpen.update(open => !open);
  }

  closeMore(): void {
    this.moreOpen.set(false);
  }
}
