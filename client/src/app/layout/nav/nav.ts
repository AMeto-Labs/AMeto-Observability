import { Component, inject } from '@angular/core';
import { RouterLink, RouterLinkActive } from '@angular/router';
import { LucideAngularModule } from 'lucide-angular';
import { ThemeService } from '../../core/services/theme.service';
import { AuthService } from '../../core/services/auth.service';

interface NavItem {
  path: string;
  icon: string;
  label: string;
}

@Component({
  selector: 'app-nav',
  imports: [RouterLink, RouterLinkActive, LucideAngularModule],
  templateUrl: './nav.html',
  styleUrl: './nav.scss',
})
export class NavComponent {
  theme = inject(ThemeService);
  auth  = inject(AuthService);

  readonly mainItems: NavItem[] = [
    { path: '/events',  icon: 'list',       label: 'Logs'    },
    { path: '/traces',  icon: 'activity',   label: 'Traces'  },
    { path: '/metrics', icon: 'chart-bar',  label: 'Metrics' },
    { path: '/signals', icon: 'bell',       label: 'Alerts'  },
  ];

  readonly settingsItem: NavItem = { path: '/settings', icon: 'settings', label: 'Settings' };

  readonly userLabel: string = (() => {
    const r = this.auth.role;
    return r.charAt(0).toUpperCase() + r.slice(1);
  })();

  readonly userInitials: string = this.userLabel.slice(0, 2).toUpperCase();
}
