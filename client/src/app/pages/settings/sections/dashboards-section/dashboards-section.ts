import { ChangeDetectionStrategy, Component } from '@angular/core';
import { RouterLink } from '@angular/router';
import { LucideAngularModule } from 'lucide-angular';
import { SectionComponent } from '../../../../shared/components/ui';

@Component({
  selector: 'app-dashboards-section',
  imports: [LucideAngularModule, RouterLink, SectionComponent],
  templateUrl: './dashboards-section.html',
  styleUrl: './dashboards-section.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class DashboardsSectionComponent {}
