import { Component, OnInit, inject, ChangeDetectionStrategy } from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';
import { AuthService } from '../../core/services/auth.service';
import { LucideAngularModule } from 'lucide-angular';

@Component({
  selector: 'app-oauth-callback',
  standalone: true,
  imports: [LucideAngularModule],
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: `
    <div style="display:flex;height:100dvh;align-items:center;justify-content:center;background:var(--bg-main);">
      <lucide-icon name="loader-circle" [size]="24" style="color:var(--txt-dim);animation:spin 1s linear infinite" />
    </div>
    <style>@keyframes spin{to{transform:rotate(360deg)}}</style>
  `,
})
export class OauthCallbackComponent implements OnInit {
  private route  = inject(ActivatedRoute);
  private auth   = inject(AuthService);
  private router = inject(Router);

  ngOnInit(): void {
    const params = this.route.snapshot.queryParamMap;
    const token     = params.get('token');
    const expiresIn = Number(params.get('expiresIn') ?? 0);
    const role      = params.get('role') ?? 'viewer';

    if (token && expiresIn > 0) {
      this.auth.handleOAuthCallback(token, expiresIn, role);
    } else {
      this.router.navigate(['/login'], { queryParams: { error: 'oauth_failed' } });
    }
  }
}
