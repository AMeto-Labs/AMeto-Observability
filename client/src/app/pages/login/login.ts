import { Component, signal, inject, OnInit, ChangeDetectionStrategy, ChangeDetectorRef } from '@angular/core';
import { Router, ActivatedRoute } from '@angular/router';
import { FormsModule } from '@angular/forms';
import { LucideAngularModule } from 'lucide-angular';
import { AuthService } from '../../core/services/auth.service';
import { AuthProvidersDto } from '../../core/models/auth.model';

@Component({
  selector: 'app-login',
  standalone: true,
  imports: [FormsModule, LucideAngularModule],
  changeDetection: ChangeDetectionStrategy.OnPush,
  templateUrl: './login.html',
  styleUrl: './login.scss',
})
export class LoginComponent implements OnInit {
  private auth   = inject(AuthService);
  private router = inject(Router);
  private route  = inject(ActivatedRoute);
  private cdr    = inject(ChangeDetectorRef);

  username = '';
  password = '';
  loading  = signal(false);
  error    = signal<string | null>(null);
  providers = signal<AuthProvidersDto>({ local: true, google: false, microsoft: false });

  ngOnInit(): void {
    // Show error from OAuth redirect
    this.route.queryParamMap.subscribe(params => {
      const err = params.get('error');
      if (err === 'access_denied') {
        const email = params.get('email') ?? '';
        this.error.set(`Access denied. Ask an admin to add ${email} to the system.`);
      } else if (err === 'oauth_failed') {
        this.error.set('OAuth sign-in failed. Please try again.');
      } else if (err === 'no_email') {
        this.error.set('Could not retrieve email from OAuth provider.');
      }
    });

    // Load available providers
    this.auth.getProviders().subscribe({
      next: p => { this.providers.set(p); this.cdr.markForCheck(); },
      error: () => {},
    });
  }

  submit(): void {
    if (!this.username || !this.password) {
      this.error.set('Enter username and password.');
      return;
    }
    this.loading.set(true);
    this.error.set(null);
    this.auth.login(this.username, this.password).subscribe({
      next: () => this.router.navigate(['/']),
      error: () => { this.loading.set(false); this.error.set('Invalid credentials.'); },
    });
  }

  loginGoogle(): void     { this.auth.loginWithOAuth('google'); }
  loginMicrosoft(): void  { this.auth.loginWithOAuth('microsoft'); }
}
