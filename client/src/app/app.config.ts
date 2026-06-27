import { ApplicationConfig, ErrorHandler, provideBrowserGlobalErrorListeners } from '@angular/core';
import { provideRouter } from '@angular/router';
import { provideHttpClient, withInterceptors } from '@angular/common/http';
import { LUCIDE_ICONS, LucideIconProvider, icons } from 'lucide-angular';
import { authInterceptor } from './core/interceptors/auth.interceptor';

import { routes } from './app.routes';

/**
 * Ignores the benign "ResizeObserver loop completed with undelivered notifications"
 * browser quirk (fired by Chart.js responsive resizes) that Angular's global error
 * listener otherwise surfaces as an ERROR. All other errors pass through unchanged.
 */
class FilteringErrorHandler extends ErrorHandler {
  override handleError(error: unknown): void {
    // Angular wraps the raw ErrorEvent in an Error whose own message is just a prefix
    // ("An ErrorEvent with no error occurred…") and puts the real text on `.cause`.
    const e = error as any;
    const text = [e?.message, e?.cause?.message, e?.cause, String(error)]
      .map(v => (typeof v === 'string' ? v : ''))
      .join(' ');
    if (text.includes('ResizeObserver loop')) return;
    super.handleError(error);
  }
}

export const appConfig: ApplicationConfig = {
  providers: [
    provideBrowserGlobalErrorListeners(),
    { provide: ErrorHandler, useClass: FilteringErrorHandler },
    provideRouter(routes),
    provideHttpClient(withInterceptors([authInterceptor])),
    { provide: LUCIDE_ICONS, multi: true, useValue: new LucideIconProvider(icons) },
  ],
};
