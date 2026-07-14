import { ApplicationConfig, ErrorHandler, provideBrowserGlobalErrorListeners } from '@angular/core';
import { provideRouter, withPreloading } from '@angular/router';
import { provideHttpClient, withInterceptors } from '@angular/common/http';
import { IdlePreloadStrategy } from './core/idle-preload.strategy';
import {
  LUCIDE_ICONS, LucideIconProvider,
  // Curated icon set — only the icons the app actually renders. Registering the
  // full lucide `icons` barrel (~2.5 MB source) eagerly was the single biggest
  // contributor to the initial bundle. Add an icon here when you use a new one.
  Activity, AlertCircle, ArrowDown, ArrowLeft, ArrowRight, ArrowUp, Asterisk, Ban,
  Bell, BellOff, BellRing, BookOpen, Box, Boxes, Braces, Brackets,
  Calendar, CalendarClock, ChartBar, ChartLine, ChartPie, Check, CheckCheck, CheckCircle,
  ChevronDown, ChevronLeft, ChevronRight, ChevronsLeft, ChevronsRight, CircleAlert, Clock, Copy,
  Database, Equal, ExternalLink, FileQuestion, Flame, GitBranch, GitCompare, Globe,
  Hash, History, Inbox, Info, Key, LayoutDashboard, List, ListFilter,
  Loader, LoaderCircle, LogOut, Mail, Maximize2, Network, Pause, Pencil,
  Pin, PinOff, Play, Plus, RefreshCw, Regex, Save, ScrollText, Search, Send,
  Settings, Share2, Sigma, SlidersHorizontal, Sparkles, Terminal, TextWrap, ToggleLeft,
  ToggleRight, Trash2, Type, User, UserPlus, Users, X, ZoomOut,
} from 'lucide-angular';
import { authInterceptor } from './core/interceptors/auth.interceptor';

import { routes } from './app.routes';

const APP_ICONS = {
  Activity, AlertCircle, ArrowDown, ArrowLeft, ArrowRight, ArrowUp, Asterisk, Ban,
  Bell, BellOff, BellRing, BookOpen, Box, Boxes, Braces, Brackets,
  Calendar, CalendarClock, ChartBar, ChartLine, ChartPie, Check, CheckCheck, CheckCircle,
  ChevronDown, ChevronLeft, ChevronRight, ChevronsLeft, ChevronsRight, CircleAlert, Clock, Copy,
  Database, Equal, ExternalLink, FileQuestion, Flame, GitBranch, GitCompare, Globe,
  Hash, History, Inbox, Info, Key, LayoutDashboard, List, ListFilter,
  Loader, LoaderCircle, LogOut, Mail, Maximize2, Network, Pause, Pencil,
  Pin, PinOff, Play, Plus, RefreshCw, Regex, Save, ScrollText, Search, Send,
  Settings, Share2, Sigma, SlidersHorizontal, Sparkles, Terminal, TextWrap, ToggleLeft,
  ToggleRight, Trash2, Type, User, UserPlus, Users, X, ZoomOut,
};

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
    provideRouter(routes, withPreloading(IdlePreloadStrategy)),
    provideHttpClient(withInterceptors([authInterceptor])),
    { provide: LUCIDE_ICONS, multi: true, useValue: new LucideIconProvider(APP_ICONS) },
  ],
};
