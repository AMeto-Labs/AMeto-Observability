import { Injectable, signal } from '@angular/core';

@Injectable({ providedIn: 'root' })
export class ThemeService {
  readonly theme = signal<'dark' | 'light'>('dark');

  constructor() {
    const saved = localStorage.getItem('ameto-theme') as 'dark' | 'light' | null;
    const t = saved === 'light' ? 'light' : 'dark';
    this.theme.set(t);
    document.documentElement.setAttribute('data-theme', t);
  }

  toggle(): void {
    const next = this.theme() === 'dark' ? 'light' : 'dark';
    this.theme.set(next);
    document.documentElement.setAttribute('data-theme', next);
    localStorage.setItem('ameto-theme', next);
  }
}
