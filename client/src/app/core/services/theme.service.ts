import { Injectable } from '@angular/core';

@Injectable({ providedIn: 'root' })
export class ThemeService {
  constructor() {
    localStorage.removeItem('ameto-theme');
    document.documentElement.removeAttribute('data-theme');
  }
}
