import { inject, Injectable, TemplateRef, ViewContainerRef } from '@angular/core';
import { Overlay } from '@angular/cdk/overlay';

import { attachTemplate } from './overlay-core';
import { OverlayPanelRef } from './overlay-ref';

export interface ContextMenuConfig<D = unknown> {
  /** Template rendering the menu (kept in the host component with its actions). */
  template: TemplateRef<unknown>;
  /** Host's view container — the template is instantiated in its injection context. */
  viewContainerRef: ViewContainerRef;
  /** Viewport coordinates of the menu's top-left corner (already edge-clamped by caller). */
  x: number;
  y: number;
  data?: D;
  panelClass?: string | string[];
  /** Close on a pointer event outside the panel. Defaults to `true`. */
  closeOnOutside?: boolean;
  /** Close on the Escape key. Defaults to `true`. */
  closeOnEscape?: boolean;
}

/**
 * Opens context menus positioned at a viewport point. Only one menu is active
 * at a time — opening a new one dismisses the previous. The overlay escapes any
 * `transform`ed ancestor (e.g. the virtual-scroll container), which a
 * `position: fixed` menu cannot.
 */
@Injectable({ providedIn: 'root' })
export class ContextMenuService {
  private readonly overlay = inject(Overlay);
  // `any` params sidestep the contravariance of OverlayPanelRef's generic result
  // type — this field only ever needs `close()`/identity, not the typed result.
  private active?: OverlayPanelRef<any, any>;

  open<R = unknown, D = unknown>(config: ContextMenuConfig<D>): OverlayPanelRef<R, D> {
    this.active?.close();

    const overlayRef = this.overlay.create({
      positionStrategy: this.overlay
        .position()
        .global()
        .left(`${config.x}px`)
        .top(`${config.y}px`),
      scrollStrategy: this.overlay.scrollStrategies.reposition(),
      panelClass: config.panelClass ?? 'app-context-menu',
      hasBackdrop: false,
    });

    const ref = attachTemplate<R, D>(
      overlayRef,
      config.template,
      config.viewContainerRef,
      config.data as D,
    );
    this.active = ref;
    ref.closed.then(() => {
      if (this.active === ref) this.active = undefined;
    });

    if (config.closeOnOutside ?? true) {
      overlayRef.outsidePointerEvents().subscribe(() => ref.close());
    }
    if (config.closeOnEscape ?? true) {
      overlayRef.keydownEvents().subscribe((e) => {
        if (e.key === 'Escape') {
          e.preventDefault();
          ref.close();
        }
      });
    }
    return ref;
  }

  /** Closes the active menu, if any. */
  close(): void {
    this.active?.close();
  }
}
