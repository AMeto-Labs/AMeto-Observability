import { inject, Injectable, TemplateRef, ViewContainerRef } from '@angular/core';
import { ConnectedPosition, Overlay } from '@angular/cdk/overlay';

import { attachTemplate } from './overlay-core';
import { OverlayPanelRef } from './overlay-ref';

export interface ContextMenuConfig<D = unknown> {
  /** Template rendering the menu (kept in the host component with its actions). */
  template: TemplateRef<unknown>;
  /** Host's view container — the template is instantiated in its injection context. */
  viewContainerRef: ViewContainerRef;
  /**
   * The element the menu belongs to — its ⋮ button. The menu opens below it, above it when
   * there is no room below, and is pushed back on screen when neither fits. Takes precedence
   * over `x` / `y`.
   */
  origin?: Element;
  /** Viewport point for the menu's top-left corner when there is no origin element. */
  x?: number;
  y?: number;
  data?: D;
  panelClass?: string | string[];
  /** Close on a pointer event outside the panel. Defaults to `true`. */
  closeOnOutside?: boolean;
  /** Close on the Escape key. Defaults to `true`. */
  closeOnEscape?: boolean;
}

/** Below the origin, else above it; left edges aligned, else right edges. */
const AROUND_ORIGIN: ConnectedPosition[] = [
  { originX: 'start', originY: 'bottom', overlayX: 'start', overlayY: 'top', offsetY: 4 },
  { originX: 'start', originY: 'top', overlayX: 'start', overlayY: 'bottom', offsetY: -4 },
  { originX: 'end', originY: 'bottom', overlayX: 'end', overlayY: 'top', offsetY: 4 },
  { originX: 'end', originY: 'top', overlayX: 'end', overlayY: 'bottom', offsetY: -4 },
];

/** Down-right of the point, else up and / or left of it. */
const AROUND_POINT: ConnectedPosition[] = [
  { originX: 'start', originY: 'top', overlayX: 'start', overlayY: 'top' },
  { originX: 'start', originY: 'top', overlayX: 'start', overlayY: 'bottom' },
  { originX: 'start', originY: 'top', overlayX: 'end', overlayY: 'top' },
  { originX: 'start', originY: 'top', overlayX: 'end', overlayY: 'bottom' },
];

/** Kept clear of the window's edges. */
const VIEWPORT_MARGIN = 8;

/**
 * Opens context menus anchored to an element or a viewport point. Only one menu is active
 * at a time — opening a new one dismisses the previous. The overlay escapes any
 * `transform`ed ancestor (e.g. the virtual-scroll container), which a
 * `position: fixed` menu cannot.
 *
 * The position is decided on the menu's real size, measured by the CDK once it renders:
 * callers used to pass a point pre-clamped against a guessed height, and any menu taller than
 * the guess ran off the bottom of the window. A menu taller than the window itself scrolls
 * (`.ctx-menu` max-height).
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
        .flexibleConnectedTo(config.origin ?? { x: config.x ?? 0, y: config.y ?? 0 })
        .withPositions(config.origin ? AROUND_ORIGIN : AROUND_POINT)
        .withFlexibleDimensions(false)
        .withPush(true)
        .withViewportMargin(VIEWPORT_MARGIN),
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
