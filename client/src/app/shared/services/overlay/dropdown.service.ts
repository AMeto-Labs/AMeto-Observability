import { ElementRef, inject, Injectable, Injector } from '@angular/core';
import { ConnectedPosition, Overlay } from '@angular/cdk/overlay';
import { ComponentType } from '@angular/cdk/portal';

import { attachComponent } from './overlay-core';
import { OverlayPanelRef } from './overlay-ref';

export interface DropdownConfig<D = unknown> {
  /** The trigger element the dropdown is anchored to. */
  origin: ElementRef<HTMLElement> | HTMLElement;
  /** Data provided to the dropdown component. */
  data?: D;
  /** Override the default connected positions (below-start with above fallback). */
  positions?: ConnectedPosition[];
  offsetY?: number;
  offsetX?: number;
  width?: string | number;
  minWidth?: string | number;
  panelClass?: string | string[];
  /** Close when a pointer event fires outside the panel (not on the origin). Defaults to `true`. */
  closeOnOutside?: boolean;
  /** Close on the Escape key. Defaults to `true`. */
  closeOnEscape?: boolean;
}

/** Default: open below the trigger aligned to its left edge, flip above when clipped. */
const DEFAULT_POSITIONS: ConnectedPosition[] = [
  { originX: 'start', originY: 'bottom', overlayX: 'start', overlayY: 'top', offsetY: 4 },
  { originX: 'start', originY: 'top', overlayX: 'start', overlayY: 'bottom', offsetY: -4 },
  { originX: 'end', originY: 'bottom', overlayX: 'end', overlayY: 'top', offsetY: 4 },
  { originX: 'end', originY: 'top', overlayX: 'end', overlayY: 'bottom', offsetY: -4 },
];

/** Opens components as dropdown panels anchored to a trigger element. */
@Injectable({ providedIn: 'root' })
export class DropdownService {
  private readonly overlay = inject(Overlay);
  private readonly injector = inject(Injector);

  open<T, R = unknown, D = unknown>(
    component: ComponentType<T>,
    config: DropdownConfig<D>,
  ): OverlayPanelRef<R, D> {
    const origin = config.origin instanceof ElementRef ? config.origin.nativeElement : config.origin;

    const basePositions = config.positions ?? DEFAULT_POSITIONS;
    const positions = basePositions.map((p) => ({
      ...p,
      offsetX: config.offsetX ?? p.offsetX,
      offsetY: config.offsetY ?? p.offsetY,
    }));

    const positionStrategy = this.overlay
      .position()
      .flexibleConnectedTo(origin)
      .withPush(true)
      .withFlexibleDimensions(false)
      .withPositions(positions);

    const overlayRef = this.overlay.create({
      positionStrategy,
      scrollStrategy: this.overlay.scrollStrategies.reposition(),
      hasBackdrop: false,
      panelClass: config.panelClass ?? 'app-dropdown-panel',
      width: config.width,
      minWidth: config.minWidth,
    });

    const ref = attachComponent<T, R, D>(overlayRef, component, this.injector, config.data as D);

    if (config.closeOnOutside ?? true) {
      overlayRef.outsidePointerEvents().subscribe((ev) => {
        // Ignore clicks on the trigger itself — the caller toggles that path.
        if (!origin.contains(ev.target as Node)) ref.close();
      });
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
}
