import { Injector, TemplateRef, ViewContainerRef } from '@angular/core';
import type { OverlayRef } from '@angular/cdk/overlay';
import { ComponentPortal, ComponentType, TemplatePortal } from '@angular/cdk/portal';

import { OVERLAY_DATA, OverlayPanelRef } from './overlay-ref';

/**
 * Attaches a standalone component to `overlayRef` and returns its
 * {@link OverlayPanelRef}. The component receives a child injector exposing
 * both the ref (inject `OverlayPanelRef`) and its data (inject `OVERLAY_DATA`).
 */
export function attachComponent<T, R, D>(
  overlayRef: OverlayRef,
  component: ComponentType<T>,
  parentInjector: Injector,
  data: D,
): OverlayPanelRef<R, D> {
  const panelRef = new OverlayPanelRef<R, D>(overlayRef, data);
  const injector = Injector.create({
    parent: parentInjector,
    providers: [
      { provide: OverlayPanelRef, useValue: panelRef },
      { provide: OVERLAY_DATA, useValue: data },
    ],
  });
  overlayRef.attach(new ComponentPortal(component, null, injector));
  return panelRef;
}

/**
 * Attaches an embedded template to `overlayRef`. Used when the menu markup must
 * stay inside its host component (e.g. the event-row context menu, whose action
 * handlers live on that component). The template context exposes `$implicit`
 * (data) and `ref`.
 */
export function attachTemplate<R, D>(
  overlayRef: OverlayRef,
  template: TemplateRef<unknown>,
  viewContainerRef: ViewContainerRef,
  data: D,
): OverlayPanelRef<R, D> {
  const panelRef = new OverlayPanelRef<R, D>(overlayRef, data);
  overlayRef.attach(
    new TemplatePortal(template, viewContainerRef, { $implicit: data, ref: panelRef }),
  );
  return panelRef;
}
