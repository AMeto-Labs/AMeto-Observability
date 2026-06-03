import {
  Component, Input, OnChanges, AfterViewInit, OnDestroy,
  ViewChild, ElementRef, ChangeDetectionStrategy,
} from '@angular/core';
import { Chart } from 'chart.js';

@Component({
  selector: 'app-sparkline',
  standalone: true,
  template: '<canvas #c></canvas>',
  styles: [`:host { display: block; width: 100%; height: 100%; }
canvas { display: block; width: 100% !important; height: 100% !important; }`],
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class SparklineComponent implements AfterViewInit, OnChanges, OnDestroy {
  @ViewChild('c') private canvasRef!: ElementRef<HTMLCanvasElement>;
  @Input() values: number[] = [];
  @Input() color = '#22C55E';

  private chart?: Chart;
  private ready = false;

  ngAfterViewInit(): void {
    this.ready = true;
    this.render();
  }

  ngOnChanges(): void {
    if (this.ready) this.render();
  }

  ngOnDestroy(): void {
    this.chart?.destroy();
  }

  private render(): void {
    if (!this.canvasRef?.nativeElement || this.values.length < 2) return;
    this.chart?.destroy();
    this.chart = new Chart(this.canvasRef.nativeElement, {
      type: 'line',
      data: {
        datasets: [{
          data: this.values.map((y, x) => ({ x, y })),
          borderColor: this.color,
          backgroundColor: this.color + '28',
          fill: true,
          tension: 0.3,
          pointRadius: 0,
          borderWidth: 1.5,
        }],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        animation: false,
        scales: { x: { display: false, type: 'linear' }, y: { display: false } },
        plugins: { legend: { display: false }, tooltip: { enabled: false } },
      },
    });
  }
}
