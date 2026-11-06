import { Controller, Get, Module } from '@nestjs/common';
import { ExecutionOutboxWorker } from './infrastructure/service-bus/outbox-worker';
import { ExecutionCommandWorker } from './infrastructure/service-bus/command-worker';
import { CatalogController, RepairController, StockController, WorkActions } from './interfaces/reparo-controller';
import { AdminGuard } from './interfaces/admin-guard';
import { ObservabilityShutdown } from './infrastructure/observability/shutdown';

@Controller()
export class HealthController {
  @Get('health')
  health() {
    return { status: 'ok', service: process.env.OTEL_SERVICE_NAME ?? 'soat-execution-service' };
  }
}

@Module({ controllers: [HealthController, RepairController, CatalogController, StockController], providers: [ObservabilityShutdown, ExecutionOutboxWorker, ExecutionCommandWorker, AdminGuard, WorkActions] })
export class AppModule {}
