import { Controller, Get, Module } from '@nestjs/common';
import { ExecutionOutboxWorker } from './infrastructure/service-bus/outbox-worker';
import { ExecutionCommandWorker } from './infrastructure/service-bus/command-worker';
import { CatalogController, RepairController, WorkActions } from './interfaces/reparo-controller';
import { AdminGuard } from './interfaces/admin-guard';

@Controller()
export class HealthController {
  @Get('health')
  health() {
    return { status: 'ok', service: process.env.OTEL_SERVICE_NAME ?? 'soat-execution-service' };
  }
}

@Module({ controllers: [HealthController, RepairController, CatalogController], providers: [ExecutionOutboxWorker, ExecutionCommandWorker, AdminGuard, WorkActions] })
export class AppModule {}
