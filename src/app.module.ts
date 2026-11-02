import { Controller, Get, Module } from '@nestjs/common';
import { ExecutionOutboxWorker } from './infrastructure/service-bus/outbox-worker';
import { ExecutionCommandWorker } from './infrastructure/service-bus/command-worker';

@Controller()
export class HealthController {
  @Get('health')
  health() {
    return { status: 'ok', service: process.env.OTEL_SERVICE_NAME ?? 'soat-execution-service' };
  }
}

@Module({ controllers: [HealthController], providers: [ExecutionOutboxWorker, ExecutionCommandWorker] })
export class AppModule {}
