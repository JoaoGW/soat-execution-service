import { Injectable, Logger, OnApplicationShutdown, OnModuleInit } from '@nestjs/common';
import { CosmosClient } from '@azure/cosmos';
import { cosmosConfigured, createWorkerClients, serviceBusConfigured } from '../azure-clients';
import { ServiceBusClient, ServiceBusSender } from '@azure/service-bus';
import { CosmosOutboxDispatcher } from '../cosmos/outbox';
import { ServiceBusEventPublisher } from './event-publisher';
import { MessageSigner } from './message-signature';

@Injectable()
export class ExecutionOutboxWorker implements OnModuleInit, OnApplicationShutdown {
  private readonly logger = new Logger(ExecutionOutboxWorker.name);
  private cosmos?: CosmosClient;
  private bus?: ServiceBusClient;
  private sender?: ServiceBusSender;
  private dispatcher?: CosmosOutboxDispatcher;
  private timer?: NodeJS.Timeout;
  private active?: Promise<void>;
  private stopping = false;

  onModuleInit(): void {
    if (process.env.ENABLE_EXECUTION_OUTBOX !== 'true') return;
    const endpoint = process.env.COSMOS_ENDPOINT;
    const database = process.env.COSMOS_DATABASE;
    const container = process.env.COSMOS_CONTAINER;
    const namespace = process.env.SERVICE_BUS_NAMESPACE;
    const topic = process.env.SERVICE_BUS_EVENT_TOPIC;
    const signingKey = process.env.EXECUTION_MESSAGE_PRIVATE_KEY;
    if (!signingKey || !cosmosConfigured(endpoint) || !database || !container || !serviceBusConfigured(namespace) || !topic) {
      throw new Error('Configuração incompleta do worker da outbox');
    }
    const signer = new MessageSigner(signingKey);
    ({ cosmos: this.cosmos, bus: this.bus } = createWorkerClients(endpoint, namespace));
    this.sender = this.bus.createSender(topic);
    this.dispatcher = new CosmosOutboxDispatcher(this.cosmos.database(database).container(container), new ServiceBusEventPublisher(this.sender, signer));
    this.active = this.poll();
  }

  private async poll(): Promise<void> {
    if (this.stopping || !this.dispatcher) return;
    try { await this.dispatcher.dispatchOne(); }
    catch { this.logger.error('Falha ao processar outbox'); }
    if (!this.stopping) this.timer = setTimeout(() => { this.active = this.poll(); }, 1000);
  }

  async onApplicationShutdown(): Promise<void> {
    this.stopping = true;
    if (this.timer) clearTimeout(this.timer);
    try { await this.active; } catch { this.logger.error('Falha ao encerrar processamento'); }
    try { await this.sender?.close(); } catch { this.logger.error('Falha ao encerrar sender'); }
    try { await this.bus?.close(); } catch { this.logger.error('Falha ao encerrar Service Bus'); }
    this.cosmos?.dispose();
  }
}
