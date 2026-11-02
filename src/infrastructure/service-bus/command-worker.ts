import { Injectable, Logger, OnApplicationShutdown, OnModuleInit } from '@nestjs/common';
import { CosmosClient } from '@azure/cosmos';
import { WorkloadIdentityCredential } from '@azure/identity';
import { ServiceBusClient, ServiceBusReceiver } from '@azure/service-bus';
import { resolve } from 'node:path';
import { CosmosReservationStore } from '../cosmos/reservas';
import { CosmosEnqueueStore } from '../cosmos/enfileirar';
import { ReserveCommandConsumer, createReserveValidator } from './reserve-consumer';
import { EnqueueCommandConsumer, createEnqueueValidator } from './enqueue-consumer';

/** Disabled by default; unsupported commands remain unacknowledged. */
@Injectable()
export class ExecutionCommandWorker implements OnModuleInit, OnApplicationShutdown {
  private readonly logger = new Logger(ExecutionCommandWorker.name);
  private cosmos?: CosmosClient;
  private bus?: ServiceBusClient;
  private receiver?: ServiceBusReceiver;
  private subscription?: { close(): Promise<void> };

  onModuleInit(): void {
    if (process.env.ENABLE_EXECUTION_COMMANDS !== 'true') return;
    const endpoint = process.env.COSMOS_ENDPOINT; const database = process.env.COSMOS_DATABASE; const container = process.env.COSMOS_CONTAINER;
    const namespace = process.env.SERVICE_BUS_NAMESPACE; const queue = process.env.SERVICE_BUS_COMMAND_QUEUE;
    if (!endpoint || !/^https:\/\/[a-z0-9-]+\.documents\.azure\.com(?::443)?\/?$/i.test(endpoint) || !database || !container ||
        !namespace || !/^[a-z0-9-]+\.servicebus\.windows\.net$/i.test(namespace) || !queue ||
        !process.env.AZURE_TENANT_ID || !process.env.AZURE_CLIENT_ID || !process.env.AZURE_FEDERATED_TOKEN_FILE) throw new Error('Configuração incompleta do worker de comandos');
    const directory = resolve(process.cwd(), 'docs/contracts/messages');
    const reserveValidator = createReserveValidator(directory); const enqueueValidator = createEnqueueValidator(directory);
    const identity = new WorkloadIdentityCredential();
    this.cosmos = new CosmosClient({ endpoint, aadCredentials: identity });
    this.bus = new ServiceBusClient(namespace, identity);
    this.receiver = this.bus.createReceiver(queue, { receiveMode: 'peekLock' });
    const receiver = this.receiver; const operations = this.cosmos.database(database).container(container);
    const reserve = new ReserveCommandConsumer(new CosmosReservationStore(operations), reserveValidator);
    const enqueue = new EnqueueCommandConsumer(new CosmosEnqueueStore(operations), enqueueValidator);
    this.subscription = receiver.subscribe({
      processMessage: async (message) => {
        const body: unknown = message.body;
        const type = body && typeof body === 'object' && 'type' in body ? body.type : undefined;
        if (type === 'execution.reserve-parts.v1') return reserve.process(message, receiver);
        if (type === 'execution.enqueue-work.v1') return enqueue.process(message, receiver);
        // The broker may eventually send unsupported commands to the DLQ; do not claim success.
        this.logger.warn('Comando ainda não suportado; permanecem necessárias outras etapas de Execution');
        await receiver.abandonMessage(message);
      },
      processError: async (args) => { this.logger.error(`Falha no transporte Service Bus: ${args.errorSource}`); },
    }, { autoCompleteMessages: false, maxConcurrentCalls: 1 });
  }

  async onApplicationShutdown(): Promise<void> {
    try { await this.subscription?.close(); } catch { this.logger.error('Falha ao encerrar assinatura'); }
    try { await this.receiver?.close(); } catch { this.logger.error('Falha ao encerrar receptor'); }
    try { await this.bus?.close(); } catch { this.logger.error('Falha ao encerrar Service Bus'); }
    this.cosmos?.dispose();
  }
}
