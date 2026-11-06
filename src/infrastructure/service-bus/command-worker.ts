import { Injectable, Logger, OnApplicationShutdown, OnModuleInit } from '@nestjs/common';
import { CosmosClient } from '@azure/cosmos';
import { WorkloadIdentityCredential } from '@azure/identity';
import { ServiceBusClient, ServiceBusReceiver } from '@azure/service-bus';
import { resolve } from 'node:path';
import { CosmosReservationStore } from '../cosmos/reservas';
import { CosmosEnqueueStore } from '../cosmos/enfileirar';
import { CosmosWorkStore } from '../cosmos/reparo';
import { CosmosDiagnosisStore } from '../cosmos/diagnostico';
import { DiagnosisCommandConsumer, createDiagnosisValidator } from './diagnosis-consumer';
import { CancelCommandConsumer, createCancelValidator } from './cancel-consumer';
import { ReleaseCommandConsumer, createReleaseValidator } from './release-consumer';
import { ReserveCommandConsumer, createReserveValidator } from './reserve-consumer';
import { EnqueueCommandConsumer, createEnqueueValidator } from './enqueue-consumer';
import { MessageVerifier } from './message-signature';
import { withMessageTrace } from '../observability/message-trace';

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
    const osKey = process.env.OS_MESSAGE_PUBLIC_KEY;
    if (!osKey || !endpoint || !/^https:\/\/[a-z0-9-]+\.documents\.azure\.com(?::443)?\/?$/i.test(endpoint) || !database || !container ||
        !namespace || !/^[a-z0-9-]+\.servicebus\.windows\.net$/i.test(namespace) || !queue ||
        !process.env.AZURE_TENANT_ID || !process.env.AZURE_CLIENT_ID || !process.env.AZURE_FEDERATED_TOKEN_FILE) throw new Error('Configuração incompleta do worker de comandos');
    const verifier = new MessageVerifier(osKey, 'soat-os-service');
    const directory = resolve(process.cwd(), 'docs/contracts/messages');
    const reserveValidator = createReserveValidator(directory); const enqueueValidator = createEnqueueValidator(directory); const cancelValidator = createCancelValidator(directory); const releaseValidator = createReleaseValidator(directory); const diagnosisValidator = createDiagnosisValidator(directory);
    const identity = new WorkloadIdentityCredential();
    this.cosmos = new CosmosClient({ endpoint, aadCredentials: identity });
    this.bus = new ServiceBusClient(namespace, identity);
    this.receiver = this.bus.createReceiver(queue, { receiveMode: 'peekLock' });
    const receiver = this.receiver; const operations = this.cosmos.database(database).container(container);
    const reserve = new ReserveCommandConsumer(new CosmosReservationStore(operations), reserveValidator);
    const enqueue = new EnqueueCommandConsumer(new CosmosEnqueueStore(operations), enqueueValidator);
    const cancel = new CancelCommandConsumer(new CosmosWorkStore(operations), cancelValidator);
    const release = new ReleaseCommandConsumer(new CosmosWorkStore(operations), releaseValidator);
    const diagnosis = new DiagnosisCommandConsumer(new CosmosDiagnosisStore(operations), diagnosisValidator);
    this.subscription = receiver.subscribe({
      processMessage: async (message) => {
        const body: unknown = message.body;
        if (!verifier.verify(body, message.applicationProperties?.signature)) {
          await receiver.deadLetterMessage(message, { deadLetterReason: 'UnauthenticatedProducer', deadLetterErrorDescription: 'Assinatura inválida' }); return;
        }
        return withMessageTrace(body, async () => {
        const type = body && typeof body === 'object' && 'type' in body ? body.type : undefined;
        if (type === 'execution.reserve-parts.v1') return reserve.process(message, receiver);
        if (type === 'execution.enqueue-work.v1') return enqueue.process(message, receiver);
        if (type === 'execution.cancel-work.v1') return cancel.process(message, receiver);
        if (type === 'execution.release-parts.v1') return release.process(message, receiver);
        if (type === 'execution.request-diagnosis.v1') return diagnosis.process(message, receiver);
        // The broker may eventually send unsupported commands to the DLQ; do not claim success.
        this.logger.warn('Comando ainda não suportado; permanecem necessárias outras etapas de Execution');
        await receiver.abandonMessage(message);
        });
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
