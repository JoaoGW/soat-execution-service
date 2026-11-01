import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import Ajv from 'ajv';
import addFormats from 'ajv-formats';
import { ServiceBusReceivedMessage, ServiceBusReceiver } from '@azure/service-bus';
import { EnqueueWorkCommand } from '../cosmos/enfileirar';

type Receiver = Pick<ServiceBusReceiver, 'completeMessage' | 'abandonMessage' | 'deadLetterMessage'>;
type Store = { enqueue(command: EnqueueWorkCommand): Promise<unknown> };
type Envelope = Pick<EnqueueWorkCommand, 'messageId' | 'operationId' | 'correlationId' | 'osId' | 'sagaId' | 'filialId' | 'traceparent' | 'tracestate'> & { data: Omit<EnqueueWorkCommand, 'messageId' | 'operationId' | 'correlationId' | 'osId' | 'sagaId' | 'filialId' | 'traceparent' | 'tracestate'> };

export function createEnqueueValidator(directory: string): (value: unknown) => boolean {
  const ajv = new Ajv({ allErrors: true }); addFormats(ajv);
  const read = (name: string): object => JSON.parse(readFileSync(resolve(directory, name), 'utf8')) as object;
  ajv.addSchema(read('envelope.schema.json'));
  const validate = ajv.compile(read('execution.enqueue-work.v1.schema.json'));
  return (value) => validate(value) === true;
}

export class EnqueueCommandConsumer {
  constructor(private readonly store: Store, private readonly validate: (value: unknown) => boolean) {}
  async process(message: ServiceBusReceivedMessage, receiver: Receiver): Promise<void> {
    const body: unknown = message.body;
    if (!this.validate(body) || !body || typeof body !== 'object' || !('messageId' in body) || body.messageId !== message.messageId || (message.subject && message.subject !== 'execution.enqueue-work.v1')) {
      await receiver.deadLetterMessage(message, { deadLetterReason: 'InvalidContract', deadLetterErrorDescription: 'Mensagem inválida ou versão não suportada' }); return;
    }
    const event = body as Envelope; const d = event.data;
    try {
      await this.store.enqueue({ messageId: event.messageId, operationId: event.operationId, osId: event.osId, sagaId: event.sagaId, filialId: event.filialId, correlationId: event.correlationId, quoteId: d.quoteId, quoteVersion: d.quoteVersion, reservationId: d.reservationId, executionId: d.executionId, chargeId: d.chargeId, paymentId: d.paymentId, providerOrderId: d.providerOrderId, providerPaymentId: d.providerPaymentId, ...(event.traceparent ? { traceparent: event.traceparent } : {}), ...(event.tracestate ? { tracestate: event.tracestate } : {}) });
    } catch { await receiver.abandonMessage(message); return; }
    await receiver.completeMessage(message);
  }
}
