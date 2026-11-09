import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import Ajv from 'ajv';
import addFormats from 'ajv-formats';
import { ServiceBusReceivedMessage, ServiceBusReceiver } from '@azure/service-bus';
import { ReservePartsCommand } from '../cosmos/reservas';

type Receiver = Pick<ServiceBusReceiver, 'completeMessage' | 'abandonMessage' | 'deadLetterMessage'>;
type ReservationStore = { reserve(command: ReservePartsCommand): Promise<unknown> };
type ReserveEnvelope = Omit<ReservePartsCommand, 'quoteId' | 'quoteVersion' | 'reservationId' | 'deadline' | 'items'> & {
  data: Pick<ReservePartsCommand, 'quoteId' | 'quoteVersion' | 'reservationId' | 'deadline' | 'items'>;
};

export function createReserveValidator(directory: string): (value: unknown) => boolean {
  const ajv = new Ajv({ allErrors: true });
  addFormats(ajv);
  const read = (filename: string): object => JSON.parse(readFileSync(resolve(directory, filename), 'utf8')) as object;
  ajv.addSchema(read('envelope.schema.json'));
  const validate = ajv.compile(read('execution.reserve-parts.v1.schema.json'));
  return (value) => validate(value) === true;
}

/** The caller configures manual settlement and owns the receiver lifecycle. */
export class ReserveCommandConsumer {
  constructor(private readonly store: ReservationStore, private readonly validate: (value: unknown) => boolean) {}

  async process(message: ServiceBusReceivedMessage, receiver: Receiver): Promise<void> {
    const body: unknown = message.body;
    if (!this.validate(body) || typeof body !== 'object' || body === null || !('messageId' in body) ||
        body.messageId !== message.messageId || (message.subject && message.subject !== 'execution.reserve-parts.v1')) {
      await receiver.deadLetterMessage(message, { deadLetterReason: 'InvalidContract', deadLetterErrorDescription: 'Mensagem inválida ou versão não suportada' });
      return;
    }
    const event = body as ReserveEnvelope;
    const command: ReservePartsCommand = {
      messageId: event.messageId, operationId: event.operationId, osId: event.osId, sagaId: event.sagaId,
      filialId: event.filialId, correlationId: event.correlationId, quoteId: event.data.quoteId,
      quoteVersion: event.data.quoteVersion, reservationId: event.data.reservationId,
      deadline: event.data.deadline, items: event.data.items,
      ...(event.traceparent ? { traceparent: event.traceparent } : {}),
      ...(event.tracestate ? { tracestate: event.tracestate } : {}),
    };
    try {
      await this.store.reserve(command);
    } catch {
      // No failure is acknowledged until a durable business outcome exists.
      await receiver.abandonMessage(message);
      return;
    }
    // If settlement fails, lock expiry causes a safe idempotent redelivery.
    await receiver.completeMessage(message);
  }
}
