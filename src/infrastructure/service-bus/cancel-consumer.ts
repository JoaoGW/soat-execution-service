import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import Ajv from 'ajv';
import addFormats from 'ajv-formats';
import { ServiceBusReceivedMessage, ServiceBusReceiver } from '@azure/service-bus';
import { WorkAction } from '../cosmos/reparo';

type Receiver = Pick<ServiceBusReceiver, 'completeMessage' | 'abandonMessage' | 'deadLetterMessage'>;
type Store = { cancel(command: WorkAction): Promise<unknown> };
type Envelope = WorkAction & { data: { executionId?: string; reason: string } };
export function createCancelValidator(directory: string): (value: unknown) => boolean {
  const ajv = new Ajv({ allErrors: true }); addFormats(ajv);
  const read = (file: string): object => JSON.parse(readFileSync(resolve(directory, file), 'utf8')) as object;
  ajv.addSchema(read('envelope.schema.json')); const validate = ajv.compile(read('execution.cancel-work.v1.schema.json'));
  return (value) => validate(value) === true;
}
export class CancelCommandConsumer {
  constructor(private readonly store: Store, private readonly validate: (value: unknown) => boolean) {}
  async process(message: ServiceBusReceivedMessage, receiver: Receiver): Promise<void> {
    const body: unknown = message.body;
    if (!this.validate(body) || !body || typeof body !== 'object' || !('messageId' in body) || body.messageId !== message.messageId || (message.subject && message.subject !== 'execution.cancel-work.v1')) {
      await receiver.deadLetterMessage(message, { deadLetterReason: 'InvalidContract', deadLetterErrorDescription: 'Mensagem inválida ou versão não suportada' }); return;
    }
    const event = body as Envelope;
    try { await this.store.cancel({ messageId: event.messageId, operationId: event.operationId, osId: event.osId, sagaId: event.sagaId, filialId: event.filialId, correlationId: event.correlationId, reason: event.data.reason, ...(event.data.executionId ? { executionId: event.data.executionId } : {}), ...(event.traceparent ? { traceparent: event.traceparent } : {}), ...(event.tracestate ? { tracestate: event.tracestate } : {}) }); }
    catch { await receiver.abandonMessage(message); return; }
    await receiver.completeMessage(message);
  }
}
