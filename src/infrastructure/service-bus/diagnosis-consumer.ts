import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import Ajv from 'ajv';
import addFormats from 'ajv-formats';
import { ServiceBusReceivedMessage, ServiceBusReceiver } from '@azure/service-bus';
import { RequestDiagnosis } from '../cosmos/diagnostico';

type Receiver = Pick<ServiceBusReceiver, 'completeMessage' | 'abandonMessage' | 'deadLetterMessage'>;
type Envelope = Omit<RequestDiagnosis, 'veiculoId' | 'solicitacoes'> & { data: Pick<RequestDiagnosis, 'veiculoId' | 'solicitacoes'> };
export function createDiagnosisValidator(directory: string): (value: unknown) => boolean {
  const ajv = new Ajv({ allErrors: true }); addFormats(ajv);
  const read = (filename: string): object => JSON.parse(readFileSync(resolve(directory, filename), 'utf8')) as object;
  ajv.addSchema(read('envelope.schema.json'));
  const validate = ajv.compile(read('execution.request-diagnosis.v1.schema.json'));
  return (value) => validate(value) === true;
}
export class DiagnosisCommandConsumer {
  constructor(private readonly store: { request(command: RequestDiagnosis): Promise<void> }, private readonly validate: (value: unknown) => boolean) {}
  async process(message: ServiceBusReceivedMessage, receiver: Receiver): Promise<void> {
    const body: unknown = message.body;
    if (!this.validate(body) || typeof body !== 'object' || body === null || !('messageId' in body) || body.messageId !== message.messageId || (message.subject && message.subject !== 'execution.request-diagnosis.v1')) {
      await receiver.deadLetterMessage(message, { deadLetterReason: 'InvalidContract', deadLetterErrorDescription: 'Mensagem inválida ou versão não suportada' }); return;
    }
    const event = body as Envelope;
    try {
      await this.store.request({ messageId: event.messageId, operationId: event.operationId, osId: event.osId, sagaId: event.sagaId, filialId: event.filialId, correlationId: event.correlationId, veiculoId: event.data.veiculoId, ...(event.traceparent ? { traceparent: event.traceparent } : {}), ...(event.tracestate ? { tracestate: event.tracestate } : {}), ...(event.data.solicitacoes ? { solicitacoes: event.data.solicitacoes } : {}) });
    } catch { await receiver.abandonMessage(message); return; }
    await receiver.completeMessage(message);
  }
}
