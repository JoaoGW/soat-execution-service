import { createHash, randomUUID } from 'node:crypto';
import { Container, ItemDefinition, OperationInput } from '@azure/cosmos';

export interface EnqueueWorkCommand {
  messageId: string; operationId: string; osId: string; sagaId: string; filialId: string; correlationId: string;
  quoteId: string; quoteVersion: number; reservationId: string; executionId: string; chargeId: string; paymentId: string;
  providerOrderId: string; providerPaymentId: string; traceparent?: string; tracestate?: string;
}
type Stored<T> = { value: T; etag: string };
type Gate = ItemDefinition & { id: string; filialId: string; cancelled: boolean };
type Reservation = ItemDefinition & { id: string; filialId: string; sagaId: string; osId: string; reservationId: string; quoteId: string; quoteVersion: number; status: string };
type Operation = ItemDefinition & { hash: string; executionId: string };
type Inbox = ItemDefinition & { hash: string; operationId: string };

/** A branch is one logical Cosmos partition. No physical work is started here. */
export class CosmosEnqueueStore {
  constructor(private readonly container: Pick<Container, 'item' | 'items'>) {}
  private async read<T extends ItemDefinition>(id: string, filialId: string): Promise<Stored<T> | null> {
    try {
      const response = await this.container.item(id, filialId).read<T>();
      if (!response.resource) return null;
      if (!response.etag) throw new Error('Documento sem ETag');
      return { value: response.resource, etag: response.etag };
    } catch (error) {
      if (typeof error === 'object' && error !== null && 'code' in error && Number(error.code) === 404) return null;
      throw error;
    }
  }
  private async commit(operations: OperationInput[], filialId: string): Promise<boolean> {
    try {
      const response = await this.container.items.batch(operations, filialId);
      const codes = response.result?.map((entry) => entry.statusCode) ?? [];
      if ([409, 412].includes(response.code ?? 0) || codes.some((code) => [409, 412].includes(code))) return false;
      if ((response.code ?? 200) >= 400 || codes.length !== operations.length || codes.some((code) => code < 200 || code >= 300)) throw new Error('Falha no batch Cosmos');
      return true;
    } catch (error) {
      if (typeof error === 'object' && error !== null && 'code' in error && [409, 412].includes(Number(error.code))) return false;
      throw error;
    }
  }
  async enqueue(command: EnqueueWorkCommand, now = new Date()): Promise<{ executionId: string }> {
    const ids = [command.messageId, command.operationId, command.osId, command.sagaId, command.filialId, command.correlationId, command.quoteId, command.reservationId, command.executionId, command.chargeId, command.paymentId, command.providerOrderId, command.providerPaymentId];
    if (ids.some((id) => typeof id !== 'string' || !/^[a-z0-9_-]{1,128}$/i.test(id)) || !Number.isSafeInteger(command.quoteVersion) || command.quoteVersion < 1 || !Number.isFinite(now.getTime())) throw new Error('Comando inválido');
    const fingerprint = { osId: command.osId, sagaId: command.sagaId, filialId: command.filialId, quoteId: command.quoteId, quoteVersion: command.quoteVersion, reservationId: command.reservationId, executionId: command.executionId, chargeId: command.chargeId, paymentId: command.paymentId, providerOrderId: command.providerOrderId, providerPaymentId: command.providerPaymentId };
    const hash = createHash('sha256').update(JSON.stringify(fingerprint)).digest('hex');
    const operationId = `operation:enqueue:${command.operationId}`; const inboxId = `inbox:enqueue:${command.messageId}`; const gateId = `gate:${command.sagaId}`;
    for (let attempt = 0; attempt < 5; attempt++) {
      const inbox = await this.read<Inbox>(inboxId, command.filialId);
      if (inbox && (inbox.value.operationId !== command.operationId || inbox.value.hash !== hash)) throw new Error('Mensagem reutilizada com outro conteúdo');
      const previous = await this.read<Operation>(operationId, command.filialId);
      if (previous) {
        if (previous.value.hash !== hash) throw new Error('Operação reutilizada com outro conteúdo');
        if (!inbox && !await this.commit([{ operationType: 'Create', resourceBody: { id: inboxId, filialId: command.filialId, operationId: command.operationId, hash } }], command.filialId)) continue;
        return { executionId: previous.value.executionId };
      }
      if (inbox) throw new Error('Mensagem sem operação correspondente');
      const gate = await this.read<Gate>(gateId, command.filialId);
      if (!gate || gate.value.cancelled) throw new Error('Saga cancelada ou sem reserva');
      const reservation = await this.read<Reservation>(`reservation:${command.osId}`, command.filialId);
      if (!reservation || reservation.value.status !== 'RESERVED' || reservation.value.reservationId !== command.reservationId || reservation.value.sagaId !== command.sagaId || reservation.value.osId !== command.osId || reservation.value.quoteId !== command.quoteId || reservation.value.quoteVersion !== command.quoteVersion) throw new Error('Reserva não corresponde ao comando');
      if (await this.read(`task:${command.osId}`, command.filialId)) throw new Error('Execução já existente');
      const messageId = randomUUID(); const type = 'execution.work-enqueued.v1';
      const event = { messageId, type, schemaVersion: 1, occurredAt: now.toISOString(), producer: 'soat-execution-service', aggregateType: 'execution', aggregateId: command.executionId, aggregateVersion: 1, osId: command.osId, sagaId: command.sagaId, filialId: command.filialId, correlationId: command.correlationId, causationId: command.messageId, operationId: command.operationId, ...(command.traceparent ? { traceparent: command.traceparent } : {}), ...(command.tracestate ? { tracestate: command.tracestate } : {}), data: { executionId: command.executionId, reservationId: command.reservationId, quoteId: command.quoteId, quoteVersion: command.quoteVersion, chargeId: command.chargeId, paymentId: command.paymentId } };
      const { _rid: gr, _self: gs, _etag: ge, _attachments: ga, _ts: gt, ...gateBody } = gate.value; void gr; void gs; void ge; void ga; void gt;
      const { _rid: rr, _self: rs, _etag: re, _attachments: ra, _ts: rt, ...reservationBody } = reservation.value; void rr; void rs; void re; void ra; void rt;
      const operations: OperationInput[] = [
        { operationType: 'Replace', id: gateId, ifMatch: gate.etag, resourceBody: gateBody },
        { operationType: 'Replace', id: reservation.value.id, ifMatch: reservation.etag, resourceBody: reservationBody },
        { operationType: 'Create', resourceBody: { id: `task:${command.osId}`, filialId: command.filialId, osId: command.osId, sagaId: command.sagaId, executionId: command.executionId, reservationId: command.reservationId, quoteId: command.quoteId, quoteVersion: command.quoteVersion, chargeId: command.chargeId, paymentId: command.paymentId, providerOrderId: command.providerOrderId, providerPaymentId: command.providerPaymentId, status: 'QUEUED', createdAt: now.toISOString() } },
        { operationType: 'Create', resourceBody: { id: operationId, filialId: command.filialId, hash, executionId: command.executionId } },
        { operationType: 'Create', resourceBody: { id: inboxId, filialId: command.filialId, operationId: command.operationId, hash } },
        { operationType: 'Create', resourceBody: { id: `outbox:${messageId}`, filialId: command.filialId, messageId, status: 'PENDING', payload: event, createdAt: now.toISOString() } },
      ];
      if (await this.commit(operations, command.filialId)) return { executionId: command.executionId };
      await new Promise((resolve) => setTimeout(resolve, 10 * 2 ** attempt));
    }
    throw new Error('Conflito de concorrência Cosmos');
  }
}
