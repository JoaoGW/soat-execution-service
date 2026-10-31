import { createHash, randomUUID } from 'node:crypto';
import { Container, ItemDefinition, OperationInput } from '@azure/cosmos';
import { Line, reserve, Stock } from '../../domain/reserva';

export interface ReservePartsCommand {
  messageId: string; operationId: string; osId: string; sagaId: string; filialId: string;
  correlationId: string; quoteId: string; quoteVersion: number; reservationId: string;
  deadline: string; items: Line[]; traceparent?: string; tracestate?: string;
}

type Stored<T> = { value: T; etag: string };
type Gate = { id: string; filialId: string; cancelled: boolean };
type StockDocument = Stock & { id: string; filialId: string };
type OperationDocument = { id: string; filialId: string; hash: string; reservationId: string };
type InboxDocument = { id: string; filialId: string; operationId: string; hash: string };

/** One branch is one Cosmos logical partition; never mix branches in a batch. */
export class CosmosReservationStore {
  constructor(private readonly container: Pick<Container, 'item' | 'items'>) {}

  private async read<T extends ItemDefinition>(id: string, filialId: string): Promise<Stored<T> | null> {
    try {
      const response = await this.container.item(id, filialId).read<T>();
      if (!response.resource) return null;
      if (!response.etag) throw new Error('Documento sem ETag');
      return { value: response.resource, etag: response.etag };
    } catch (error) {
      if (typeof error === 'object' && error !== null && 'code' in error && error.code === 404) return null;
      throw error;
    }
  }

  private async commit(operations: OperationInput[], filialId: string): Promise<boolean> {
    try {
      const result = await this.container.items.batch(operations, filialId);
      const codes = result.result?.map((item) => item.statusCode) ?? [];
      if (result.code === 409 || result.code === 412 || codes.some((code) => code === 409 || code === 412)) return false;
      if ((result.code !== undefined && result.code >= 400) || codes.length !== operations.length || codes.some((code) => code < 200 || code >= 300)) throw new Error('Falha no batch Cosmos');
      return true;
    } catch (error) {
      if (typeof error === 'object' && error !== null && 'code' in error && (error.code === 409 || error.code === 412)) return false;
      throw error;
    }
  }

  async reserve(command: ReservePartsCommand, now = new Date()): Promise<{ reservationId: string }> {
    if (!Array.isArray(command.items)) throw new Error('Comando inválido');
    const ids = [command.messageId, command.operationId, command.osId, command.sagaId, command.quoteId, command.reservationId, ...command.items.map((item) => item.sku)];
    if (ids.some((id) => typeof id !== 'string' || !/^[a-z0-9_-]{1,128}$/i.test(id)) || !command.filialId || !command.correlationId ||
        !Number.isSafeInteger(command.quoteVersion) || command.quoteVersion < 1 || !Number.isFinite(Date.parse(command.deadline)) || !Number.isFinite(now.getTime())) throw new Error('Comando inválido');
    const fingerprint = { osId: command.osId, sagaId: command.sagaId, filialId: command.filialId, quoteId: command.quoteId, quoteVersion: command.quoteVersion, reservationId: command.reservationId, deadline: command.deadline, items: command.items.map(({ sku, quantity }) => ({ sku, quantity })) };
    const hash = createHash('sha256').update(JSON.stringify(fingerprint)).digest('hex');
    const operationId = `operation:reserve:${command.operationId}`;
    const inboxId = `inbox:reserve:${command.messageId}`;
    const gateId = `gate:${command.sagaId}`;
    for (let attempt = 0; attempt < 5; attempt++) {
      const inbox = await this.read<InboxDocument>(inboxId, command.filialId);
      if (inbox && (inbox.value.operationId !== command.operationId || inbox.value.hash !== hash)) throw new Error('Mensagem reutilizada com outro conteúdo');
      const previous = await this.read<OperationDocument>(operationId, command.filialId);
      if (previous) {
        if (previous.value.hash !== hash) throw new Error('Operação reutilizada com outro conteúdo');
        if (!inbox) {
          const operation: OperationInput = { operationType: 'Create', resourceBody: { id: inboxId, filialId: command.filialId, operationId: command.operationId, hash } };
          if (!await this.commit([operation], command.filialId)) continue;
        }
        return { reservationId: previous.value.reservationId };
      }
      if (inbox) throw new Error('Mensagem sem operação correspondente');
      if (Date.parse(command.deadline) <= now.getTime()) throw new Error('Prazo de reserva expirado');
      const gate = await this.read<Gate>(gateId, command.filialId);
      if (gate?.value.cancelled) throw new Error('Saga cancelada');
      const reservationId = `reservation:${command.osId}`;
      if (await this.read(reservationId, command.filialId)) throw new Error('Reserva já existente');
      const skus = [...new Set(command.items.map((item) => item.sku))];
      if (skus.length > 20) throw new Error('Máximo de 20 SKUs');
      const stock = await Promise.all(skus.map((sku) => this.read<StockDocument>(`stock:${sku}`, command.filialId)));
      if (stock.some((item) => !item)) throw new Error('Estoque insuficiente');
      const entries = stock as Stored<StockDocument>[];
      const next = reserve(command.reservationId, command.items, entries.map((item) => item.value));
      const messageId = randomUUID();
      const eventType = 'execution.parts-reserved.v1';
      const event = { messageId, type: eventType, schemaVersion: 1, occurredAt: now.toISOString(), producer: 'soat-execution-service', aggregateType: 'reservation', aggregateId: command.reservationId, aggregateVersion: 1, osId: command.osId, sagaId: command.sagaId, filialId: command.filialId, correlationId: command.correlationId, causationId: command.messageId, operationId: command.operationId, ...(command.traceparent ? { traceparent: command.traceparent } : {}), ...(command.tracestate ? { tracestate: command.tracestate } : {}), data: { quoteId: command.quoteId, quoteVersion: command.quoteVersion, reservationId: command.reservationId, deadline: command.deadline, items: next.reservation.items } };
      const gateBody = { id: gateId, filialId: command.filialId, cancelled: false };
      const operations: OperationInput[] = [
        gate ? { operationType: 'Replace', id: gateId, ifMatch: gate.etag, resourceBody: gateBody } : { operationType: 'Create', resourceBody: gateBody },
        ...entries.map((entry, index): OperationInput => ({ operationType: 'Replace', id: entry.value.id, ifMatch: entry.etag, resourceBody: { id: entry.value.id, filialId: command.filialId, ...next.stock[index] } })),
        { operationType: 'Create', resourceBody: { ...next.reservation, id: reservationId, filialId: command.filialId, sagaId: command.sagaId, osId: command.osId, reservationId: command.reservationId, deadline: command.deadline } },
        { operationType: 'Create', resourceBody: { id: operationId, filialId: command.filialId, hash, reservationId: command.reservationId } },
        { operationType: 'Create', resourceBody: { id: inboxId, filialId: command.filialId, operationId: command.operationId, hash } },
        { operationType: 'Create', resourceBody: { id: `outbox:${messageId}`, filialId: command.filialId, messageId, status: 'PENDING', payload: event, createdAt: now.toISOString() } },
      ];
      if (await this.commit(operations, command.filialId)) return { reservationId: command.reservationId };
      await new Promise((resolve) => setTimeout(resolve, 10 * 2 ** attempt));
    }
    throw new Error('Conflito de concorrência Cosmos');
  }

  /** Writes a fence only; does not prove that physical work has not started. */
  async writeCancellationFence(sagaId: string, filialId: string): Promise<void> {
    if (!/^[a-z0-9_-]{1,128}$/i.test(sagaId) || !filialId) throw new Error('Cancelamento inválido');
    const id = `gate:${sagaId}`;
    for (let attempt = 0; attempt < 5; attempt++) {
      const gate = await this.read<Gate>(id, filialId);
      if (gate?.value.cancelled) return;
      const body = { id, filialId, cancelled: true };
      const operation: OperationInput = gate ? { operationType: 'Replace', id, ifMatch: gate.etag, resourceBody: body } : { operationType: 'Create', resourceBody: body };
      if (await this.commit([operation], filialId)) return;
      await new Promise((resolve) => setTimeout(resolve, 10 * 2 ** attempt));
    }
    throw new Error('Conflito de concorrência Cosmos');
  }
}
