import { createHash, randomUUID } from 'node:crypto';
import { Container, ItemDefinition, OperationInput } from '@azure/cosmos';
import { Line, Reservation, settle, Stock } from '../../domain/reserva';

export interface WorkAction {
  messageId: string; operationId: string; osId: string; sagaId: string; filialId: string; correlationId: string;
  executionId?: string; reservationId?: string; cancelOperationId?: string; reason?: string; traceparent?: string; tracestate?: string;
}
type Action = 'start' | 'complete' | 'cancel' | 'release';
type Outcome = 'STARTED' | 'COMPLETED' | 'CANCELLED' | 'ALREADY_STARTED' | 'RELEASED' | 'NO_RESERVATION';
type Stored<T> = { value: T; etag: string };
type Gate = ItemDefinition & { id: string; filialId: string; cancelled: boolean };
type Task = ItemDefinition & { id: string; filialId: string; osId: string; sagaId: string; executionId: string; reservationId: string; status: string; version?: number; startedAt?: string };
type StoredReservation = ItemDefinition & { id: string; filialId: string; osId: string; sagaId: string; reservationId: string; status: 'RESERVED' | 'CONSUMED' | 'RELEASED'; items: Line[] };
type StockDoc = ItemDefinition & Stock & { id: string; filialId: string };
type Ledger = ItemDefinition & { hash: string; outcome: Outcome; osId: string; sagaId: string };
type Inbox = ItemDefinition & { hash: string; operationId: string };

function clean<T extends ItemDefinition>(value: T): ItemDefinition {
  const { _rid, _self, _etag, _attachments, _ts, ...rest } = value;
  void _rid; void _self; void _etag; void _attachments; void _ts;
  return rest as ItemDefinition;
}

/** The caller must authorize actions and validate commands before invoking this store. */
export class CosmosWorkStore {
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
  async start(input: WorkAction, now = new Date()): Promise<Outcome> { return this.apply('start', input, now); }
  async complete(input: WorkAction, now = new Date()): Promise<Outcome> { return this.apply('complete', input, now); }
  async cancel(input: WorkAction, now = new Date()): Promise<Outcome> { return this.apply('cancel', input, now); }
  async release(input: WorkAction, now = new Date()): Promise<Outcome> { return this.apply('release', input, now); }

  private async apply(action: Action, input: WorkAction, now: Date): Promise<Outcome> {
    const ids = [input.messageId, input.operationId, input.osId, input.sagaId, input.filialId, input.correlationId, ...(input.executionId ? [input.executionId] : []), ...(input.reservationId ? [input.reservationId] : []), ...(input.cancelOperationId ? [input.cancelOperationId] : [])];
    if (ids.some((id) => typeof id !== 'string' || !/^[a-z0-9_-]{1,128}$/i.test(id)) || (['start', 'complete'].includes(action) && !input.executionId) || (action === 'release' && (!input.reservationId || !input.cancelOperationId)) || (['cancel', 'release'].includes(action) && (typeof input.reason !== 'string' || !/^[a-z0-9_-]{1,128}$/i.test(input.reason))) || !Number.isFinite(now.getTime())) throw new Error('Comando inválido');
    const hash = createHash('sha256').update(JSON.stringify({ action, osId: input.osId, sagaId: input.sagaId, filialId: input.filialId, executionId: input.executionId ?? null, reason: ['cancel', 'release'].includes(action) ? input.reason : null, reservationId: input.reservationId ?? null, cancelOperationId: input.cancelOperationId ?? null })).digest('hex');
    const opId = `operation:${action}:${input.operationId}`; const inboxId = `inbox:${action}:${input.messageId}`; const gateId = `gate:${input.sagaId}`; const taskId = `task:${input.osId}`;
    for (let attempt = 0; attempt < 5; attempt++) {
      const inbox = await this.read<Inbox>(inboxId, input.filialId);
      if (inbox && (inbox.value.operationId !== input.operationId || inbox.value.hash !== hash)) throw new Error('Mensagem reutilizada com outro conteúdo');
      const previous = await this.read<Ledger>(opId, input.filialId);
      if (previous) {
        if (previous.value.hash !== hash) throw new Error('Operação reutilizada com outro conteúdo');
        if (!inbox && !await this.commit([{ operationType: 'Create', resourceBody: { id: inboxId, filialId: input.filialId, operationId: input.operationId, hash } }], input.filialId)) continue;
        return previous.value.outcome;
      }
      if (inbox) throw new Error('Mensagem sem operação correspondente');
      const gate = await this.read<Gate>(gateId, input.filialId); const task = await this.read<Task>(taskId, input.filialId);
      if (task && (task.value.sagaId !== input.sagaId || task.value.osId !== input.osId || (input.executionId && task.value.executionId !== input.executionId))) throw new Error('Execução não corresponde ao comando');
      const currentVersion = task?.value.version ?? 1;
      if (!Number.isSafeInteger(currentVersion) || currentVersion < 1 || currentVersion >= Number.MAX_SAFE_INTEGER) throw new Error('Versão de execução inválida');
      const operations: OperationInput[] = [];
      let outcome: Outcome; let type: string; let data: Record<string, string>;
      if (action === 'cancel') {
        outcome = task && ['STARTED', 'COMPLETED'].includes(task.value.status) ? 'ALREADY_STARTED' : 'CANCELLED';
        if (task && !['QUEUED', 'STARTED', 'COMPLETED', 'CANCELLED'].includes(task.value.status)) throw new Error('Estado de execução inválido');
        const body = { ...(gate ? clean(gate.value) : {}), id: gateId, filialId: input.filialId, cancelled: true };
        operations.push(gate ? { operationType: 'Replace', id: gateId, ifMatch: gate.etag, resourceBody: body } : { operationType: 'Create', resourceBody: body });
        if (task && task.value.status === 'QUEUED') operations.push({ operationType: 'Replace', id: taskId, ifMatch: task.etag, resourceBody: { ...clean(task.value), status: 'CANCELLED', version: currentVersion + 1, cancelledAt: now.toISOString() } });
        type = 'execution.work-cancelled.v1'; data = { outcome, reason: input.reason as string, ...(task ? { executionId: task.value.executionId } : input.executionId ? { executionId: input.executionId } : {}) };
      } else if (action === 'release') {
        const cancellation = await this.read<Ledger>(`operation:cancel:${input.cancelOperationId}`, input.filialId);
        if (!gate || !gate.value.cancelled || !cancellation || cancellation.value.outcome !== 'CANCELLED' || cancellation.value.osId !== input.osId || cancellation.value.sagaId !== input.sagaId || (task && task.value.status !== 'CANCELLED')) throw new Error('Cancelamento sem confirmação de não início');
        operations.push({ operationType: 'Replace', id: gateId, ifMatch: gate.etag, resourceBody: clean(gate.value) });
        if (task) operations.push({ operationType: 'Replace', id: taskId, ifMatch: task.etag, resourceBody: clean(task.value) });
        const reservation = await this.read<StoredReservation>(`reservation:${input.osId}`, input.filialId);
        if (reservation) {
          if (reservation.value.reservationId !== input.reservationId || reservation.value.sagaId !== input.sagaId || reservation.value.osId !== input.osId || reservation.value.status !== 'RESERVED' || !Array.isArray(reservation.value.items) || reservation.value.items.length > 20) throw new Error('Reserva não pode ser liberada');
          const stocks = await Promise.all(reservation.value.items.map((line) => this.read<StockDoc>(`stock:${line.sku}`, input.filialId)));
          if (stocks.some((stock) => !stock)) throw new Error('Estoque inválido');
          const entries = stocks as Stored<StockDoc>[];
          const transition = settle({ id: reservation.value.reservationId, status: 'RESERVED', items: reservation.value.items } satisfies Reservation, entries.map((entry) => entry.value), 'RELEASE');
          operations.push({ operationType: 'Replace', id: reservation.value.id, ifMatch: reservation.etag, resourceBody: { ...clean(reservation.value), status: 'RELEASED' } }, ...entries.map((entry, index): OperationInput => ({ operationType: 'Replace', id: entry.value.id, ifMatch: entry.etag, resourceBody: { id: entry.value.id, filialId: input.filialId, ...transition.stock[index] } })));
        }
        outcome = reservation ? 'RELEASED' : 'NO_RESERVATION'; type = 'execution.parts-released.v1'; data = { outcome, reservationId: input.reservationId as string, cancelOperationId: input.cancelOperationId as string, reason: input.reason as string };
      } else if (action === 'start') {
        if (!gate || gate.value.cancelled || !task || task.value.status !== 'QUEUED') throw new Error('Trabalho não pode ser iniciado');
        const reservation = await this.read<StoredReservation>(`reservation:${input.osId}`, input.filialId);
        if (!reservation || reservation.value.status !== 'RESERVED' || reservation.value.reservationId !== task.value.reservationId || reservation.value.sagaId !== input.sagaId || reservation.value.osId !== input.osId || !Array.isArray(reservation.value.items) || reservation.value.items.length > 20) throw new Error('Reserva inválida');
        const stocks = await Promise.all(reservation.value.items.map((line) => this.read<StockDoc>(`stock:${line.sku}`, input.filialId)));
        if (stocks.some((stock) => !stock)) throw new Error('Estoque inválido');
        const entries = stocks as Stored<StockDoc>[];
        const transition = settle({ id: reservation.value.reservationId, status: 'RESERVED', items: reservation.value.items } satisfies Reservation, entries.map((entry) => entry.value), 'CONSUME');
        operations.push({ operationType: 'Replace', id: gateId, ifMatch: gate.etag, resourceBody: clean(gate.value) }, { operationType: 'Replace', id: taskId, ifMatch: task.etag, resourceBody: { ...clean(task.value), status: 'STARTED', version: currentVersion + 1, startedAt: now.toISOString() } }, { operationType: 'Replace', id: reservation.value.id, ifMatch: reservation.etag, resourceBody: { ...clean(reservation.value), status: 'CONSUMED' } }, ...entries.map((entry, index): OperationInput => ({ operationType: 'Replace', id: entry.value.id, ifMatch: entry.etag, resourceBody: { id: entry.value.id, filialId: input.filialId, ...transition.stock[index] } })));
        outcome = 'STARTED'; type = 'execution.work-started.v1'; data = { executionId: task.value.executionId, reservationId: task.value.reservationId, startedAt: now.toISOString() };
      } else {
        if (!task || task.value.status !== 'STARTED' || !task.value.startedAt || !Number.isFinite(Date.parse(task.value.startedAt)) || Date.parse(task.value.startedAt) > now.getTime()) throw new Error('Trabalho não pode ser concluído');
        operations.push({ operationType: 'Replace', id: taskId, ifMatch: task.etag, resourceBody: { ...clean(task.value), status: 'COMPLETED', version: currentVersion + 1, completedAt: now.toISOString() } });
        outcome = 'COMPLETED'; type = 'execution.work-completed.v1'; data = { executionId: task.value.executionId, reservationId: task.value.reservationId, startedAt: task.value.startedAt, completedAt: now.toISOString() };
      }
      const messageId = randomUUID(); const aggregateId = task?.value.executionId ?? input.executionId ?? input.sagaId;
      const event = { messageId, type, schemaVersion: 1, occurredAt: now.toISOString(), producer: 'soat-execution-service', aggregateType: action === 'cancel' ? 'work-cancellation' : action === 'release' ? 'stock-release' : 'execution', aggregateId: ['cancel', 'release'].includes(action) ? input.operationId : aggregateId, aggregateVersion: ['cancel', 'release'].includes(action) ? 1 : currentVersion + 1, osId: input.osId, sagaId: input.sagaId, filialId: input.filialId, correlationId: input.correlationId, causationId: input.messageId, operationId: input.operationId, ...(input.traceparent ? { traceparent: input.traceparent } : {}), ...(input.tracestate ? { tracestate: input.tracestate } : {}), data };
      operations.push({ operationType: 'Create', resourceBody: { id: opId, filialId: input.filialId, osId: input.osId, sagaId: input.sagaId, hash, outcome } }, { operationType: 'Create', resourceBody: { id: inboxId, filialId: input.filialId, operationId: input.operationId, hash } }, { operationType: 'Create', resourceBody: { id: `outbox:${messageId}`, filialId: input.filialId, messageId, status: 'PENDING', payload: event, createdAt: now.toISOString() } });
      if (await this.commit(operations, input.filialId)) return outcome;
      await new Promise((resolve) => setTimeout(resolve, 10 * 2 ** attempt));
    }
    throw new Error('Conflito de concorrência Cosmos');
  }
}
