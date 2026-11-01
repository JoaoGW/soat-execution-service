import { Container, OperationInput } from '@azure/cosmos';
import Ajv from 'ajv';
import addFormats from 'ajv-formats';
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { CosmosReservationStore } from './reservas';
import { CosmosEnqueueStore, EnqueueWorkCommand } from './enfileirar';

class FakeCosmos {
  private readonly documents = new Map<string, { value: Record<string, unknown>; etag: string }>();
  private revision = 0;
  conflicts = 0;
  batchError?: Error;
  batchCode?: number;
  missingEtag = false;
  private key(id: string, partition: string) { return `${partition}|${id}`; }
  seed(value: Record<string, unknown> & { id: string; filialId: string }) { this.documents.set(this.key(value.id, value.filialId), { value, etag: `${++this.revision}` }); }
  get(id: string, partition = 'f1') { return this.documents.get(this.key(id, partition))?.value; }
  list(prefix: string) { return [...this.documents.values()].map((entry) => entry.value).filter((value) => String(value.id).startsWith(prefix)); }
  item(id: string, partition: string) {
    return { read: async () => {
      const found = this.documents.get(this.key(id, partition));
      if (!found) throw { code: 404 };
      return { resource: { ...found.value }, etag: this.missingEtag ? undefined : found.etag };
    } };
  }
  items = { batch: async (operations: OperationInput[], partition: string) => {
    if (this.batchError) throw this.batchError;
    if (this.batchCode) return { code: this.batchCode, result: [] };
    if (this.conflicts > 0) { this.conflicts--; return { code: 412 }; }
    for (const operation of operations) {
      if (operation.operationType !== 'Create' && operation.operationType !== 'Replace') throw new Error('Operação inesperada');
      const body = operation.resourceBody as Record<string, unknown>;
      if (body.filialId !== partition) throw new Error('Partição incorreta');
      const id = String(body.id);
      const existing = this.documents.get(this.key(id, partition));
      if (operation.operationType === 'Create' && existing) return { code: 409 };
      if (operation.operationType === 'Replace' && (!existing || operation.ifMatch !== existing.etag)) return { code: 412 };
    }
    for (const operation of operations) {
      if (operation.operationType !== 'Create' && operation.operationType !== 'Replace') throw new Error('Operação inesperada');
      const value = operation.resourceBody as Record<string, unknown>;
      this.documents.set(this.key(String(value.id), partition), { value: { ...value }, etag: `${++this.revision}` });
    }
    return { code: 200, result: operations.map(() => ({ statusCode: 200 })) };
  } };
}

describe('CosmosEnqueueStore', () => {
  const now = new Date('2030-01-01T00:00:00Z');
  const command: EnqueueWorkCommand = { messageId: 'm1', operationId: 'op1', osId: 'os1', sagaId: 's1', filialId: 'f1', correlationId: 'c1', quoteId: 'q1', quoteVersion: 1, reservationId: 'r1', executionId: 'e1', chargeId: 'charge1', paymentId: 'pay1', providerOrderId: 'order1', providerPaymentId: 'provider1' };
  let fake: FakeCosmos; let store: CosmosEnqueueStore;
  beforeEach(() => {
    fake = new FakeCosmos();
    fake.seed({ id: 'gate:s1', filialId: 'f1', cancelled: false });
    fake.seed({ id: 'reservation:os1', filialId: 'f1', sagaId: 's1', osId: 'os1', quoteId: 'q1', quoteVersion: 1, reservationId: 'r1', status: 'RESERVED' });
    store = new CosmosEnqueueStore(fake as unknown as Container);
  });
  it('grava tarefa, inbox e evento no mesmo batch com ETag', async () => {
    await expect(store.enqueue(command, now)).resolves.toEqual({ executionId: 'e1' });
    expect(fake.get('task:os1')).toMatchObject({ status: 'QUEUED', executionId: 'e1', reservationId: 'r1' });
    expect(fake.get('inbox:enqueue:m1')).toBeDefined();
    const events = fake.list('outbox:'); expect(events).toHaveLength(1);
    const dir = resolve(process.cwd(), 'docs/contracts/messages'); const ajv = new Ajv(); addFormats(ajv);
    ajv.addSchema(JSON.parse(readFileSync(resolve(dir, 'envelope.schema.json'), 'utf8')));
    expect(ajv.validate(JSON.parse(readFileSync(resolve(dir, 'execution.work-enqueued.v1.schema.json'), 'utf8')), events[0].payload)).toBe(true);
  });
  it('encadeia reserva e enfileiramento sem iniciar reparo físico', async () => {
    const fresh = new FakeCosmos(); fresh.seed({ id: 'stock:p1', filialId: 'f1', sku: 'p1', balance: 10, reserved: 0 });
    const reservations = new CosmosReservationStore(fresh as unknown as Container);
    await reservations.reserve({ messageId: 'reserve-message', operationId: 'reserve-operation', osId: command.osId, sagaId: command.sagaId, filialId: command.filialId, correlationId: command.correlationId, quoteId: command.quoteId, quoteVersion: command.quoteVersion, reservationId: command.reservationId, deadline: '2030-01-01T00:20:00Z', items: [{ sku: 'p1', quantity: 2 }] }, now);
    await new CosmosEnqueueStore(fresh as unknown as Container).enqueue(command, now);
    expect(fresh.get('reservation:os1')).toMatchObject({ quoteId: 'q1', quoteVersion: 1, status: 'RESERVED' });
    expect(fresh.get('task:os1')).toMatchObject({ status: 'QUEUED' });
    expect(fresh.get('stock:p1')).toMatchObject({ balance: 10, reserved: 2 });
    expect(fresh.list('outbox:')).toHaveLength(2);
  });
  it('deduplica operação e mensagem e rejeita conteúdo alterado', async () => {
    await store.enqueue(command, now);
    await expect(store.enqueue({ ...command, messageId: 'm2' }, now)).resolves.toEqual({ executionId: 'e1' });
    expect(fake.get('inbox:enqueue:m2')).toBeDefined();
    await expect(store.enqueue({ ...command, messageId: 'm3', paymentId: 'other' }, now)).rejects.toThrow('outro conteúdo');
    await expect(store.enqueue({ ...command, operationId: 'other' }, now)).rejects.toThrow('Mensagem reutilizada');
    expect(fake.list('outbox:')).toHaveLength(1);
  });
  it('não enfileira após cancelamento nem com reserva incompatível', async () => {
    const reservationStore = new CosmosReservationStore(fake as unknown as Container);
    await reservationStore.writeCancellationFence('s1', 'f1');
    await expect(store.enqueue(command, now)).rejects.toThrow('Saga cancelada');
    fake.seed({ id: 'gate:s1', filialId: 'f1', cancelled: false });
    await expect(store.enqueue({ ...command, quoteVersion: 2 }, now)).rejects.toThrow('Reserva não corresponde');
    expect(fake.get('task:os1')).toBeUndefined();
  });
  it('serializa cancelamento concorrente com enfileiramento', async () => {
    const reservationStore = new CosmosReservationStore(fake as unknown as Container);
    const [queued, cancelled] = await Promise.allSettled([store.enqueue(command, now), reservationStore.writeCancellationFence('s1', 'f1')]);
    expect(cancelled.status).toBe('fulfilled'); expect(fake.get('gate:s1')).toMatchObject({ cancelled: true });
    if (queued.status === 'fulfilled') expect(fake.get('task:os1')).toMatchObject({ status: 'QUEUED' });
    else expect(fake.get('task:os1')).toBeUndefined();
  });
  it('retenta conflito e impede duas tarefas para mesma OS', async () => {
    fake.conflicts = 1;
    const results = await Promise.allSettled([store.enqueue(command, now), store.enqueue({ ...command, messageId: 'm2', operationId: 'op2', executionId: 'e2' }, now)]);
    expect(results.filter((item) => item.status === 'fulfilled')).toHaveLength(1);
    expect(fake.list('outbox:')).toHaveLength(1);
  });
  it('recusa dados inválidos, sem ETag e falhas do Cosmos', async () => {
    await expect(store.enqueue({ ...command, osId: '../' }, now)).rejects.toThrow('Comando inválido');
    fake.missingEtag = true; await expect(store.enqueue(command, now)).rejects.toThrow('sem ETag');
    fake.missingEtag = false; fake.batchCode = 503; await expect(store.enqueue(command, now)).rejects.toThrow('Falha no batch');
    expect(fake.get('task:os1')).toBeUndefined();
  });
});
