import { Container, OperationInput } from '@azure/cosmos';
import Ajv from 'ajv';
import addFormats from 'ajv-formats';
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { CosmosReservationStore, ReservePartsCommand } from './reservas';

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

describe('CosmosReservationStore', () => {
  const now = new Date('2030-01-01T00:00:00Z');
  const base: ReservePartsCommand = { messageId: 'm1', operationId: 'op1', osId: 'os1', sagaId: 's1', filialId: 'f1', correlationId: 'c1', quoteId: 'q1', quoteVersion: 1, reservationId: 'r1', deadline: '2030-01-01T00:20:00Z', items: [{ sku: 'p1', quantity: 7 }] };
  let fake: FakeCosmos;
  let store: CosmosReservationStore;
  beforeEach(() => { fake = new FakeCosmos(); fake.seed({ id: 'stock:p1', filialId: 'f1', sku: 'p1', balance: 10, reserved: 0 }); store = new CosmosReservationStore(fake as unknown as Container); });

  it('grava reserva, estoque, inbox e outbox no mesmo batch', async () => {
    await expect(store.reserve(base, now)).resolves.toEqual({ reservationId: 'r1' });
    expect(fake.get('stock:p1')).toMatchObject({ balance: 10, reserved: 7 });
    expect(fake.get('reservation:os1')).toMatchObject({ reservationId: 'r1', status: 'RESERVED' });
    expect(fake.get('inbox:reserve:m1')).toBeDefined();
    expect(fake.list('outbox:')).toHaveLength(1);
    const dir = resolve(process.cwd(), 'docs/contracts/messages');
    const ajv = new Ajv();
    addFormats(ajv);
    ajv.addSchema(JSON.parse(readFileSync(resolve(dir, 'envelope.schema.json'), 'utf8')));
    const schema = JSON.parse(readFileSync(resolve(dir, 'execution.parts-reserved.v1.schema.json'), 'utf8'));
    expect(ajv.validate(schema, fake.list('outbox:')[0].payload)).toBe(true);
  });
  it('deduplica nova mensagem da mesma operação e rejeita conteúdo alterado', async () => {
    await store.reserve(base, now);
    await expect(store.reserve({ ...base, messageId: 'm2' }, new Date('2040-01-01T00:00:00Z'))).resolves.toEqual({ reservationId: 'r1' });
    await expect(store.reserve({ ...base, messageId: 'm3', items: [{ sku: 'p1', quantity: 6 }] }, now)).rejects.toThrow('outro conteúdo');
    expect(fake.get('stock:p1')).toMatchObject({ reserved: 7 });
    expect(fake.get('inbox:reserve:m2')).toBeDefined();
    expect(fake.list('outbox:')).toHaveLength(1);
  });
  it('impede reserva após a criação do bloqueio de cancelamento', async () => {
    await store.writeCancellationFence('s1', 'f1');
    await store.writeCancellationFence('s1', 'f1');
    await expect(store.reserve(base, now)).rejects.toThrow('Saga cancelada');
    expect(fake.get('stock:p1')).toMatchObject({ reserved: 0 });
  });
  it('serializa a corrida entre reserva e bloqueio de cancelamento', async () => {
    const [reservation, cancellation] = await Promise.allSettled([store.reserve(base, now), store.writeCancellationFence('s1', 'f1')]);
    expect(cancellation.status).toBe('fulfilled');
    expect(fake.get('gate:s1')).toMatchObject({ cancelled: true });
    if (reservation.status === 'fulfilled') {
      expect(fake.get('stock:p1')).toMatchObject({ reserved: 7 });
      expect(fake.list('outbox:')).toHaveLength(1);
    } else {
      expect(fake.get('stock:p1')).toMatchObject({ reserved: 0 });
      expect(fake.list('outbox:')).toHaveLength(0);
    }
    await expect(store.reserve({ ...base, messageId: 'new', operationId: 'new', osId: 'new', reservationId: 'new' }, now)).rejects.toThrow('Saga cancelada');
  });
  it('retenta conflito de ETag e impede duas reservas acima do saldo', async () => {
    fake.conflicts = 1;
    const other = { ...base, messageId: 'm2', operationId: 'op2', osId: 'os2', sagaId: 's2', reservationId: 'r2' };
    const results = await Promise.allSettled([store.reserve(base, now), store.reserve(other, now)]);
    expect(results.filter((result) => result.status === 'fulfilled' && !result.value.reason)).toHaveLength(1);
    expect(results.filter((result) => result.status === 'fulfilled' && result.value.reason === 'INSUFFICIENT_STOCK')).toHaveLength(1);
    expect(fake.get('stock:p1')).toMatchObject({ reserved: 7 });
    expect(fake.list('outbox:')).toHaveLength(2);
  });
  it('rejeita entradas inválidas, prazos vencidos e estoque inexistente', async () => {
    await expect(store.reserve({ ...base, osId: 'inválido/' }, now)).rejects.toThrow('Comando inválido');
    await expect(store.reserve({ ...base, quoteVersion: 0 }, now)).rejects.toThrow('Comando inválido');
    await expect(store.reserve({ ...base, deadline: 'invalid' }, now)).rejects.toThrow('Comando inválido');
    await expect(store.reserve(base, new Date('2040-01-01'))).resolves.toMatchObject({ reason: 'EXPIRED' });
    await expect(store.reserve({ ...base, messageId: 'missing', operationId: 'missing', items: [{ sku: 'missing', quantity: 1 }] }, now)).resolves.toMatchObject({ reason: 'INSUFFICIENT_STOCK' });
    await expect(store.writeCancellationFence('invalid/', 'f1')).rejects.toThrow('Cancelamento inválido');
  });
  it('persiste falha de estoque uma vez, valida evento e mantém resultado após reposição', async () => {
    const command = { ...base, items: [{ sku: 'p1', quantity: 11 }] };
    await expect(store.reserve(command, now)).resolves.toMatchObject({ reason: 'INSUFFICIENT_STOCK' });
    fake.seed({ id: 'stock:p1', filialId: 'f1', sku: 'p1', balance: 20, reserved: 0 });
    await expect(store.reserve({ ...command, messageId: 'retry' }, now)).resolves.toMatchObject({ reason: 'INSUFFICIENT_STOCK' });
    expect(fake.get('stock:p1')).toMatchObject({ reserved: 0 });
    expect(fake.get('inbox:reserve:retry')).toBeDefined();
    expect(fake.list('outbox:')).toHaveLength(1);
    const dir = resolve(process.cwd(), 'docs/contracts/messages');
    const ajv = new Ajv(); addFormats(ajv);
    ajv.addSchema(JSON.parse(readFileSync(resolve(dir, 'envelope.schema.json'), 'utf8')));
    expect(ajv.validate(JSON.parse(readFileSync(resolve(dir, 'execution.parts-reservation-failed.v1.schema.json'), 'utf8')), fake.list('outbox:')[0].payload)).toBe(true);
  });
  it('não confirma falha de negócio se o batch não for persistido', async () => {
    fake.batchCode = 503;
    await expect(store.reserve({ ...base, items: [{ sku: 'missing', quantity: 1 }] }, now)).rejects.toThrow('Falha no batch');
    expect(fake.list('outbox:')).toHaveLength(0);
    expect(fake.get('inbox:reserve:m1')).toBeUndefined();
  });
  it('impede reutilização de mensagem e reserva com operação diferente', async () => {
    await store.reserve(base, now);
    await expect(store.reserve({ ...base, operationId: 'other' }, now)).rejects.toThrow('Mensagem reutilizada');
    await expect(store.reserve({ ...base, messageId: 'other', operationId: 'other' }, now)).rejects.toThrow('Reserva já existente');
    await store.writeCancellationFence('s1', 'f1');
    expect(fake.get('gate:s1')).toMatchObject({ cancelled: true });
  });
  it('não grava efeitos após erro do serviço ou resposta inválida', async () => {
    fake.batchError = new Error('Cosmos indisponível');
    await expect(store.reserve(base, now)).rejects.toThrow('Cosmos indisponível');
    fake.batchError = undefined;
    fake.batchCode = 503;
    await expect(store.reserve(base, now)).rejects.toThrow('Falha no batch Cosmos');
    expect(fake.get('stock:p1')).toMatchObject({ reserved: 0 });
    expect(fake.list('outbox:')).toHaveLength(0);
  });
  it('exige ETag e limita os conflitos transitórios', async () => {
    fake.missingEtag = true;
    await expect(store.reserve(base, now)).rejects.toThrow('sem ETag');
    fake.missingEtag = false;
    fake.conflicts = 5;
    await expect(store.reserve(base, now)).rejects.toThrow('Conflito de concorrência');
  });
});
