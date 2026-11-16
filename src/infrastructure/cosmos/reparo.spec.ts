import { Container, OperationInput } from '@azure/cosmos';
import Ajv from 'ajv';
import addFormats from 'ajv-formats';
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { CosmosWorkStore, WorkAction } from './reparo';
import { CosmosEnqueueStore } from './enfileirar';
import { BusinessError } from '../../domain/erros';

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

describe('CosmosWorkStore', () => {
  const now = new Date('2030-01-01T00:00:00Z');
  const base: WorkAction = { messageId: 'm1', operationId: 'op1', osId: 'os1', sagaId: 's1', filialId: 'f1', correlationId: 'c1', executionId: 'e1', reason: 'SAGA_COMPENSATION' };
  let fake: FakeCosmos; let store: CosmosWorkStore;
  beforeEach(() => {
    fake = new FakeCosmos(); fake.seed({ id: 'gate:s1', filialId: 'f1', cancelled: false });
    fake.seed({ id: 'task:os1', filialId: 'f1', osId: 'os1', sagaId: 's1', executionId: 'e1', reservationId: 'r1', status: 'QUEUED', version: 1 });
    fake.seed({ id: 'reservation:os1', filialId: 'f1', osId: 'os1', sagaId: 's1', reservationId: 'r1', status: 'RESERVED', items: [{ sku: 'p1', quantity: 2 }] });
    fake.seed({ id: 'stock:p1', filialId: 'f1', sku: 'p1', balance: 10, reserved: 2 });
    store = new CosmosWorkStore(fake as unknown as Container);
  });
  function eventValid(event: Record<string, unknown>) {
    const dir = resolve(process.cwd(), 'docs/contracts/messages'); const ajv = new Ajv(); addFormats(ajv);
    ajv.addSchema(JSON.parse(readFileSync(resolve(dir, 'envelope.schema.json'), 'utf8')));
    return ajv.validate(JSON.parse(readFileSync(resolve(dir, `${String(event.type)}.schema.json`), 'utf8')), event);
  }
  it('inicia e conclui reparo sem peças', async () => {
    fake.seed({ id: 'reservation:os1', filialId: 'f1', osId: 'os1', sagaId: 's1', reservationId: 'r1', status: 'RESERVED', items: [] });
    await expect(store.start(base, now)).resolves.toBe('STARTED');
    await expect(store.complete({ ...base, messageId: 'finish', operationId: 'finish' }, new Date('2030-01-01T01:00:00Z'))).resolves.toBe('COMPLETED');
  });
  it('consome reserva atomicamente, registra início, conclusão e cancelamento tardio', async () => {
    await expect(store.start(base, now)).resolves.toBe('STARTED');
    expect(fake.get('stock:p1')).toMatchObject({ balance: 8, reserved: 0 });
    expect(fake.get('reservation:os1')).toMatchObject({ status: 'CONSUMED' });
    expect(fake.get('task:os1')).toMatchObject({ status: 'STARTED', version: 2 });
    await expect(store.cancel({ ...base, messageId: 'cancel', operationId: 'cancel' }, now)).resolves.toBe('ALREADY_STARTED');
    await expect(store.complete({ ...base, messageId: 'finish', operationId: 'finish' }, new Date('2030-01-01T01:00:00Z'))).resolves.toBe('COMPLETED');
    expect(fake.get('task:os1')).toMatchObject({ status: 'COMPLETED', version: 3 });
    for (const row of fake.list('outbox:')) expect(eventValid(row.payload as Record<string, unknown>)).toBe(true);
  });
  it('registra falha do reparo iniciado com evento válido, sem devolver peças, e bloqueia novas transições', async () => {
    await expect(store.start(base, now)).resolves.toBe('STARTED');
    const failure = { ...base, messageId: 'fail', operationId: 'fail', reason: 'REPARO_INVIAVEL' }; const later = new Date('2030-01-01T02:00:00Z');
    await expect(store.fail(failure, later)).resolves.toBe('FAILED');
    expect(fake.get('task:os1')).toMatchObject({ status: 'FAILED', version: 3, failedAt: later.toISOString(), failureReason: 'REPARO_INVIAVEL' });
    expect(fake.get('stock:p1')).toMatchObject({ balance: 8, reserved: 0 }); expect(fake.get('reservation:os1')).toMatchObject({ status: 'CONSUMED' });
    const event = fake.list('outbox:').map((row) => row.payload as Record<string, unknown>).find((payload) => payload.type === 'execution.work-failed.v1');
    expect(event).toMatchObject({ aggregateType: 'execution', aggregateId: 'e1', aggregateVersion: 3, operationId: 'fail', causationId: 'fail', data: { executionId: 'e1', reservationId: 'r1', startedAt: now.toISOString(), failedAt: later.toISOString(), reason: 'REPARO_INVIAVEL' } });
    expect(eventValid(event as Record<string, unknown>)).toBe(true);
    await expect(store.fail({ ...failure, messageId: 'fail-retry' }, later)).resolves.toBe('FAILED');
    expect(fake.get('inbox:fail:fail-retry')).toBeDefined();
    await expect(store.fail({ ...failure, messageId: 'fail-other', reason: 'OUTRO' }, later)).rejects.toThrow('Operação reutilizada');
    expect(fake.list('outbox:').filter((row) => (row.payload as Record<string, unknown>).type === 'execution.work-failed.v1')).toHaveLength(1);
    await expect(store.cancel({ ...base, messageId: 'cancel', operationId: 'cancel' }, later)).resolves.toBe('ALREADY_STARTED');
    await expect(store.complete({ ...base, messageId: 'finish', operationId: 'finish' }, later)).rejects.toThrow('não pode ser concluído');
    await expect(store.start({ ...base, messageId: 'again', operationId: 'again' }, later)).rejects.toThrow('não pode ser iniciado');
    await expect(store.fail({ ...failure, messageId: 'fail2', operationId: 'fail2' }, later)).rejects.toThrow('marcado como falho');
    await expect(store.release({ ...base, messageId: 'release', operationId: 'release', reservationId: 'r1', cancelOperationId: 'cancel' }, later)).rejects.toThrow('Cancelamento sem confirmação');
    expect(fake.get('task:os1')).toMatchObject({ status: 'FAILED', version: 3 });
  });
  it('recusa falha antes do início, sem motivo válido ou de outra execução, com erros de negócio tipados', async () => {
    const failure = { ...base, messageId: 'fail', operationId: 'fail', reason: 'REPARO_INVIAVEL' };
    await expect(store.fail(failure, now)).rejects.toMatchObject({ kind: 'CONFLICT', message: 'Trabalho não pode ser marcado como falho' });
    await expect(store.fail({ ...failure, reason: 'com espaço' }, now)).rejects.toMatchObject({ kind: 'INVALID_INPUT', message: 'Comando inválido' });
    await expect(store.fail({ ...failure, executionId: undefined }, now)).rejects.toThrow('Comando inválido');
    await store.start(base, now);
    await expect(store.fail({ ...failure, executionId: 'other' }, now)).rejects.toMatchObject({ kind: 'CONFLICT', message: 'Execução não corresponde ao comando' });
    await expect(store.fail(failure, new Date('2029-12-31T23:00:00Z'))).rejects.toThrow('marcado como falho');
    await expect(store.complete({ ...base, messageId: 'x', operationId: 'x' }, new Date('2029-12-31T23:00:00Z'))).rejects.toBeInstanceOf(BusinessError);
    expect(fake.get('task:os1')).toMatchObject({ status: 'STARTED' });
  });
  it('trata estado desconhecido no cancelamento como erro inesperado', async () => {
    fake.seed({ id: 'task:os1', filialId: 'f1', osId: 'os1', sagaId: 's1', executionId: 'e1', reservationId: 'r1', status: 'UNKNOWN', version: 1 });
    const error = await store.cancel(base, now).catch((caught: unknown) => caught);
    expect(error).toBeInstanceOf(Error); expect(error).not.toBeInstanceOf(BusinessError); expect((error as Error).message).toBe('Estado de execução inválido');
  });
  it('esgota tentativas sob concorrência com erro de conflito tipado', async () => {
    fake.conflicts = 5;
    await expect(store.start(base, now)).rejects.toMatchObject({ kind: 'CONFLICT', message: 'Conflito de concorrência Cosmos' });
  });
  it('cancelamento anterior impede início e não devolve estoque sozinho', async () => {
    await expect(store.cancel(base, now)).resolves.toBe('CANCELLED');
    expect(fake.get('task:os1')).toMatchObject({ status: 'CANCELLED' });
    expect(fake.get('stock:p1')).toMatchObject({ balance: 10, reserved: 2 });
    await expect(store.start({ ...base, messageId: 'start', operationId: 'start' }, now)).rejects.toThrow('não pode ser iniciado');
  });
  it('cancelamento antes da tarefa cria barreira que impede enqueue', async () => {
    const fresh = new FakeCosmos(); const worker = new CosmosWorkStore(fresh as unknown as Container);
    await expect(worker.cancel({ ...base, executionId: undefined }, now)).resolves.toBe('CANCELLED');
    await expect(new CosmosEnqueueStore(fresh as unknown as Container).enqueue({ messageId: 'e', operationId: 'e', osId: 'os1', sagaId: 's1', filialId: 'f1', correlationId: 'c1', quoteId: 'q1', quoteVersion: 1, reservationId: 'r1', executionId: 'e1', chargeId: 'ch1', paymentId: 'p1', providerOrderId: 'ord1', providerPaymentId: 'pid1' }, now)).rejects.toThrow('Saga cancelada');
  });
  it('disputa cancelamento e início e nunca faz rollback físico', async () => {
    const [started, cancelled] = await Promise.allSettled([store.start(base, now), store.cancel({ ...base, messageId: 'cancel', operationId: 'cancel' }, now)]);
    expect(cancelled.status).toBe('fulfilled');
    if (started.status === 'fulfilled') {
      expect(cancelled).toMatchObject({ value: 'ALREADY_STARTED' }); expect(fake.get('stock:p1')).toMatchObject({ balance: 8, reserved: 0 });
    } else {
      expect(cancelled).toMatchObject({ value: 'CANCELLED' }); expect(fake.get('stock:p1')).toMatchObject({ balance: 10, reserved: 2 });
    }
  });
  it('libera estoque somente após cancelamento confirmado, sem duplicar', async () => {
    await expect(store.cancel(base, now)).resolves.toBe('CANCELLED');
    const release = { ...base, messageId: 'release', operationId: 'release', reservationId: 'r1', cancelOperationId: 'op1' };
    await expect(store.release(release, now)).resolves.toBe('RELEASED');
    await expect(store.release({ ...release, messageId: 'retry' }, now)).resolves.toBe('RELEASED');
    expect(fake.get('stock:p1')).toMatchObject({ balance: 10, reserved: 0 });
    expect(fake.get('reservation:os1')).toMatchObject({ status: 'RELEASED' });
    expect(fake.list('outbox:')).toHaveLength(2);
    for (const row of fake.list('outbox:')) expect(eventValid(row.payload as Record<string, unknown>)).toBe(true);
  });
  it('não libera se trabalho começou ou cancelamento não foi confirmado', async () => {
    const release = { ...base, messageId: 'release', operationId: 'release', reservationId: 'r1', cancelOperationId: 'cancel' };
    await expect(store.release(release, now)).rejects.toThrow('Cancelamento sem confirmação');
    await store.start(base, now);
    await expect(store.cancel({ ...base, messageId: 'cancel', operationId: 'cancel' }, now)).resolves.toBe('ALREADY_STARTED');
    await expect(store.release(release, now)).rejects.toThrow('Cancelamento sem confirmação');
    expect(fake.get('stock:p1')).toMatchObject({ balance: 8, reserved: 0 });
  });
  it('registra ausência de reserva somente após bloqueio confirmado', async () => {
    const fresh = new FakeCosmos(); const worker = new CosmosWorkStore(fresh as unknown as Container);
    await worker.cancel({ ...base, executionId: undefined }, now);
    await expect(worker.release({ ...base, executionId: undefined, messageId: 'release', operationId: 'release', reservationId: 'r1', cancelOperationId: 'op1' }, now)).resolves.toBe('NO_RESERVATION');
  });
  it('deduplica operação, grava inbox e recusa reutilização', async () => {
    await store.start(base, now);
    await expect(store.start({ ...base, messageId: 'm2' }, now)).resolves.toBe('STARTED');
    expect(fake.get('inbox:start:m2')).toBeDefined();
    await expect(store.start({ ...base, messageId: 'm3', executionId: 'other' }, now)).rejects.toThrow('outro conteúdo');
    await expect(store.start({ ...base, operationId: 'other' }, now)).rejects.toThrow('Mensagem reutilizada');
    expect(fake.list('outbox:')).toHaveLength(1);
  });
  it('recusa conclusão prematura, dados inválidos e erro de batch sem efeitos', async () => {
    await expect(store.complete(base, now)).rejects.toThrow('não pode ser concluído');
    await expect(store.start({ ...base, osId: '../' }, now)).rejects.toThrow('Comando inválido');
    fake.batchCode = 503; await expect(store.start(base, now)).rejects.toThrow('Falha no batch');
    expect(fake.get('stock:p1')).toMatchObject({ balance: 10, reserved: 2 }); expect(fake.list('outbox:')).toHaveLength(0);
  });
});
