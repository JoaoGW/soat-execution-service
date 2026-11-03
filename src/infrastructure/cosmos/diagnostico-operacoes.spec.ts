import { Container, OperationInput } from '@azure/cosmos';
import { CosmosDiagnosisActions } from './diagnostico-operacoes';
import Ajv from 'ajv';
import addFormats from 'ajv-formats';
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
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


describe('diagnóstico com snapshot', () => {
  let fake: FakeCosmos; let store: CosmosDiagnosisActions;
  const input = { osId: 'os1', sagaId: 's1', filialId: 'f1', operationId: 'start', correlationId: 'c1' };
  beforeEach(() => {
    fake = new FakeCosmos();
    fake.seed({ id: 'gate:s1', filialId: 'f1', cancelled: false });
    fake.seed({ id: 'diagnosis:os1', filialId: 'f1', osId: 'os1', sagaId: 's1', status: 'PENDING', version: 1 });
    store = new CosmosDiagnosisActions(fake as unknown as Container);
  });
  it('fixa preço de catálogo e publica eventos transacionais, mesmo após alteração de preço', async () => {
    await store.putCatalog('f1', { sourceId: 'p1', itemType: 'PART', name: 'Peça', unitPriceInCents: 500, active: true });
    await store.apply('start', input);
    const done = { ...input, operationId: 'done', items: [{ sourceId: 'p1', itemType: 'PART' as const, quantity: 2 }] };
    await store.apply('complete', done); await store.apply('complete', done);
    await store.putCatalog('f1', { sourceId: 'p1', itemType: 'PART', name: 'Peça', unitPriceInCents: 900, active: true });
    expect(fake.get('diagnosis:os1')).toMatchObject({ status: 'COMPLETED', version: 3, items: [{ sourceId: 'p1', itemType: 'PART', quantity: 2, unitPriceInCents: 500 }] });
    expect(fake.list('outbox:')).toHaveLength(2);
    const directory = resolve(process.cwd(), 'docs/contracts/messages'); const ajv = new Ajv(); addFormats(ajv);
    ajv.addSchema(JSON.parse(readFileSync(resolve(directory, 'envelope.schema.json'), 'utf8')));
    for (const row of fake.list('outbox:')) {
      const event = row.payload as { type: string };
      expect(ajv.validate(JSON.parse(readFileSync(resolve(directory, `${event.type}.schema.json`), 'utf8')), event)).toBe(true);
    }
    await expect(store.apply('complete', { ...done, items: [{ ...done.items[0], quantity: 3 }] })).rejects.toThrow('Operação reutilizada');
  });
  it('bloqueia conclusão sem item ativo e após cancelamento', async () => {
    await store.apply('start', input);
    await expect(store.apply('complete', { ...input, operationId: 'done', items: [{ sourceId: 'p1', itemType: 'PART', quantity: 1 }] })).rejects.toThrow('indisponível');
    fake.seed({ id: 'gate:s1', filialId: 'f1', cancelled: true });
    await expect(store.apply('complete', { ...input, operationId: 'done', items: [{ sourceId: 'p1', itemType: 'PART', quantity: 1 }] })).rejects.toThrow('não pode avançar');
  });
  it('uma alteração concorrente de preço não gera snapshot parcial', async () => {
    await store.putCatalog('f1', { sourceId: 'p1', itemType: 'PART', name: 'Peça', unitPriceInCents: 500, active: true });
    await store.apply('start', input);
    const done = { ...input, operationId: 'done', items: [{ sourceId: 'p1', itemType: 'PART' as const, quantity: 1 }] };
    await Promise.all([store.apply('complete', done), store.putCatalog('f1', { sourceId: 'p1', itemType: 'PART', name: 'Peça', unitPriceInCents: 900, active: true })]);
    const result = fake.get('diagnosis:os1');
    expect([500, 900]).toContain((result?.items as Array<{ unitPriceInCents: number }>)[0].unitPriceInCents);
    expect(fake.list('outbox:')).toHaveLength(2);
  });
});
