import { Container, OperationInput } from '@azure/cosmos';
import { CosmosDiagnosisStore, RequestDiagnosis } from './diagnostico';
import { CosmosWorkStore } from './reparo';
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


describe('solicitação de diagnóstico', () => {
  const input: RequestDiagnosis = { messageId: 'm1', operationId: 'op1', osId: 'os1', sagaId: 's1', filialId: 'f1', correlationId: 'c1', veiculoId: 'v1', solicitacoes: ['ruído'] };
  let fake: FakeCosmos; let store: CosmosDiagnosisStore;
  beforeEach(() => { fake = new FakeCosmos(); store = new CosmosDiagnosisStore(fake as unknown as Container); });
  it('registra diagnóstico e deduplica entrega e operação', async () => {
    await store.request(input); await store.request(input); await store.request({ ...input, messageId: 'm2' });
    expect(fake.get('diagnosis:os1')).toMatchObject({ status: 'PENDING', veiculoId: 'v1', solicitacoes: ['ruído'] });
    expect(fake.list('inbox:diagnosis:')).toHaveLength(2);
    await expect(store.request({ ...input, veiculoId: 'outro' })).rejects.toThrow('Mensagem reutilizada');
    await expect(store.request({ ...input, messageId: 'm3', veiculoId: 'outro' })).rejects.toThrow('Operação reutilizada');
  });
  it('não aceita solicitação após cancelamento e disputa gate atomicamente', async () => {
    const work = new CosmosWorkStore(fake as unknown as Container);
    await work.cancel({ messageId: 'cancel', operationId: 'cancel', osId: 'os1', sagaId: 's1', filialId: 'f1', correlationId: 'c1', reason: 'CANCEL' });
    await expect(store.request(input)).rejects.toThrow('Saga cancelada');
    expect(fake.get('diagnosis:os1')).toBeUndefined();
  });
  it('retenta conflito e rejeita falha de banco sem registrar sucesso', async () => {
    fake.conflicts = 1; await store.request(input); expect(fake.get('diagnosis:os1')).toBeDefined();
    const other = new FakeCosmos(); other.batchCode = 503;
    await expect(new CosmosDiagnosisStore(other as unknown as Container).request(input)).rejects.toThrow('Falha no batch');
    expect(other.get('diagnosis:os1')).toBeUndefined();
  });
});
