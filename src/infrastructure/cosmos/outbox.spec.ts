import { Container, OperationInput } from '@azure/cosmos';
import { CosmosOutboxDispatcher, OutboxEvent } from './outbox';

class FakeStore {
  doc: OutboxEvent = { id: 'outbox:m1', filialId: 'f1', messageId: 'm1', status: 'PENDING', payload: { messageId: 'm1', type: 'event' }, createdAt: '2030-01-01' };
  version = 0;
  failNext = false;
  readError?: unknown;
  missingResource = false;
  missingEtag = false;
  batchError?: unknown;
  invalidBatch = false;
  item() { return { read: async () => {
    if (this.readError) throw this.readError;
    return { resource: this.missingResource ? undefined : { ...this.doc }, etag: this.missingEtag ? undefined : String(this.version) };
  } }; }
  items = {
    query: () => ({ fetchAll: async () => ({ resources: [{ id: this.doc.id, filialId: this.doc.filialId }] }) }),
    batch: async (operations: OperationInput[], partition: string) => {
      if (this.batchError) throw this.batchError;
      if (this.invalidBatch) return { code: 503 };
      if (this.failNext) { this.failNext = false; return { code: 412 }; }
      const op = operations[0];
      if (op.operationType !== 'Replace' || op.ifMatch !== String(this.version) || partition !== this.doc.filialId) return { code: 412 };
      this.doc = { ...op.resourceBody } as OutboxEvent; this.version++;
      return { code: 200, result: [{ statusCode: 200 }] };
    },
  };
}

describe('CosmosOutboxDispatcher', () => {
  let fake: FakeStore;
  let now: number;
  const publisher = { publish: jest.fn() };
  const dispatcher = () => new CosmosOutboxDispatcher(fake as unknown as Container, publisher, () => now, 1000, 2);
  beforeEach(() => { fake = new FakeStore(); now = 10000; jest.resetAllMocks(); });
  it('reivindica e publica uma vez, mantendo o identificador', async () => {
    await expect(dispatcher().dispatchOne()).resolves.toBe('SENT');
    expect(fake.doc.status).toBe('SENT');
    expect(publisher.publish).toHaveBeenCalledWith({ messageId: 'm1', payload: fake.doc.payload });
    await expect(dispatcher().dispatchOne()).resolves.toBe('EMPTY');
  });
  it('reagenda erro transitório e marca falha após o limite', async () => {
    publisher.publish.mockRejectedValue(new Error('offline'));
    await expect(dispatcher().dispatchOne()).resolves.toBe('RETRY');
    await expect(dispatcher().dispatchOne()).resolves.toBe('EMPTY');
    now += 1000;
    await expect(dispatcher().dispatchOne()).resolves.toBe('FAILED');
    expect(fake.doc.attempts).toBe(2);
  });
  it('recupera lease expirado com mesmo ID e ignora lease vigente', async () => {
    fake.doc = { ...fake.doc, status: 'PROCESSING', claimToken: 'old', leaseUntil: now + 1 };
    await expect(dispatcher().dispatchOne()).resolves.toBe('EMPTY');
    now += 2;
    await expect(dispatcher().dispatchOne()).resolves.toBe('SENT');
    expect(publisher.publish).toHaveBeenCalledWith(expect.objectContaining({ messageId: 'm1' }));
  });
  it('não envia se perder a disputa pela ETag', async () => {
    fake.failNext = true;
    await expect(dispatcher().dispatchOne()).resolves.toBe('EMPTY');
    expect(publisher.publish).not.toHaveBeenCalled();
  });
  it('não sobrescreve claim que outra instância assumiu durante o envio', async () => {
    publisher.publish.mockImplementation(async () => { fake.doc = { ...fake.doc, claimToken: 'new' }; fake.version++; });
    await expect(dispatcher().dispatchOne()).resolves.toBe('CONFLICT');
    expect(fake.doc.claimToken).toBe('new');
  });
  it('ignora documento ausente e exige ETag quando ele existe', async () => {
    fake.readError = { code: 404 };
    await expect(dispatcher().dispatchOne()).resolves.toBe('EMPTY');
    fake.readError = undefined; fake.missingResource = true;
    await expect(dispatcher().dispatchOne()).resolves.toBe('EMPTY');
    fake.missingResource = false; fake.missingEtag = true;
    await expect(dispatcher().dispatchOne()).rejects.toThrow('sem ETag');
    fake.missingEtag = false; fake.readError = new Error('offline');
    await expect(dispatcher().dispatchOne()).rejects.toThrow('offline');
  });
  it('propaga falhas do batch e interpreta conflito retornado pelo SDK', async () => {
    fake.invalidBatch = true;
    await expect(dispatcher().dispatchOne()).rejects.toThrow('Falha ao atualizar');
    fake.invalidBatch = false; fake.batchError = { code: '412' };
    await expect(dispatcher().dispatchOne()).resolves.toBe('EMPTY');
    fake.batchError = new Error('indisponível');
    await expect(dispatcher().dispatchOne()).rejects.toThrow('indisponível');
  });
  it('permite que apenas um worker reivindique o mesmo registro', async () => {
    const results = await Promise.all([dispatcher().dispatchOne(), dispatcher().dispatchOne()]);
    expect(results.filter((result) => result === 'SENT')).toHaveLength(1);
    expect(publisher.publish).toHaveBeenCalledTimes(1);
  });
});
