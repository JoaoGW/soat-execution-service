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

class FakeOutbox {
  readonly docs = new Map<string, { doc: OutboxEvent; version: number }>();
  readonly takes: number[] = [];
  add(index: number, extra: Partial<OutboxEvent> = {}) {
    const messageId = `m${index}`;
    this.docs.set(`outbox:${messageId}`, { doc: { id: `outbox:${messageId}`, filialId: 'f1', messageId, status: 'PENDING', payload: { messageId }, createdAt: `2030-01-01T00:00:${String(index).padStart(2, '0')}Z`, ...extra }, version: 0 });
  }
  item(id: string) { return { read: async () => { const entry = this.docs.get(id); return entry ? { resource: { ...entry.doc }, etag: String(entry.version) } : { resource: undefined }; } }; }
  items = {
    query: (spec: { parameters: Array<{ name: string; value: number }> }) => ({ fetchAll: async () => {
      const take = spec.parameters.find((parameter) => parameter.name === '@take')!.value; const now = spec.parameters.find((parameter) => parameter.name === '@now')!.value;
      this.takes.push(take);
      const eligible = [...this.docs.values()].map((entry) => entry.doc).filter((doc) => (doc.status === 'PENDING' && (doc.nextAttemptAt ?? 0) <= now) || (doc.status === 'PROCESSING' && (doc.leaseUntil ?? Infinity) <= now));
      return { resources: eligible.sort((a, b) => a.createdAt.localeCompare(b.createdAt)).slice(0, take).map((doc) => ({ id: doc.id, filialId: doc.filialId })) };
    } }),
    batch: async (operations: OperationInput[]) => {
      const op = operations[0]; if (op.operationType !== 'Replace') return { code: 400 };
      const entry = this.docs.get(op.id); if (!entry || op.ifMatch !== String(entry.version)) return { code: 412 };
      this.docs.set(op.id, { doc: { ...op.resourceBody } as OutboxEvent, version: entry.version + 1 });
      return { code: 200, result: [{ statusCode: 200 }] };
    },
  };
}

describe('CosmosOutboxDispatcher em lote', () => {
  let fake: FakeOutbox; const now = 10000;
  const publisher = { publish: jest.fn() };
  const dispatcher = () => new CosmosOutboxDispatcher(fake as unknown as Container, publisher, () => now, 1000, 2);
  beforeEach(() => { fake = new FakeOutbox(); jest.resetAllMocks(); });
  it('drena em ordem de criação até o limite e sinaliza lote completo', async () => {
    for (const index of [3, 1, 2, 5, 4]) fake.add(index);
    await expect(dispatcher().dispatchBatch(3)).resolves.toEqual({ sent: 3, full: true });
    expect(publisher.publish.mock.calls.map(([event]) => event.messageId)).toEqual(['m1', 'm2', 'm3']);
    await expect(dispatcher().dispatchBatch(3)).resolves.toEqual({ sent: 2, full: false });
    expect(publisher.publish.mock.calls.map(([event]) => event.messageId)).toEqual(['m1', 'm2', 'm3', 'm4', 'm5']);
    await expect(dispatcher().dispatchBatch()).resolves.toEqual({ sent: 0, full: false });
    expect(fake.takes).toEqual([3, 3, 20]);
    expect([...fake.docs.values()].every((entry) => entry.doc.status === 'SENT')).toBe(true);
  });
  it('interrompe o lote na primeira falha de publicação, reagendando sem adiantar os seguintes', async () => {
    for (const index of [1, 2, 3]) fake.add(index);
    publisher.publish.mockResolvedValueOnce(undefined).mockRejectedValueOnce(new Error('offline'));
    await expect(dispatcher().dispatchBatch(3)).resolves.toEqual({ sent: 1, full: false });
    expect(publisher.publish).toHaveBeenCalledTimes(2);
    expect(fake.docs.get('outbox:m2')?.doc).toMatchObject({ status: 'PENDING', attempts: 1, nextAttemptAt: now + 1000 });
    expect(fake.docs.get('outbox:m3')?.doc).toMatchObject({ status: 'PENDING' }); expect(fake.docs.get('outbox:m3')?.doc.attempts).toBeUndefined();
  });
  it('marca FAILED após o limite de tentativas e também encerra o lote', async () => {
    fake.add(1, { attempts: 1 }); fake.add(2);
    publisher.publish.mockRejectedValue(new Error('offline'));
    await expect(dispatcher().dispatchBatch(5)).resolves.toEqual({ sent: 0, full: false });
    expect(fake.docs.get('outbox:m1')?.doc.status).toBe('FAILED'); expect(publisher.publish).toHaveBeenCalledTimes(1);
  });
  it('ignora candidato com lease vigente ou removido e continua o lote', async () => {
    fake.add(1); fake.add(2); fake.add(3);
    const original = fake.item.bind(fake);
    jest.spyOn(fake, 'item').mockImplementation((id: string) => id === 'outbox:m1' ? { read: async () => ({ resource: { ...fake.docs.get(id)!.doc, status: 'PROCESSING' as const, leaseUntil: now + 1 }, etag: '0' }) } : id === 'outbox:m2' ? { read: async () => { throw { code: 404 }; } } as never : original(id));
    await expect(dispatcher().dispatchBatch(3)).resolves.toEqual({ sent: 1, full: false });
    expect(publisher.publish).toHaveBeenCalledWith({ messageId: 'm3', payload: { messageId: 'm3' } });
  });
  it('recusa tamanho de lote inválido', async () => {
    for (const limit of [0, 101, 1.5]) await expect(dispatcher().dispatchBatch(limit)).rejects.toThrow('Tamanho de lote inválido');
  });
});
