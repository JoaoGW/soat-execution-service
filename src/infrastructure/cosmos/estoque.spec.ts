import { Container, OperationInput } from '@azure/cosmos';
import { CosmosStockActions } from './estoque';
import { CosmosReservationStore } from './reservas';

class FakeCosmos {
  private docs = new Map<string, { value: Record<string, unknown>; etag: string }>(); private revision = 0; conflicts = 0;
  seed(value: Record<string, unknown> & { id: string; filialId: string }) { this.docs.set(`${value.filialId}|${value.id}`, { value, etag: String(++this.revision) }); }
  item(id: string, partition: string) { return { read: async () => { const item = this.docs.get(`${partition}|${id}`); if (!item) throw { code: 404 }; return { resource: { ...item.value }, etag: item.etag }; } }; }
  items = { batch: async (operations: OperationInput[], partition: string) => {
    if (this.conflicts-- > 0) return { code: 412 };
    for (const operation of operations) {
      if (operation.operationType !== 'Create' && operation.operationType !== 'Replace') throw new Error('Operação inesperada');
      const value = operation.resourceBody as Record<string, unknown>; const old = this.docs.get(`${partition}|${String(value.id)}`);
      if (operation.operationType === 'Create' && old) return { code: 409 };
      if (operation.operationType === 'Replace' && (!old || old.etag !== operation.ifMatch)) return { code: 412 };
    }
    for (const operation of operations) { if (operation.operationType !== 'Create' && operation.operationType !== 'Replace') throw new Error('Operação inesperada'); const value = operation.resourceBody as Record<string, unknown> & { id: string; filialId: string }; this.seed(value); }
    return { code: 200, result: operations.map(() => ({ statusCode: 200 })) };
  } };
}

describe('ajuste administrativo de estoque', () => {
  let fake: FakeCosmos; let service: CosmosStockActions;
  const input = { filialId: 'f1', sku: 'p1', balance: 10, operationId: 'op1', actorId: 'admin', reason: 'INVENTORY' };
  beforeEach(() => { fake = new FakeCosmos(); fake.seed({ id: 'catalog:PART:p1', filialId: 'f1', sourceId: 'p1', itemType: 'PART' }); service = new CosmosStockActions(fake as unknown as Container); });
  it('registra ajuste e retorna o resultado original sem desfazer operação posterior', async () => {
    expect(await service.set(input)).toMatchObject({ balance: 10, reserved: 0, available: 10 });
    expect(await service.set({ ...input, operationId: 'op2', balance: 15 })).toMatchObject({ balance: 15 });
    expect(await service.set(input)).toMatchObject({ balance: 10 });
    expect(await service.get('f1', 'p1')).toMatchObject({ balance: 15 });
    await expect(service.set({ ...input, balance: 9 })).rejects.toThrow('Chave de idempotência conflitante');
  });
  it('não reduz saldo abaixo da reserva e retenta conflitos de ETag', async () => {
    fake.seed({ id: 'stock:p1', filialId: 'f1', sku: 'p1', balance: 10, reserved: 7 }); fake.conflicts = 1;
    await expect(service.set({ ...input, balance: 6 })).rejects.toThrow('Saldo menor que o reservado');
    await expect(service.set({ ...input, balance: 8 })).resolves.toMatchObject({ balance: 8, reserved: 7, available: 1 });
  });
  it('recusa peça ausente e dados inválidos', async () => {
    await expect(service.set({ ...input, sku: 'missing' })).rejects.toThrow('Peça não encontrada');
    await expect(service.set({ ...input, balance: -1 })).rejects.toThrow('Dados inválidos');
    await expect(service.get('f1', 'missing')).resolves.toBeNull();
  });
  it('serializa ajuste concorrente com reserva e preserva o invariante', async () => {
    fake.seed({ id: 'stock:p1', filialId: 'f1', sku: 'p1', balance: 10, reserved: 0 });
    const reservations = new CosmosReservationStore(fake as unknown as Container);
    const command = { messageId: 'message', operationId: 'reserve', osId: 'os', sagaId: 'saga', filialId: 'f1', correlationId: 'corr', quoteId: 'quote', quoteVersion: 1, reservationId: 'res', deadline: '2030-01-01T00:20:00.000Z', items: [{ sku: 'p1', quantity: 7 }] };
    await Promise.allSettled([service.set({ ...input, balance: 6 }), reservations.reserve(command, new Date('2030-01-01T00:00:00.000Z'))]);
    const stock = await service.get('f1', 'p1');
    expect(stock).not.toBeNull(); expect(stock!.balance).toBeGreaterThanOrEqual(stock!.reserved);
    expect([[6, 0], [10, 7]]).toContainEqual([stock!.balance, stock!.reserved]);
  });
});
