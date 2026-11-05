import { BadRequestException } from '@nestjs/common';
import { CatalogController, RepairController, StockController, WorkActions } from './reparo-controller';

describe('ações de reparo', () => {
  const work = { start: jest.fn(), complete: jest.fn(), listQueue: jest.fn() };
  const controller = new RepairController(work as unknown as WorkActions);
  const body = { sagaId: 's1', filialId: 'f1', executionId: 'e1' };
  beforeEach(() => jest.resetAllMocks());
  it('propaga identidade da ação e chave idempotente', async () => {
    work.start.mockResolvedValue('STARTED'); work.complete.mockResolvedValue('COMPLETED');
    await expect(controller.start('os1', 'op1', body)).resolves.toEqual({ status: 'STARTED' });
    expect(work.start).toHaveBeenCalledWith({ osId: 'os1', sagaId: 's1', filialId: 'f1', executionId: 'e1', operationId: 'op1', messageId: 'op1', correlationId: 'op1' });
    await expect(controller.complete('os1', 'op2', body)).resolves.toEqual({ status: 'COMPLETED' });
  });
  it('rejeita chave ausente, IDs inválidos e campos extras', async () => {
    await expect(controller.start('os1', undefined, body)).rejects.toThrow(BadRequestException);
    await expect(controller.complete('../', 'op2', body)).rejects.toThrow(BadRequestException);
    const extra = { ...body, status: 'STARTED' };
    await expect(controller.start('os1', 'op1', extra)).rejects.toThrow(BadRequestException);
    expect(work.start).not.toHaveBeenCalled();
  });
  it('valida filial e paginação da fila', async () => {
    work.listQueue.mockResolvedValue({ items: [], continuationToken: null });
    await expect(controller.listQueue('f1', '10', 'token')).resolves.toEqual({ items: [], continuationToken: null });
    expect(work.listQueue).toHaveBeenCalledWith({ filialId: 'f1', limit: 10, continuationToken: 'token' });
    await expect(controller.listQueue('../', '10')).rejects.toThrow(BadRequestException);
    await expect(controller.listQueue('f1', '101')).rejects.toThrow(BadRequestException);
  });
});

describe('consulta e ajuste de estoque', () => {
  const work = { getStock: jest.fn(), setStock: jest.fn() }; const controller = new StockController(work as unknown as WorkActions);
  beforeEach(() => jest.resetAllMocks());
  it('passa o ator autenticado e a chave de idempotência', async () => {
    work.setStock.mockResolvedValue({ balance: 5, reserved: 2, available: 3 });
    await expect(controller.set('f1', 'p1', 'op1', { adminId: 'admin' }, { balance: 5, reason: 'COUNT' })).resolves.toMatchObject({ available: 3 });
    expect(work.setStock).toHaveBeenCalledWith({ filialId: 'f1', sku: 'p1', operationId: 'op1', actorId: 'admin', balance: 5, reason: 'COUNT' });
    await expect(controller.set('f1', 'p1', undefined, { adminId: 'admin' }, { balance: 5, reason: 'COUNT' })).rejects.toThrow(BadRequestException);
  });
  it('traduz conflito e ausência de peça', async () => {
    work.setStock.mockRejectedValueOnce(new Error('Saldo menor que o reservado')).mockRejectedValueOnce(new Error('Peça não encontrada'));
    await expect(controller.set('f1', 'p1', 'op1', { adminId: 'admin' }, { balance: 0, reason: 'COUNT' })).rejects.toMatchObject({ status: 409 });
    await expect(controller.set('f1', 'p1', 'op2', { adminId: 'admin' }, { balance: 0, reason: 'COUNT' })).rejects.toMatchObject({ status: 404 });
  });
});

describe('catálogo e ações de diagnóstico', () => {
  const work = { diagnosisAction: jest.fn(), putCatalog: jest.fn(), getCatalog: jest.fn() };
  const repair = new RepairController(work as unknown as WorkActions);
  const catalog = new CatalogController(work as unknown as WorkActions);
  const body = { sagaId: 's1', filialId: 'f1', correlationId: 'c1' };
  beforeEach(() => jest.resetAllMocks());
  it('valida e encaminha as ações de diagnóstico', async () => {
    await expect(repair.startDiagnosis('os1', 'op1', body)).resolves.toEqual({ status: 'IN_PROGRESS' });
    const items = [{ sourceId: 'p1', itemType: 'PART' as const, quantity: 2 }];
    await expect(repair.completeDiagnosis('os1', 'op2', { ...body, items })).resolves.toEqual({ status: 'COMPLETED' });
    expect(work.diagnosisAction).toHaveBeenCalledWith('complete', expect.objectContaining({ osId: 'os1', sagaId: 's1', operationId: 'op2' }), items);
    await expect(repair.completeDiagnosis('os1', 'op2', null as never)).rejects.toThrow(BadRequestException);
  });
  it('valida dados do catálogo e retorna o item salvo', async () => {
    const value = { name: 'Peça', unitPriceInCents: 500, active: true };
    await expect(catalog.put('f1', 'PART', 'p1', value)).resolves.toEqual({ status: 'SAVED' });
    expect(work.putCatalog).toHaveBeenCalledWith('f1', { sourceId: 'p1', itemType: 'PART', ...value });
    work.getCatalog.mockResolvedValue({ sourceId: 'p1', itemType: 'PART', ...value, version: 1 });
    await expect(catalog.get('f1', 'PART', 'p1')).resolves.toMatchObject({ unitPriceInCents: 500 });
    work.getCatalog.mockResolvedValue(null);
    await expect(catalog.get('f1', 'PART', 'missing')).rejects.toThrow();
    await expect(catalog.get('../', 'PART', 'p1')).rejects.toThrow(BadRequestException);
    await expect(catalog.put('f1', 'OTHER', 'p1', value)).rejects.toThrow(BadRequestException);
    await expect(catalog.put('f1', 'PART', 'p1', { ...value, unitPriceInCents: -1 })).rejects.toThrow(BadRequestException);
  });
});
