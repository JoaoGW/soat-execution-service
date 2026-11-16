import { BadRequestException, ConflictException, NotFoundException, UnprocessableEntityException } from '@nestjs/common';
import { CatalogController, httpError, RepairController, StockController, WorkActions } from './reparo-controller';
import { conflict, invalidInput, notFound, unprocessable } from '../domain/erros';

describe('tradução de erros de negócio', () => {
  it('mapeia tipos conhecidos e preserva erros inesperados', () => {
    expect(httpError(invalidInput('Comando inválido'))).toBeInstanceOf(BadRequestException);
    expect(httpError(notFound('Peça não encontrada'))).toBeInstanceOf(NotFoundException);
    expect(httpError(conflict('Trabalho não pode ser iniciado'))).toMatchObject({ status: 409, message: 'Trabalho não pode ser iniciado' });
    expect(httpError(unprocessable('Item de catálogo indisponível'))).toBeInstanceOf(UnprocessableEntityException);
    const unexpected = new Error('Falha no batch Cosmos');
    expect(httpError(unexpected)).toBe(unexpected);
  });
});

describe('ações de reparo', () => {
  const work = { start: jest.fn(), complete: jest.fn(), fail: jest.fn(), listQueue: jest.fn() };
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
  it('registra falha do reparo com motivo e mesma chave idempotente', async () => {
    work.fail.mockResolvedValue('FAILED');
    await expect(controller.fail('os1', 'op3', { ...body, reason: 'REPARO_INVIAVEL' })).resolves.toEqual({ status: 'FAILED' });
    expect(work.fail).toHaveBeenCalledWith({ osId: 'os1', sagaId: 's1', filialId: 'f1', executionId: 'e1', operationId: 'op3', messageId: 'op3', correlationId: 'op3', reason: 'REPARO_INVIAVEL' });
  });
  it('rejeita falha sem motivo válido, sem chave ou com corpo inválido', async () => {
    for (const reason of [undefined, '', 'motivo com espaço', 'x'.repeat(129), 7]) await expect(controller.fail('os1', 'op3', { ...body, reason } as never)).rejects.toThrow(BadRequestException);
    await expect(controller.fail('os1', undefined, { ...body, reason: 'REPARO_INVIAVEL' })).rejects.toThrow(BadRequestException);
    await expect(controller.fail('os1', 'op3', null as never)).rejects.toThrow('Corpo inválido');
    await expect(controller.fail('os1', 'op3', [] as never)).rejects.toThrow('Corpo inválido');
    await expect(controller.fail('os1', 'op3', { ...body, reason: 'REPARO_INVIAVEL', extra: 1 } as never)).rejects.toThrow(BadRequestException);
    expect(work.fail).not.toHaveBeenCalled();
  });
  it('traduz transição inválida em 409 e mantém 500 para erro inesperado', async () => {
    work.start.mockRejectedValueOnce(conflict('Trabalho não pode ser iniciado'));
    await expect(controller.start('os1', 'op1', body)).rejects.toBeInstanceOf(ConflictException);
    work.complete.mockRejectedValueOnce(conflict('Execução não corresponde ao comando'));
    await expect(controller.complete('os1', 'op2', body)).rejects.toMatchObject({ status: 409 });
    work.fail.mockRejectedValueOnce(conflict('Trabalho não pode ser marcado como falho'));
    await expect(controller.fail('os1', 'op3', { ...body, reason: 'REPARO_INVIAVEL' })).rejects.toMatchObject({ status: 409 });
    work.start.mockRejectedValueOnce(invalidInput('Comando inválido'));
    await expect(controller.start('os1', 'op1', body)).rejects.toMatchObject({ status: 400 });
    const unexpected = new Error('Falha no batch Cosmos'); work.complete.mockRejectedValueOnce(unexpected);
    await expect(controller.complete('os1', 'op2', body)).rejects.toBe(unexpected);
  });
  it('valida filial e paginação da fila', async () => {
    work.listQueue.mockResolvedValue({ items: [], continuationToken: null });
    await expect(controller.listQueue('f1', '10', 'token')).resolves.toEqual({ items: [], continuationToken: null });
    expect(work.listQueue).toHaveBeenCalledWith({ filialId: 'f1', limit: 10, continuationToken: 'token' });
    await expect(controller.listQueue('../', '10')).rejects.toThrow(BadRequestException);
    await expect(controller.listQueue('f1', '101')).rejects.toThrow(BadRequestException);
    work.listQueue.mockRejectedValueOnce(invalidInput('Consulta inválida'));
    await expect(controller.listQueue('f1', '10', 'outro')).rejects.toThrow(BadRequestException);
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
    work.setStock.mockRejectedValueOnce(conflict('Saldo menor que o reservado')).mockRejectedValueOnce(notFound('Peça não encontrada'));
    await expect(controller.set('f1', 'p1', 'op1', { adminId: 'admin' }, { balance: 0, reason: 'COUNT' })).rejects.toMatchObject({ status: 409 });
    await expect(controller.set('f1', 'p1', 'op2', { adminId: 'admin' }, { balance: 0, reason: 'COUNT' })).rejects.toMatchObject({ status: 404 });
    work.getStock.mockResolvedValueOnce(null);
    await expect(controller.get('f1', 'p1')).rejects.toThrow(NotFoundException);
  });
});

describe('catálogo e ações de diagnóstico', () => {
  const work = { diagnosisAction: jest.fn(), putCatalog: jest.fn(), getCatalog: jest.fn(), listCatalog: jest.fn(), deactivateCatalog: jest.fn() };
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
    work.diagnosisAction.mockRejectedValueOnce(conflict('Diagnóstico não pode avançar')).mockRejectedValueOnce(unprocessable('Item de catálogo indisponível'));
    await expect(repair.startDiagnosis('os1', 'op1', body)).rejects.toMatchObject({ status: 409 });
    await expect(repair.completeDiagnosis('os1', 'op2', { ...body, items })).rejects.toMatchObject({ status: 422 });
  });
  it('valida dados do catálogo e retorna o item salvo', async () => {
    const value = { name: 'Peça', unitPriceInCents: 500, active: true };
    await expect(catalog.put('f1', 'PART', 'p1', 'op1', value)).resolves.toEqual({ status: 'SAVED' });
    expect(work.putCatalog).toHaveBeenCalledWith('f1', { sourceId: 'p1', itemType: 'PART', ...value }, 'op1');
    work.getCatalog.mockResolvedValue({ sourceId: 'p1', itemType: 'PART', ...value, version: 1 });
    await expect(catalog.get('f1', 'PART', 'p1')).resolves.toMatchObject({ unitPriceInCents: 500 });
    work.getCatalog.mockResolvedValue(null);
    await expect(catalog.get('f1', 'PART', 'missing')).rejects.toThrow();
    await expect(catalog.get('../', 'PART', 'p1')).rejects.toThrow(BadRequestException);
    await expect(catalog.put('f1', 'OTHER', 'p1', 'op1', value)).rejects.toThrow(BadRequestException);
    await expect(catalog.put('f1', 'PART', 'p1', 'op1', { ...value, unitPriceInCents: -1 })).rejects.toThrow(BadRequestException);
  });
  it('exige Idempotency-Key no catálogo e traduz chave conflitante', async () => {
    const value = { name: 'Peça', unitPriceInCents: 500, active: true };
    for (const key of [undefined, '', '../']) {
      await expect(catalog.put('f1', 'PART', 'p1', key, value)).rejects.toThrow('Idempotency-Key inválida');
      await expect(catalog.remove('f1', 'PART', 'p1', key)).rejects.toThrow('Idempotency-Key inválida');
    }
    expect(work.putCatalog).not.toHaveBeenCalled(); expect(work.deactivateCatalog).not.toHaveBeenCalled();
    work.putCatalog.mockRejectedValueOnce(conflict('Chave de idempotência conflitante'));
    await expect(catalog.put('f1', 'PART', 'p1', 'op1', value)).rejects.toMatchObject({ status: 409 });
    work.deactivateCatalog.mockResolvedValueOnce(false);
    await expect(catalog.remove('f1', 'PART', 'missing', 'op2')).rejects.toThrow(NotFoundException);
    await expect(catalog.remove('f1', 'OTHER', 'p1', 'op2')).rejects.toThrow('Item inválido');
  });
  it('lista itens ativos e desativa logicamente', async () => {
    work.listCatalog.mockResolvedValue({ items: [], continuationToken: null }); work.deactivateCatalog.mockResolvedValue(true);
    await expect(catalog.list('f1', 'PART', '10')).resolves.toEqual({ items: [], continuationToken: null });
    expect(work.listCatalog).toHaveBeenCalledWith({ filialId: 'f1', itemType: 'PART', limit: 10 });
    await expect(catalog.remove('f1', 'PART', 'p1', 'op1')).resolves.toEqual({ status: 'INACTIVE' });
    expect(work.deactivateCatalog).toHaveBeenCalledWith('f1', 'PART', 'p1', 'op1');
    await expect(catalog.list('f1', 'OTHER')).rejects.toThrow(BadRequestException);
  });
});
