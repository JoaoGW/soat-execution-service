import { ServiceUnavailableException } from '@nestjs/common';
import { cosmosConfigured, createCosmosClient } from '../infrastructure/azure-clients';
import { CosmosWorkStore } from '../infrastructure/cosmos/reparo';
import { CosmosDiagnosisActions } from '../infrastructure/cosmos/diagnostico-operacoes';
import { CosmosQueueQuery } from '../infrastructure/cosmos/fila';
import { CosmosStockActions } from '../infrastructure/cosmos/estoque';
import { WorkActions } from './reparo-controller';

jest.mock('../infrastructure/azure-clients', () => ({ cosmosConfigured: jest.fn(), createCosmosClient: jest.fn() }));
jest.mock('../infrastructure/cosmos/reparo', () => ({ CosmosWorkStore: jest.fn() }));
jest.mock('../infrastructure/cosmos/diagnostico-operacoes', () => ({ CosmosDiagnosisActions: jest.fn() }));
jest.mock('../infrastructure/cosmos/fila', () => ({ CosmosQueueQuery: jest.fn() }));
jest.mock('../infrastructure/cosmos/estoque', () => ({ CosmosStockActions: jest.fn() }));

describe('WorkActions', () => {
  const original = process.env;
  const client = { database: jest.fn(() => ({ container: jest.fn(() => ({})) })), dispose: jest.fn() };
  const store = { start: jest.fn(async () => 'STARTED'), complete: jest.fn(async () => 'COMPLETED'), fail: jest.fn(async () => 'FAILED') };
  const diagnosis = { apply: jest.fn(), putCatalog: jest.fn(), getCatalog: jest.fn(), listCatalog: jest.fn(), deactivateCatalog: jest.fn(async () => true) };
  const queue = { execute: jest.fn() }; const stock = { get: jest.fn(), set: jest.fn() };
  const input = { messageId: 'op1', operationId: 'op1', osId: 'os1', sagaId: 's1', filialId: 'f1', correlationId: 'c1', executionId: 'e1' };
  beforeEach(() => {
    jest.clearAllMocks(); process.env = { ...original, COSMOS_ENDPOINT: 'https://example.documents.azure.com/', COSMOS_DATABASE: 'db', COSMOS_CONTAINER: 'operations' };
    jest.mocked(cosmosConfigured).mockReturnValue(true); jest.mocked(createCosmosClient).mockReturnValue(client as never);
    jest.mocked(CosmosWorkStore).mockImplementation(() => store as never); jest.mocked(CosmosDiagnosisActions).mockImplementation(() => diagnosis as never);
    jest.mocked(CosmosQueueQuery).mockImplementation(() => queue as never); jest.mocked(CosmosStockActions).mockImplementation(() => stock as never);
  });
  afterEach(() => { process.env = original; });
  it('responde 503 sem Cosmos configurado', async () => {
    jest.mocked(cosmosConfigured).mockReturnValue(false);
    await expect(new WorkActions().fail({ ...input, reason: 'REPARO_INVIAVEL' })).rejects.toBeInstanceOf(ServiceUnavailableException);
  });
  it('cria o cliente uma vez e delega cada ação, incluindo falha e chaves do catálogo', async () => {
    const work = new WorkActions();
    await expect(work.start(input)).resolves.toBe('STARTED');
    await expect(work.complete(input)).resolves.toBe('COMPLETED');
    await expect(work.fail({ ...input, reason: 'REPARO_INVIAVEL' })).resolves.toBe('FAILED');
    expect(store.fail).toHaveBeenCalledWith({ ...input, reason: 'REPARO_INVIAVEL' });
    await work.diagnosisAction('complete', input, [{ sourceId: 'p1', itemType: 'PART', quantity: 1 }]);
    await work.diagnosisAction('start', input);
    expect(diagnosis.apply).toHaveBeenLastCalledWith('start', { osId: 'os1', sagaId: 's1', filialId: 'f1', operationId: 'op1', correlationId: 'c1' });
    const item = { sourceId: 'p1', itemType: 'PART' as const, name: 'Peça', unitPriceInCents: 1, active: true };
    await work.putCatalog('f1', item, 'put1'); expect(diagnosis.putCatalog).toHaveBeenCalledWith('f1', item, 'put1');
    await expect(work.deactivateCatalog('f1', 'PART', 'p1', 'del1')).resolves.toBe(true); expect(diagnosis.deactivateCatalog).toHaveBeenCalledWith('f1', 'PART', 'p1', 'del1');
    await work.getCatalog('f1', 'PART', 'p1'); await work.listCatalog({ filialId: 'f1', itemType: 'PART', limit: 1 });
    await work.listQueue({ filialId: 'f1', limit: 1 }); await work.getStock('f1', 'p1');
    await work.setStock({ filialId: 'f1', sku: 'p1', balance: 1, operationId: 'op1', actorId: 'admin', reason: 'COUNT' });
    expect(queue.execute).toHaveBeenCalled(); expect(stock.get).toHaveBeenCalled(); expect(stock.set).toHaveBeenCalled();
    expect(createCosmosClient).toHaveBeenCalledTimes(1);
    work.onApplicationShutdown(); expect(client.dispose).toHaveBeenCalled();
  });
});
