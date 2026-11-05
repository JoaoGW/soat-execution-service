const assert = require('node:assert/strict');
const { randomUUID } = require('node:crypto');
const { CosmosClient } = require('@azure/cosmos');
const { CosmosQueueQuery } = require('../../dist/infrastructure/cosmos/fila');
const { BackfillQueueKeys } = require('../../dist/infrastructure/cosmos/backfill-fila');
const { CosmosStockActions } = require('../../dist/infrastructure/cosmos/estoque');
const { CosmosDiagnosisActions } = require('../../dist/infrastructure/cosmos/diagnostico-operacoes');
const endpoint = process.env.COSMOS_TEST_ENDPOINT;
if (!['http://localhost:8081', 'http://127.0.0.1:8081'].includes(endpoint)) throw Error('Emulador Cosmos local obrigatório');
const client = new CosmosClient({ endpoint, key: Buffer.from('local-test-key').toString('base64') });
async function main() {
  const database = (await client.databases.createIfNotExists({ id: `queue_${randomUUID().replaceAll('-', '')}` })).database;
  try {
    const container = (await database.containers.createIfNotExists({ id: 'operations', partitionKey: { paths: ['/filialId'] } })).container;
    for (const item of [
      { id: 'task:b', filialId: 'one', osId: 'b', sagaId: 's2', executionId: 'e2', status: 'QUEUED', createdAt: '2030-01-01T00:00:02.000Z', queueKey: '2030-01-01T00:00:02.000Z|b', providerPaymentId: 'private' },
      { id: 'task:a', filialId: 'one', osId: 'a', sagaId: 's1', executionId: 'e1', status: 'QUEUED', createdAt: '2030-01-01T00:00:01.000Z', queueKey: '2030-01-01T00:00:01.000Z|a' },
      { id: 'task:aa', filialId: 'one', osId: 'aa', sagaId: 's6', executionId: 'e6', status: 'QUEUED', createdAt: '2030-01-01T00:00:01.000Z', queueKey: '2030-01-01T00:00:01.000Z|aa' },
      { id: 'task:c', filialId: 'one', osId: 'c', sagaId: 's3', executionId: 'e3', status: 'STARTED', createdAt: '2030-01-01T00:00:00.000Z', queueKey: '2030-01-01T00:00:00.000Z|c' },
      { id: 'task:d', filialId: 'two', osId: 'd', sagaId: 's4', executionId: 'e4', status: 'QUEUED', createdAt: '2030-01-01T00:00:00.000Z', queueKey: '2030-01-01T00:00:00.000Z|d' },
    ]) await container.items.create(item);
    const service = new CosmosQueueQuery(container);
    const page = await service.execute({ filialId: 'one', limit: 10 });
    assert.deepEqual(page.items.map((item) => item.osId), ['a', 'aa', 'b']);
    assert.ok(page.items.every((item) => !('providerPaymentId' in item)));
    assert.deepEqual((await service.execute({ filialId: 'two', limit: 10 })).items.map((item) => item.osId), ['d']);
    const first = await service.execute({ filialId: 'one', limit: 1 });
    assert.deepEqual(first.items.map((item) => item.osId), ['a']);
    assert.ok(first.continuationToken, 'Emulador não forneceu token de continuação');
    const second = await service.execute({ filialId: 'one', limit: 1, continuationToken: first.continuationToken });
    assert.deepEqual(second.items.map((item) => item.osId), ['aa']);
    assert.ok(second.continuationToken);
    const third = await service.execute({ filialId: 'one', limit: 1, continuationToken: second.continuationToken });
    assert.deepEqual(third.items.map((item) => item.osId), ['b']);
    await container.items.create({ id: 'task:legacy', filialId: 'one', osId: 'legacy', sagaId: 's5', executionId: 'e5', status: 'QUEUED', createdAt: '2030-01-01T00:00:03.000Z' });
    const backfill = new BackfillQueueKeys(container);
    assert.deepEqual(await backfill.execute('one', false), { found: 1, updated: 0 });
    assert.deepEqual(await backfill.execute('one', true), { found: 1, updated: 1 });
    assert.deepEqual((await service.execute({ filialId: 'one', limit: 10 })).items.map((item) => item.osId), ['a', 'aa', 'b', 'legacy']);
    await container.items.create({ id: 'catalog:PART:p1', filialId: 'one', sourceId: 'p1', itemType: 'PART', name: 'Peça', unitPriceInCents: 500, active: true, version: 1 });
    const stock = new CosmosStockActions(container);
    const input = { filialId: 'one', sku: 'p1', balance: 10, operationId: 'op1', actorId: 'admin', reason: 'COUNT' };
    assert.equal((await stock.set(input)).balance, 10);
    assert.equal((await stock.set({ ...input, operationId: 'op2', balance: 12 })).balance, 12);
    assert.equal((await stock.set(input)).balance, 10);
    assert.equal((await stock.get('one', 'p1')).balance, 12);
    const item = await container.item('stock:p1', 'one').read();
    await container.item('stock:p1', 'one').replace({ ...item.resource, reserved: 3 });
    await assert.rejects(() => stock.set({ ...input, operationId: 'op3', balance: 2 }), /Saldo menor que o reservado/);
    assert.equal((await stock.get('one', 'p1')).available, 9);
    const catalog = new CosmosDiagnosisActions(container);
    await catalog.putCatalog('one', { sourceId: 'p2', itemType: 'PART', name: 'Outra', unitPriceInCents: 200, active: true });
    await catalog.putCatalog('two', { sourceId: 'p0', itemType: 'PART', name: 'Outra filial', unitPriceInCents: 100, active: true });
    const catalogFirst = await catalog.listCatalog({ filialId: 'one', itemType: 'PART', limit: 1 });
    assert.deepEqual(catalogFirst.items.map(item => item.sourceId), ['p1']); assert.ok(catalogFirst.continuationToken);
    const catalogSecond = await catalog.listCatalog({ filialId: 'one', itemType: 'PART', limit: 1, continuationToken: catalogFirst.continuationToken });
    assert.deepEqual(catalogSecond.items.map(item => item.sourceId), ['p2']);
    assert.equal(await catalog.deactivateCatalog('one', 'PART', 'p1'), true);
    assert.equal((await catalog.getCatalog('one', 'PART', 'p1')).active, false);
    assert.deepEqual((await catalog.listCatalog({ filialId: 'one', itemType: 'PART', limit: 10 })).items.map(item => item.sourceId), ['p2']);
    process.stdout.write('Cosmos emulado: fila, paginação, backfill e ajuste idempotente de estoque verificados.\n');
  } finally { await database.delete(); }
}
main().finally(() => client.dispose()).catch((error) => { console.error(error.stack || error); process.exitCode = 1; });
