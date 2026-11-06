const assert = require('node:assert/strict');
const { randomUUID } = require('node:crypto');
const { readFileSync } = require('node:fs');
const { resolve } = require('node:path');
const Ajv = require('ajv');
const addFormats = require('ajv-formats').default;
const { parse } = require('yaml');
require('reflect-metadata');
const { Test } = require('@nestjs/testing');
const request = require('supertest');
const jwt = require('jsonwebtoken');
const { AdminGuard } = require('../../dist/interfaces/admin-guard');
const { StockController, CatalogController, WorkActions } = require('../../dist/interfaces/reparo-controller');
const { CosmosClient } = require('@azure/cosmos');
const { CosmosQueueQuery } = require('../../dist/infrastructure/cosmos/fila');
const { BackfillQueueKeys } = require('../../dist/infrastructure/cosmos/backfill-fila');
const { CosmosStockActions } = require('../../dist/infrastructure/cosmos/estoque');
const { CosmosDiagnosisActions } = require('../../dist/infrastructure/cosmos/diagnostico-operacoes');
const { RecoverExecutionOutbox } = require('../../dist/infrastructure/cosmos/recover-outbox');
const endpoint = process.env.COSMOS_TEST_ENDPOINT;
if (!['http://localhost:8081', 'http://127.0.0.1:8081'].includes(endpoint)) throw Error('Emulador Cosmos local obrigatório');
const client = new CosmosClient({ endpoint, key: Buffer.from('local-test-key').toString('base64') });
const document = parse(readFileSync(resolve(process.cwd(), 'docs/openapi.yaml'), 'utf8'));
const ajv = new Ajv({ allErrors: true }); ajv.addKeyword('components');
ajv.addSchema({ $id: 'urn:soat-execution:openapi', components: document.components });
const validateStock = ajv.compile({ $ref: 'urn:soat-execution:openapi#/components/schemas/Stock' });
const validateCatalogItem = ajv.compile({ $ref: 'urn:soat-execution:openapi#/components/schemas/CatalogItem' });
const validateCatalogPage = ajv.compile({ $ref: 'urn:soat-execution:openapi#/components/schemas/CatalogPage' });
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
    const stockView = await stock.get('one', 'p1');
    assert.equal(stockView.available, 9);
    assert.ok(validateStock(stockView), JSON.stringify(validateStock.errors));
    const catalog = new CosmosDiagnosisActions(container);
    await catalog.putCatalog('one', { sourceId: 'p2', itemType: 'PART', name: 'Outra', unitPriceInCents: 200, active: true });
    await catalog.putCatalog('two', { sourceId: 'p0', itemType: 'PART', name: 'Outra filial', unitPriceInCents: 100, active: true });
    const catalogFirst = await catalog.listCatalog({ filialId: 'one', itemType: 'PART', limit: 1 });
    assert.ok(validateCatalogPage(catalogFirst), JSON.stringify(validateCatalogPage.errors));
    assert.deepEqual(catalogFirst.items.map(item => item.sourceId), ['p1']); assert.ok(catalogFirst.continuationToken);
    const catalogSecond = await catalog.listCatalog({ filialId: 'one', itemType: 'PART', limit: 1, continuationToken: catalogFirst.continuationToken });
    assert.deepEqual(catalogSecond.items.map(item => item.sourceId), ['p2']);
    assert.equal(await catalog.deactivateCatalog('one', 'PART', 'p1'), true);
    const catalogItem = await catalog.getCatalog('one', 'PART', 'p1');
    assert.equal(catalogItem.active, false);
    assert.ok(validateCatalogItem(catalogItem), JSON.stringify(validateCatalogItem.errors));
    assert.deepEqual((await catalog.listCatalog({ filialId: 'one', itemType: 'PART', limit: 10 })).items.map(item => item.sourceId), ['p2']);
    const contractDirectory = resolve(process.cwd(), 'docs/contracts/messages');
    const messageAjv = addFormats(new Ajv({ allErrors: true }));
    messageAjv.addSchema(JSON.parse(readFileSync(resolve(contractDirectory, 'envelope.schema.json'), 'utf8')));
    const validateEvent = messageAjv.compile(JSON.parse(readFileSync(resolve(contractDirectory, 'execution.work-cancelled.v1.schema.json'), 'utf8')));
    const original = { ...JSON.parse(readFileSync(resolve(contractDirectory, 'examples/execution.work-cancelled.v1.json'), 'utf8')), messageId: 'recovery-msg', filialId: 'one' };
    await container.items.create({ id: 'outbox:recovery-msg', filialId: 'one', messageId: original.messageId, status: 'FAILED', payload: original, createdAt: new Date().toISOString(), attempts: 5, claimToken: '', leaseUntil: 0 });
    const recover = new RecoverExecutionOutbox(container, (type, payload) => type === original.type && validateEvent(payload));
    const recoveryInput = { filialId: 'one', messageId: original.messageId, actor: 'operator', reasonCode: 'BROKER_RESTORED' };
    const outcomes = await Promise.allSettled(Array.from({ length: 3 }, () => recover.execute(recoveryInput)));
    assert.equal(outcomes.filter(outcome => outcome.status === 'fulfilled').length, 1);
    const recovered = (await container.item('outbox:recovery-msg', 'one').read()).resource;
    assert.equal(recovered.status, 'PENDING'); assert.deepEqual(recovered.payload, original);
    const audit = await container.items.query({ query: 'SELECT * FROM c WHERE STARTSWITH(c.id, @prefix) AND c.messageId = @id', parameters: [{ name: '@prefix', value: 'recovery:' }, { name: '@id', value: original.messageId }] }, { partitionKey: 'one' }).fetchAll();
    assert.equal(audit.resources.length, 1); assert.equal(audit.resources[0].actor, 'operator');
    await assert.rejects(() => recover.execute(recoveryInput));
    const invalid = { ...original, messageId: 'invalid-recovery', data: { ...original.data, outcome: 'INVALID' } };
    await container.items.create({ id: 'outbox:invalid-recovery', filialId: 'one', messageId: invalid.messageId, status: 'FAILED', payload: invalid, createdAt: new Date().toISOString() });
    await assert.rejects(() => recover.execute({ ...recoveryInput, messageId: invalid.messageId }), /Contrato/);
    assert.equal((await container.item('outbox:invalid-recovery', 'one').read()).resource.status, 'FAILED');
    const previousSecret = process.env.JWT_SECRET;
    const previousClientSecret = process.env.JWT_CLIENT_SECRET;
    const secret = 'segredo-administrativo-exclusivo-de-teste';
    process.env.JWT_SECRET = secret;
    process.env.JWT_CLIENT_SECRET = 'segredo-cliente-separado-para-o-teste';
    const module = await Test.createTestingModule({ controllers: [StockController, CatalogController], providers: [AdminGuard, { provide: WorkActions, useValue: {
      getStock: stock.get.bind(stock), setStock: stock.set.bind(stock),
      getCatalog: catalog.getCatalog.bind(catalog), listCatalog: catalog.listCatalog.bind(catalog),
      putCatalog: catalog.putCatalog.bind(catalog), deactivateCatalog: catalog.deactivateCatalog.bind(catalog),
    } }] }).compile();
    const app = module.createNestApplication();
    try {
      await app.init();
      const bearer = `Bearer ${jwt.sign({ role: 'admin' }, secret, { subject: 'admin-test', expiresIn: '5m' })}`;
      const stockResponse = await request(app.getHttpServer()).get('/estoque/one/p1').set('Authorization', bearer).expect(200);
      assert.ok(validateStock(stockResponse.body), JSON.stringify(validateStock.errors));
      const itemResponse = await request(app.getHttpServer()).get('/catalogo/one/PART/p1').set('Authorization', bearer).expect(200);
      assert.ok(validateCatalogItem(itemResponse.body), JSON.stringify(validateCatalogItem.errors));
      const pageResponse = await request(app.getHttpServer()).get('/catalogo/one/PART').set('Authorization', bearer).expect(200);
      assert.ok(validateCatalogPage(pageResponse.body), JSON.stringify(validateCatalogPage.errors));
      await request(app.getHttpServer()).get('/estoque/one/p1').expect(401);
    } finally {
      await app.close();
      if (previousSecret === undefined) delete process.env.JWT_SECRET; else process.env.JWT_SECRET = previousSecret;
      if (previousClientSecret === undefined) delete process.env.JWT_CLIENT_SECRET; else process.env.JWT_CLIENT_SECRET = previousClientSecret;
    }
    process.stdout.write('Cosmos emulado: fila, paginação, backfill, estoque e contratos HTTP autenticados verificados.\n');
  } finally { await database.delete(); }
}
main().finally(() => client.dispose()).catch((error) => { console.error(error.stack || error); process.exitCode = 1; });
