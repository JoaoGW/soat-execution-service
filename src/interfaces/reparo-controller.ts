import { BadRequestException, Body, ConflictException, Controller, Get, Headers, Injectable, NotFoundException, OnApplicationShutdown, Param, Post, Put, Query, Req, ServiceUnavailableException, UseGuards } from '@nestjs/common';
import { CosmosClient } from '@azure/cosmos';
import { WorkloadIdentityCredential } from '@azure/identity';
import { CosmosWorkStore, WorkAction } from '../infrastructure/cosmos/reparo';
import { AdminGuard } from './admin-guard';
import { CatalogItem, CosmosDiagnosisActions, Selection } from '../infrastructure/cosmos/diagnostico-operacoes';
import { CosmosQueueQuery } from '../infrastructure/cosmos/fila';
import { CosmosStockActions } from '../infrastructure/cosmos/estoque';

@Injectable()
export class WorkActions implements OnApplicationShutdown {
  private client?: CosmosClient;
  private store?: CosmosWorkStore;
  private diagnosis?: CosmosDiagnosisActions;
  private queue?: CosmosQueueQuery;
  private stock?: CosmosStockActions;
  private getStore(): CosmosWorkStore {
    if (this.store) return this.store;
    const endpoint = process.env.COSMOS_ENDPOINT; const database = process.env.COSMOS_DATABASE; const container = process.env.COSMOS_CONTAINER;
    if (!endpoint || !/^https:\/\/[a-z0-9-]+\.documents\.azure\.com(?::443)?\/?$/i.test(endpoint) || !database || !container || !process.env.AZURE_TENANT_ID || !process.env.AZURE_CLIENT_ID || !process.env.AZURE_FEDERATED_TOKEN_FILE) throw new ServiceUnavailableException('Execution não configurado');
    this.client = new CosmosClient({ endpoint, aadCredentials: new WorkloadIdentityCredential() });
    const operations = this.client.database(database).container(container);
    this.store = new CosmosWorkStore(operations);
    this.diagnosis = new CosmosDiagnosisActions(operations);
    this.queue = new CosmosQueueQuery(operations);
    this.stock = new CosmosStockActions(operations);
    return this.store;
  }
  async start(input: WorkAction) { return this.getStore().start(input); }
  async complete(input: WorkAction) { return this.getStore().complete(input); }
  async diagnosisAction(kind: 'start' | 'complete', input: WorkAction, items?: Selection[]) {
    this.getStore();
    return this.diagnosis!.apply(kind, { osId: input.osId, sagaId: input.sagaId, filialId: input.filialId, operationId: input.operationId, correlationId: input.correlationId, ...(items ? { items } : {}) });
  }
  async putCatalog(filialId: string, item: CatalogItem) { this.getStore(); return this.diagnosis!.putCatalog(filialId, item); }
  async getCatalog(filialId: string, itemType: string, sourceId: string) { this.getStore(); return this.diagnosis!.getCatalog(filialId, itemType, sourceId); }
  async listQueue(input: { filialId: string; limit: number; continuationToken?: string }) { this.getStore(); return this.queue!.execute(input); }
  async getStock(filialId: string, sku: string) { this.getStore(); return this.stock!.get(filialId, sku); }
  async setStock(input: { filialId: string; sku: string; balance: number; operationId: string; actorId: string; reason: string }) { this.getStore(); return this.stock!.set(input); }
  onApplicationShutdown() { this.client?.dispose(); }
}

@Controller('estoque')
@UseGuards(AdminGuard)
export class StockController {
  constructor(private readonly work: WorkActions) {}
  @Get(':filialId/:sku')
  async get(@Param('filialId') filialId: string, @Param('sku') sku: string) {
    if (![filialId, sku].every(value => typeof value === 'string' && /^[a-z0-9_-]{1,128}$/i.test(value))) throw new BadRequestException('Identificadores inválidos');
    const result = await this.work.getStock(filialId, sku); if (!result) throw new NotFoundException(); return result;
  }
  @Put(':filialId/:sku')
  async set(@Param('filialId') filialId: string, @Param('sku') sku: string, @Headers('idempotency-key') operationId: string | undefined, @Req() request: { adminId?: string }, @Body() body: { balance: number; reason: string }) {
    if (![filialId, sku, operationId, body?.reason].every(value => typeof value === 'string' && /^[a-z0-9_-]{1,128}$/i.test(value)) || typeof request.adminId !== 'string' || !/^[a-z0-9_@.-]{1,128}$/i.test(request.adminId) || !body || typeof body !== 'object' || Array.isArray(body) || Object.keys(body).some(key => !['balance', 'reason'].includes(key)) || !Number.isSafeInteger(body.balance) || body.balance < 0 || body.balance > 2147483647) throw new BadRequestException('Dados inválidos');
    try { return await this.work.setStock({ filialId, sku, operationId: operationId!, actorId: request.adminId, balance: body.balance, reason: body.reason }); }
    catch (error) { const message = error instanceof Error ? error.message : ''; if (message === 'Peça não encontrada') throw new NotFoundException(); if (['Saldo menor que o reservado', 'Chave de idempotência conflitante', 'Conflito de concorrência Cosmos'].includes(message)) throw new ConflictException(message); throw error; }
  }
}

type ActionBody = { sagaId: string; filialId: string; executionId: string; correlationId?: string };
function action(osId: string, operationId: string | undefined, body: ActionBody): WorkAction {
  if (!body || typeof body !== 'object' || Array.isArray(body) || Object.keys(body).some((key) => !['sagaId', 'filialId', 'executionId', 'correlationId'].includes(key))) throw new BadRequestException('Corpo inválido');
  const ids = [osId, operationId, body.sagaId, body.filialId, body.executionId, ...(body.correlationId === undefined ? [] : [body.correlationId])];
  if (ids.some((id) => typeof id !== 'string' || !/^[a-z0-9_-]{1,128}$/i.test(id))) throw new BadRequestException('Identificadores inválidos');
  return { osId, sagaId: body.sagaId, filialId: body.filialId, executionId: body.executionId, operationId: operationId as string, messageId: operationId as string, correlationId: body.correlationId ?? (operationId as string) };
}

@Controller('execucoes')
@UseGuards(AdminGuard)
export class RepairController {
  constructor(private readonly work: WorkActions) {}
  @Get('fila')
  async listQueue(@Query('filialId') filialId: string, @Query('limit') limit?: string, @Query('continuationToken') continuationToken?: string) {
    if (typeof filialId !== 'string' || !/^[a-z0-9_-]{1,128}$/i.test(filialId) || (limit !== undefined && (typeof limit !== 'string' || !/^(?:[1-9]|[1-9][0-9]|100)$/.test(limit))) || (continuationToken !== undefined && (typeof continuationToken !== 'string' || !/^[a-z0-9_-]{1,512}$/i.test(continuationToken)))) throw new BadRequestException('Consulta inválida');
    try { return await this.work.listQueue({ filialId, limit: limit === undefined ? 20 : Number(limit), ...(continuationToken ? { continuationToken } : {}) }); }
    catch (error) { if (error instanceof Error && error.message === 'Consulta inválida') throw new BadRequestException('Consulta inválida'); throw error; }
  }
  @Post(':osId/iniciar')
  async start(@Param('osId') osId: string, @Headers('idempotency-key') operationId: string | undefined, @Body() body: ActionBody) {
    return { status: await this.work.start(action(osId, operationId, body)) };
  }
  @Post(':osId/finalizar')
  async complete(@Param('osId') osId: string, @Headers('idempotency-key') operationId: string | undefined, @Body() body: ActionBody) {
    return { status: await this.work.complete(action(osId, operationId, body)) };
  }
  @Post(':osId/iniciar-diagnostico')
  async startDiagnosis(@Param('osId') osId: string, @Headers('idempotency-key') operationId: string | undefined, @Body() body: Omit<ActionBody, 'executionId'>) {
    const input = action(osId, operationId, { ...body, executionId: 'diagnosis' });
    await this.work.diagnosisAction('start', input);
    return { status: 'IN_PROGRESS' };
  }
  @Post(':osId/concluir-diagnostico')
  async completeDiagnosis(@Param('osId') osId: string, @Headers('idempotency-key') operationId: string | undefined, @Body() body: Omit<ActionBody, 'executionId'> & { items: Selection[] }) {
    if (!body || typeof body !== 'object' || Array.isArray(body)) throw new BadRequestException('Corpo inválido');
    const { items, ...rest } = body;
    if (!Array.isArray(items) || items.length < 1 || items.length > 20 || items.some((item) => !item || typeof item !== 'object' || Array.isArray(item) || Object.keys(item).some((key) => !['sourceId', 'itemType', 'quantity'].includes(key)) || typeof item.sourceId !== 'string' || !/^[a-z0-9_-]{1,128}$/i.test(item.sourceId) || !['PART', 'SERVICE'].includes(item.itemType) || !Number.isSafeInteger(item.quantity) || item.quantity < 1 || item.quantity > 100000)) throw new BadRequestException('Itens inválidos');
    const input = action(osId, operationId, { ...rest, executionId: 'diagnosis' });
    await this.work.diagnosisAction('complete', input, items);
    return { status: 'COMPLETED' };
  }
}

@Controller('catalogo')
@UseGuards(AdminGuard)
export class CatalogController {
  constructor(private readonly work: WorkActions) {}
  @Get(':filialId/:itemType/:sourceId')
  async get(@Param('filialId') filialId: string, @Param('itemType') itemType: string, @Param('sourceId') sourceId: string) {
    if (!/^[a-z0-9_-]{1,128}$/i.test(filialId) || !/^[a-z0-9_-]{1,128}$/i.test(sourceId) || !['PART', 'SERVICE'].includes(itemType)) throw new BadRequestException('Item inválido');
    const item = await this.work.getCatalog(filialId, itemType, sourceId);
    if (!item) throw new NotFoundException();
    return item;
  }
  @Put(':filialId/:itemType/:sourceId')
  async put(@Param('filialId') filialId: string, @Param('itemType') itemType: string, @Param('sourceId') sourceId: string, @Body() body: Pick<CatalogItem, 'name' | 'unitPriceInCents' | 'active'>) {
    if (!body || typeof body !== 'object' || Array.isArray(body) || Object.keys(body).some((key) => !['name', 'unitPriceInCents', 'active'].includes(key)) || !/^[a-z0-9_-]{1,128}$/i.test(filialId) || !/^[a-z0-9_-]{1,128}$/i.test(sourceId) || !['PART', 'SERVICE'].includes(itemType) || typeof body.name !== 'string' || !body.name.trim() || body.name.length > 200 || !Number.isSafeInteger(body.unitPriceInCents) || body.unitPriceInCents < 0 || body.unitPriceInCents > 2147483647 || typeof body.active !== 'boolean') throw new BadRequestException('Item inválido');
    await this.work.putCatalog(filialId, { sourceId, itemType: itemType as CatalogItem['itemType'], name: body.name, unitPriceInCents: body.unitPriceInCents, active: body.active });
    return { status: 'SAVED' };
  }
}
