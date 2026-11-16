import { BadRequestException, Body, ConflictException, Controller, Delete, Get, Headers, Injectable, NotFoundException, OnApplicationShutdown, Param, Post, Put, Query, Req, ServiceUnavailableException, UnprocessableEntityException, UseGuards } from '@nestjs/common';
import { CosmosClient } from '@azure/cosmos';
import { cosmosConfigured, createCosmosClient } from '../infrastructure/azure-clients';
import { CosmosWorkStore, WorkAction } from '../infrastructure/cosmos/reparo';
import { AdminGuard } from './admin-guard';
import { CatalogItem, CosmosDiagnosisActions, Selection } from '../infrastructure/cosmos/diagnostico-operacoes';
import { CosmosQueueQuery } from '../infrastructure/cosmos/fila';
import { CosmosStockActions } from '../infrastructure/cosmos/estoque';
import { captureTrace } from '../infrastructure/observability/trace-context';
import { BusinessError } from '../domain/erros';

const idPattern = /^[a-z0-9_-]{1,128}$/i;
/** Erros de negócio conhecidos viram 400/404/409/422; qualquer outro erro segue como 500. */
export function httpError(error: unknown): unknown {
  if (!(error instanceof BusinessError)) return error;
  if (error.kind === 'INVALID_INPUT') return new BadRequestException(error.message);
  if (error.kind === 'NOT_FOUND') return new NotFoundException(error.message);
  if (error.kind === 'CONFLICT') return new ConflictException(error.message);
  return new UnprocessableEntityException(error.message);
}
async function guarded<T>(work: () => Promise<T>): Promise<T> {
  try { return await work(); } catch (error) { throw httpError(error); }
}

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
    if (!cosmosConfigured(endpoint) || !database || !container) throw new ServiceUnavailableException('Execution não configurado');
    this.client = createCosmosClient(endpoint);
    const operations = this.client.database(database).container(container);
    this.store = new CosmosWorkStore(operations);
    this.diagnosis = new CosmosDiagnosisActions(operations);
    this.queue = new CosmosQueueQuery(operations);
    this.stock = new CosmosStockActions(operations);
    return this.store;
  }
  async start(input: WorkAction) { return this.getStore().start(input); }
  async complete(input: WorkAction) { return this.getStore().complete(input); }
  async fail(input: WorkAction) { return this.getStore().fail(input); }
  async diagnosisAction(kind: 'start' | 'complete', input: WorkAction, items?: Selection[]) {
    this.getStore();
    return this.diagnosis!.apply(kind, { osId: input.osId, sagaId: input.sagaId, filialId: input.filialId, operationId: input.operationId, correlationId: input.correlationId, ...(items ? { items } : {}) });
  }
  async putCatalog(filialId: string, item: CatalogItem, operationId: string) { this.getStore(); return this.diagnosis!.putCatalog(filialId, item, operationId); }
  async getCatalog(filialId: string, itemType: string, sourceId: string) { this.getStore(); return this.diagnosis!.getCatalog(filialId, itemType, sourceId); }
  async listCatalog(input: { filialId: string; itemType: string; limit: number; continuationToken?: string }) { this.getStore(); return this.diagnosis!.listCatalog(input); }
  async deactivateCatalog(filialId: string, itemType: string, sourceId: string, operationId: string) { this.getStore(); return this.diagnosis!.deactivateCatalog(filialId, itemType, sourceId, operationId); }
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
    const result = await guarded(() => this.work.getStock(filialId, sku)); if (!result) throw new NotFoundException(); return result;
  }
  @Put(':filialId/:sku')
  async set(@Param('filialId') filialId: string, @Param('sku') sku: string, @Headers('idempotency-key') operationId: string | undefined, @Req() request: { adminId?: string }, @Body() body: { balance: number; reason: string }) {
    if (![filialId, sku, operationId, body?.reason].every(value => typeof value === 'string' && /^[a-z0-9_-]{1,128}$/i.test(value)) || typeof request.adminId !== 'string' || !/^[a-z0-9_@.-]{1,128}$/i.test(request.adminId) || !body || typeof body !== 'object' || Array.isArray(body) || Object.keys(body).some(key => !['balance', 'reason'].includes(key)) || !Number.isSafeInteger(body.balance) || body.balance < 0 || body.balance > 2147483647) throw new BadRequestException('Dados inválidos');
    const actorId = request.adminId;
    return guarded(() => this.work.setStock({ filialId, sku, operationId: operationId!, actorId, balance: body.balance, reason: body.reason }));
  }
}

type ActionBody = { sagaId: string; filialId: string; executionId: string; correlationId?: string };
function action(osId: string, operationId: string | undefined, body: ActionBody): WorkAction {
  if (!body || typeof body !== 'object' || Array.isArray(body) || Object.keys(body).some((key) => !['sagaId', 'filialId', 'executionId', 'correlationId'].includes(key))) throw new BadRequestException('Corpo inválido');
  const ids = [osId, operationId, body.sagaId, body.filialId, body.executionId, ...(body.correlationId === undefined ? [] : [body.correlationId])];
  if (ids.some((id) => typeof id !== 'string' || !/^[a-z0-9_-]{1,128}$/i.test(id))) throw new BadRequestException('Identificadores inválidos');
  return { osId, sagaId: body.sagaId, filialId: body.filialId, executionId: body.executionId, operationId: operationId as string, messageId: operationId as string, correlationId: body.correlationId ?? (operationId as string), ...captureTrace() };
}

@Controller('execucoes')
@UseGuards(AdminGuard)
export class RepairController {
  constructor(private readonly work: WorkActions) {}
  @Get('fila')
  async listQueue(@Query('filialId') filialId: string, @Query('limit') limit?: string, @Query('continuationToken') continuationToken?: string) {
    if (typeof filialId !== 'string' || !/^[a-z0-9_-]{1,128}$/i.test(filialId) || (limit !== undefined && (typeof limit !== 'string' || !/^(?:[1-9]|[1-9][0-9]|100)$/.test(limit))) || (continuationToken !== undefined && (typeof continuationToken !== 'string' || !/^[a-z0-9_-]{1,512}$/i.test(continuationToken)))) throw new BadRequestException('Consulta inválida');
    return guarded(() => this.work.listQueue({ filialId, limit: limit === undefined ? 20 : Number(limit), ...(continuationToken ? { continuationToken } : {}) }));
  }
  @Post(':osId/iniciar')
  async start(@Param('osId') osId: string, @Headers('idempotency-key') operationId: string | undefined, @Body() body: ActionBody) {
    const input = action(osId, operationId, body);
    return { status: await guarded(() => this.work.start(input)) };
  }
  @Post(':osId/finalizar')
  async complete(@Param('osId') osId: string, @Headers('idempotency-key') operationId: string | undefined, @Body() body: ActionBody) {
    const input = action(osId, operationId, body);
    return { status: await guarded(() => this.work.complete(input)) };
  }
  /** Reparo iniciado declarado inviável; as peças já consumidas não são devolvidas e o OS compensa com estorno. */
  @Post(':osId/falhar')
  async fail(@Param('osId') osId: string, @Headers('idempotency-key') operationId: string | undefined, @Body() body: ActionBody & { reason: string }) {
    if (!body || typeof body !== 'object' || Array.isArray(body)) throw new BadRequestException('Corpo inválido');
    const { reason, ...rest } = body;
    const input = action(osId, operationId, rest);
    if (typeof reason !== 'string' || !idPattern.test(reason)) throw new BadRequestException('Motivo inválido');
    return { status: await guarded(() => this.work.fail({ ...input, reason })) };
  }
  @Post(':osId/iniciar-diagnostico')
  async startDiagnosis(@Param('osId') osId: string, @Headers('idempotency-key') operationId: string | undefined, @Body() body: Omit<ActionBody, 'executionId'>) {
    const input = action(osId, operationId, { ...body, executionId: 'diagnosis' });
    await guarded(() => this.work.diagnosisAction('start', input));
    return { status: 'IN_PROGRESS' };
  }
  @Post(':osId/concluir-diagnostico')
  async completeDiagnosis(@Param('osId') osId: string, @Headers('idempotency-key') operationId: string | undefined, @Body() body: Omit<ActionBody, 'executionId'> & { items: Selection[] }) {
    if (!body || typeof body !== 'object' || Array.isArray(body)) throw new BadRequestException('Corpo inválido');
    const { items, ...rest } = body;
    if (!Array.isArray(items) || items.length < 1 || items.length > 20 || items.some((item) => !item || typeof item !== 'object' || Array.isArray(item) || Object.keys(item).some((key) => !['sourceId', 'itemType', 'quantity'].includes(key)) || typeof item.sourceId !== 'string' || !/^[a-z0-9_-]{1,128}$/i.test(item.sourceId) || !['PART', 'SERVICE'].includes(item.itemType) || !Number.isSafeInteger(item.quantity) || item.quantity < 1 || item.quantity > 100000)) throw new BadRequestException('Itens inválidos');
    const input = action(osId, operationId, { ...rest, executionId: 'diagnosis' });
    await guarded(() => this.work.diagnosisAction('complete', input, items));
    return { status: 'COMPLETED' };
  }
}

@Controller('catalogo')
@UseGuards(AdminGuard)
export class CatalogController {
  constructor(private readonly work: WorkActions) {}
  @Get(':filialId/:itemType')
  async list(@Param('filialId') filialId: string, @Param('itemType') itemType: string, @Query('limit') limit?: string, @Query('continuationToken') continuationToken?: string) {
    if (typeof filialId !== 'string' || !/^[a-z0-9_-]{1,128}$/i.test(filialId) || !['PART', 'SERVICE'].includes(itemType) || (limit !== undefined && (typeof limit !== 'string' || !/^(?:[1-9]|[1-9][0-9]|100)$/.test(limit))) || (continuationToken !== undefined && (typeof continuationToken !== 'string' || !/^[a-z0-9_-]{1,512}$/i.test(continuationToken)))) throw new BadRequestException('Consulta inválida');
    return guarded(() => this.work.listCatalog({ filialId, itemType, limit: limit === undefined ? 20 : Number(limit), ...(continuationToken ? { continuationToken } : {}) }));
  }
  @Get(':filialId/:itemType/:sourceId')
  async get(@Param('filialId') filialId: string, @Param('itemType') itemType: string, @Param('sourceId') sourceId: string) {
    if (!/^[a-z0-9_-]{1,128}$/i.test(filialId) || !/^[a-z0-9_-]{1,128}$/i.test(sourceId) || !['PART', 'SERVICE'].includes(itemType)) throw new BadRequestException('Item inválido');
    const item = await guarded(() => this.work.getCatalog(filialId, itemType, sourceId));
    if (!item) throw new NotFoundException();
    return item;
  }
  @Put(':filialId/:itemType/:sourceId')
  async put(@Param('filialId') filialId: string, @Param('itemType') itemType: string, @Param('sourceId') sourceId: string, @Headers('idempotency-key') operationId: string | undefined, @Body() body: Pick<CatalogItem, 'name' | 'unitPriceInCents' | 'active'>) {
    if (typeof operationId !== 'string' || !idPattern.test(operationId)) throw new BadRequestException('Idempotency-Key inválida');
    if (!body || typeof body !== 'object' || Array.isArray(body) || Object.keys(body).some((key) => !['name', 'unitPriceInCents', 'active'].includes(key)) || !/^[a-z0-9_-]{1,128}$/i.test(filialId) || !/^[a-z0-9_-]{1,128}$/i.test(sourceId) || !['PART', 'SERVICE'].includes(itemType) || typeof body.name !== 'string' || !body.name.trim() || body.name.length > 200 || !Number.isSafeInteger(body.unitPriceInCents) || body.unitPriceInCents < 0 || body.unitPriceInCents > 2147483647 || typeof body.active !== 'boolean') throw new BadRequestException('Item inválido');
    const item = { sourceId, itemType: itemType as CatalogItem['itemType'], name: body.name, unitPriceInCents: body.unitPriceInCents, active: body.active };
    await guarded(() => this.work.putCatalog(filialId, item, operationId));
    return { status: 'SAVED' };
  }
  @Delete(':filialId/:itemType/:sourceId')
  async remove(@Param('filialId') filialId: string, @Param('itemType') itemType: string, @Param('sourceId') sourceId: string, @Headers('idempotency-key') operationId: string | undefined) {
    if (typeof operationId !== 'string' || !idPattern.test(operationId)) throw new BadRequestException('Idempotency-Key inválida');
    if (![filialId, sourceId].every(value => typeof value === 'string' && /^[a-z0-9_-]{1,128}$/i.test(value)) || !['PART', 'SERVICE'].includes(itemType)) throw new BadRequestException('Item inválido');
    if (!await guarded(() => this.work.deactivateCatalog(filialId, itemType, sourceId, operationId))) throw new NotFoundException();
    return { status: 'INACTIVE' };
  }
}
