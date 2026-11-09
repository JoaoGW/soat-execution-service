import { createHash } from 'node:crypto';
import { Container, ItemDefinition, OperationInput } from '@azure/cosmos';

type Stock = ItemDefinition & { id: string; filialId: string; sku: string; balance: number; reserved: number };
type Outcome = { sku: string; filialId: string; balance: number; reserved: number; available: number };
type Ledger = ItemDefinition & { hash: string; result: Outcome };
type Stored<T> = { value: T; etag: string };
const idValid = (value: unknown): value is string => typeof value === 'string' && /^[a-z0-9_-]{1,128}$/i.test(value);

export class CosmosStockActions {
  constructor(private readonly container: Pick<Container, 'item' | 'items'>) {}
  private async read<T extends ItemDefinition>(id: string, filialId: string): Promise<Stored<T> | null> {
    try { const result = await this.container.item(id, filialId).read<T>(); if (!result.resource) return null; if (!result.etag) throw new Error('Documento sem ETag'); return { value: result.resource, etag: result.etag }; }
    catch (error) { if (typeof error === 'object' && error !== null && 'code' in error && Number(error.code) === 404) return null; throw error; }
  }
  private view(stock: Stock): Outcome {
    if (!idValid(stock.sku) || !idValid(stock.filialId) || stock.id !== `stock:${stock.sku}` || !Number.isSafeInteger(stock.balance) || !Number.isSafeInteger(stock.reserved) || stock.reserved < 0 || stock.balance < stock.reserved || stock.balance > 2147483647) throw new Error('Estoque persistido inválido');
    return { sku: stock.sku, filialId: stock.filialId, balance: stock.balance, reserved: stock.reserved, available: stock.balance - stock.reserved };
  }
  async get(filialId: string, sku: string): Promise<Outcome | null> {
    if (!idValid(filialId) || !idValid(sku)) throw new Error('Dados inválidos');
    const stock = await this.read<Stock>(`stock:${sku}`, filialId); return stock ? this.view(stock.value) : null;
  }
  async set(input: { filialId: string; sku: string; balance: number; operationId: string; actorId: string; reason: string }, now = new Date()): Promise<Outcome> {
    if (![input.filialId, input.sku, input.operationId, input.reason].every(idValid) || typeof input.actorId !== 'string' || !/^[a-z0-9_@.-]{1,128}$/i.test(input.actorId) || !Number.isSafeInteger(input.balance) || input.balance < 0 || input.balance > 2147483647 || !Number.isFinite(now.getTime())) throw new Error('Dados inválidos');
    const hash = createHash('sha256').update(JSON.stringify({ filialId: input.filialId, sku: input.sku, balance: input.balance, actorId: input.actorId, reason: input.reason })).digest('hex');
    const ledgerId = `operation:stock-set:${input.operationId}`, stockId = `stock:${input.sku}`;
    for (let attempt = 0; attempt < 5; attempt++) {
      const prior = await this.read<Ledger>(ledgerId, input.filialId);
      if (prior) { if (prior.value.hash !== hash) throw new Error('Chave de idempotência conflitante'); return prior.value.result; }
      const catalog = await this.read<ItemDefinition & { sourceId?: string; itemType?: string }>(`catalog:PART:${input.sku}`, input.filialId);
      if (!catalog || catalog.value.sourceId !== input.sku || catalog.value.itemType !== 'PART') throw new Error('Peça não encontrada');
      const current = await this.read<Stock>(stockId, input.filialId); const reserved = current ? this.view(current.value).reserved : 0;
      if (input.balance < reserved) throw new Error('Saldo menor que o reservado');
      const stock: Stock = { id: stockId, filialId: input.filialId, sku: input.sku, balance: input.balance, reserved };
      const result = this.view(stock);
      const ops: OperationInput[] = [current ? { operationType: 'Replace', id: stockId, ifMatch: current.etag, resourceBody: stock } : { operationType: 'Create', resourceBody: stock }, { operationType: 'Create', resourceBody: { id: ledgerId, filialId: input.filialId, hash, result, actorId: input.actorId, reason: input.reason, occurredAt: now.toISOString() } }];
      try {
        const response = await this.container.items.batch(ops, input.filialId); const codes = response.result?.map(item => item.statusCode) ?? [];
        if ([409, 412].includes(response.code ?? 0) || codes.some(code => [409, 412].includes(code))) continue;
        if ((response.code ?? 200) >= 400 || codes.length !== ops.length || codes.some(code => code < 200 || code >= 300)) throw new Error('Falha no batch Cosmos');
        return result;
      } catch (error) { if (typeof error === 'object' && error !== null && 'code' in error && [409, 412].includes(Number(error.code))) continue; throw error; }
    }
    throw new Error('Conflito de concorrência Cosmos');
  }
}
