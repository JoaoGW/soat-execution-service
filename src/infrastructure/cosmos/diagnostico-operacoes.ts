import { createHash, randomUUID } from 'node:crypto';
import { Container, ItemDefinition, OperationInput } from '@azure/cosmos';

type Doc = ItemDefinition & Record<string, unknown>;
type Stored = { value: Doc; etag: string };
export type Selection = { sourceId: string; itemType: 'PART' | 'SERVICE'; quantity: number };
export type DiagnosisAction = { osId: string; sagaId: string; filialId: string; operationId: string; correlationId: string; items?: Selection[] };
export type CatalogItem = { sourceId: string; itemType: 'PART' | 'SERVICE'; name: string; unitPriceInCents: number; active: boolean };
const idValid = (value: unknown): value is string => typeof value === 'string' && /^[a-z0-9_-]{1,128}$/i.test(value);
function clean(value: Doc): Doc {
  const { _rid, _self, _etag, _attachments, _ts, ...body } = value;
  void _rid; void _self; void _etag; void _attachments; void _ts;
  return body as Doc;
}
export class CosmosDiagnosisActions {
  constructor(private readonly container: Pick<Container, 'item' | 'items'>) {}
  private async read(id: string, filialId: string): Promise<Stored | null> {
    try {
      const result = await this.container.item(id, filialId).read<Doc>();
      if (!result.resource) return null;
      if (!result.etag) throw new Error('Documento sem ETag');
      return { value: result.resource, etag: result.etag };
    } catch (error) {
      if (typeof error === 'object' && error !== null && 'code' in error && Number(error.code) === 404) return null;
      throw error;
    }
  }
  private async commit(ops: OperationInput[], filialId: string): Promise<boolean> {
    try {
      const result = await this.container.items.batch(ops, filialId); const codes = result.result?.map((item) => item.statusCode) ?? [];
      if ([409, 412].includes(result.code ?? 0) || codes.some((code) => [409, 412].includes(code))) return false;
      if ((result.code ?? 200) >= 400 || codes.length !== ops.length || codes.some((code) => code < 200 || code >= 300)) throw new Error('Falha no batch Cosmos');
      return true;
    } catch (error) {
      if (typeof error === 'object' && error !== null && 'code' in error && [409, 412].includes(Number(error.code))) return false;
      throw error;
    }
  }
  async getCatalog(filialId: string, itemType: string, sourceId: string): Promise<(CatalogItem & { version: number }) | null> {
    if (!idValid(filialId) || !idValid(sourceId) || !['PART', 'SERVICE'].includes(itemType)) throw new Error('Item inválido');
    const item = await this.read(`catalog:${itemType}:${sourceId}`, filialId);
    if (!item) return null;
    const value = item.value;
    if (value.sourceId !== sourceId || value.itemType !== itemType || typeof value.name !== 'string' || !Number.isSafeInteger(value.unitPriceInCents) || typeof value.active !== 'boolean' || !Number.isSafeInteger(value.version)) throw new Error('Item persistido inválido');
    return { sourceId, itemType: itemType as CatalogItem['itemType'], name: value.name, unitPriceInCents: value.unitPriceInCents as number, active: value.active, version: value.version as number };
  }
  async putCatalog(filialId: string, item: CatalogItem): Promise<void> {
    if (!idValid(filialId) || !item || !idValid(item.sourceId) || !['PART', 'SERVICE'].includes(item.itemType) || typeof item.name !== 'string' || !item.name.trim() || item.name.length > 200 || !Number.isSafeInteger(item.unitPriceInCents) || item.unitPriceInCents < 0 || item.unitPriceInCents > 2147483647 || typeof item.active !== 'boolean') throw new Error('Item inválido');
    const id = `catalog:${item.itemType}:${item.sourceId}`;
    for (let attempt = 0; attempt < 5; attempt++) {
      const old = await this.read(id, filialId); const version = old ? Number(old.value.version) + 1 : 1;
      if (!Number.isSafeInteger(version) || version < 1) throw new Error('Versão inválida');
      const resourceBody = { id, filialId, sourceId: item.sourceId, itemType: item.itemType, name: item.name, unitPriceInCents: item.unitPriceInCents, active: item.active, version };
      if (await this.commit([old ? { operationType: 'Replace', id, ifMatch: old.etag, resourceBody } : { operationType: 'Create', resourceBody }], filialId)) return;
    }
    throw new Error('Conflito de concorrência Cosmos');
  }
  async apply(action: 'start' | 'complete', input: DiagnosisAction, now = new Date()): Promise<void> {
    if (![input.osId, input.sagaId, input.filialId, input.operationId, input.correlationId].every(idValid) || !Number.isFinite(now.getTime())) throw new Error('Comando inválido');
    if (action === 'complete' && (!Array.isArray(input.items) || input.items.length < 1 || input.items.length > 20 || input.items.some((item) => !item || !idValid(item.sourceId) || !['PART', 'SERVICE'].includes(item.itemType) || !Number.isSafeInteger(item.quantity) || item.quantity < 1 || item.quantity > 100000))) throw new Error('Itens inválidos');
    const items = input.items ?? []; const keys = items.map((item) => `${item.itemType}:${item.sourceId}`);
    if (new Set(keys).size !== keys.length) throw new Error('Itens duplicados');
    const hash = createHash('sha256').update(JSON.stringify({ action, osId: input.osId, sagaId: input.sagaId, filialId: input.filialId, items })).digest('hex');
    const opId = `operation:diagnosis-${action}:${input.operationId}`; const diagnosisId = `diagnosis:${input.osId}`; const gateId = `gate:${input.sagaId}`;
    for (let attempt = 0; attempt < 5; attempt++) {
      const previous = await this.read(opId, input.filialId);
      if (previous) { if (previous.value.hash !== hash) throw new Error('Operação reutilizada com outro conteúdo'); return; }
      const gate = await this.read(gateId, input.filialId); const diagnosis = await this.read(diagnosisId, input.filialId);
      if (!gate || gate.value.cancelled || !diagnosis || diagnosis.value.osId !== input.osId || diagnosis.value.sagaId !== input.sagaId || diagnosis.value.status !== (action === 'start' ? 'PENDING' : 'IN_PROGRESS')) throw new Error('Diagnóstico não pode avançar');
      const version = Number(diagnosis.value.version) + 1;
      if (!Number.isSafeInteger(version) || version < 2) throw new Error('Versão inválida');
      const catalog = action === 'complete' ? await Promise.all(items.map((item) => this.read(`catalog:${item.itemType}:${item.sourceId}`, input.filialId))) : [];
      if (catalog.some((item) => !item || item.value.active !== true || !Number.isSafeInteger(item.value.unitPriceInCents) || Number(item.value.unitPriceInCents) < 0)) throw new Error('Item de catálogo indisponível');
      const snapshot = items.map((item, index) => ({ sourceId: item.sourceId, itemType: item.itemType, quantity: item.quantity, unitPriceInCents: Number(catalog[index]?.value.unitPriceInCents) }));
      const total = snapshot.reduce((sum, item) => sum + item.quantity * item.unitPriceInCents, 0);
      if (snapshot.some((item) => !Number.isSafeInteger(item.quantity * item.unitPriceInCents)) || !Number.isSafeInteger(total) || total > 2147483647) throw new Error('Total inválido');
      const messageId = randomUUID(); const type = `execution.diagnosis-${action === 'start' ? 'started' : 'completed'}.v1`;
      const event = { messageId, type, schemaVersion: 1, occurredAt: now.toISOString(), producer: 'soat-execution-service', aggregateType: 'diagnosis', aggregateId: input.osId, aggregateVersion: version, osId: input.osId, sagaId: input.sagaId, filialId: input.filialId, correlationId: input.correlationId, causationId: input.operationId, operationId: input.operationId, data: { diagnosisId: input.osId, diagnosisVersion: version, ...(action === 'complete' ? { items: snapshot } : {}) } };
      const ops: OperationInput[] = [
        { operationType: 'Replace', id: gateId, ifMatch: gate.etag, resourceBody: clean(gate.value) },
        { operationType: 'Replace', id: diagnosisId, ifMatch: diagnosis.etag, resourceBody: { ...clean(diagnosis.value), status: action === 'start' ? 'IN_PROGRESS' : 'COMPLETED', version, ...(action === 'complete' ? { items: snapshot, completedAt: now.toISOString() } : { startedAt: now.toISOString() }) } },
        ...catalog.map((entry) => ({ operationType: 'Replace' as const, id: String(entry?.value.id), ifMatch: entry?.etag, resourceBody: clean((entry as Stored).value) })),
        { operationType: 'Create', resourceBody: { id: opId, filialId: input.filialId, hash } },
        { operationType: 'Create', resourceBody: { id: `outbox:${messageId}`, filialId: input.filialId, messageId, status: 'PENDING', payload: event, createdAt: now.toISOString() } },
      ];
      if (await this.commit(ops, input.filialId)) return;
    }
    throw new Error('Conflito de concorrência Cosmos');
  }
}
