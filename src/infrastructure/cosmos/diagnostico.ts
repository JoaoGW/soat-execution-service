import { createHash } from 'node:crypto';
import { Container, ItemDefinition, OperationInput } from '@azure/cosmos';

export interface RequestDiagnosis {
  messageId: string; operationId: string; osId: string; sagaId: string; filialId: string;
  correlationId: string; veiculoId: string; solicitacoes?: string[];
  traceparent?: string; tracestate?: string;
}
type Stored<T> = { value: T; etag: string };
type RecordDoc = ItemDefinition & { hash: string; operationId?: string };
type Gate = ItemDefinition & { cancelled: boolean };

/** Persiste somente a solicitação; diagnóstico requer ação de operador autorizada. */
export class CosmosDiagnosisStore {
  constructor(private readonly container: Pick<Container, 'item' | 'items'>) {}
  private async read<T extends ItemDefinition>(id: string, filialId: string): Promise<Stored<T> | null> {
    try {
      const result = await this.container.item(id, filialId).read<T>();
      if (!result.resource) return null;
      if (!result.etag) throw new Error('Documento sem ETag');
      return { value: result.resource, etag: result.etag };
    } catch (error) {
      if (typeof error === 'object' && error !== null && 'code' in error && Number(error.code) === 404) return null;
      throw error;
    }
  }
  private async commit(operations: OperationInput[], filialId: string): Promise<boolean> {
    try {
      const result = await this.container.items.batch(operations, filialId);
      const codes = result.result?.map((item) => item.statusCode) ?? [];
      if ([409, 412].includes(result.code ?? 0) || codes.some((code) => [409, 412].includes(code))) return false;
      if ((result.code ?? 200) >= 400 || codes.length !== operations.length || codes.some((code) => code < 200 || code >= 300)) throw new Error('Falha no batch Cosmos');
      return true;
    } catch (error) {
      if (typeof error === 'object' && error !== null && 'code' in error && [409, 412].includes(Number(error.code))) return false;
      throw error;
    }
  }
  async request(input: RequestDiagnosis, now = new Date()): Promise<void> {
    const ids = [input.messageId, input.operationId, input.osId, input.sagaId, input.filialId, input.correlationId, input.veiculoId];
    if (ids.some((id) => typeof id !== 'string' || !/^[a-z0-9_-]{1,128}$/i.test(id)) || !Number.isFinite(now.getTime()) ||
        (input.solicitacoes !== undefined && (!Array.isArray(input.solicitacoes) || input.solicitacoes.length > 100 || input.solicitacoes.some((item) => typeof item !== 'string' || item.length < 1 || item.length > 500)))) throw new Error('Comando inválido');
    const hash = createHash('sha256').update(JSON.stringify({ osId: input.osId, sagaId: input.sagaId, filialId: input.filialId, veiculoId: input.veiculoId, solicitacoes: input.solicitacoes ?? [] })).digest('hex');
    const operationId = `operation:diagnosis:${input.operationId}`; const inboxId = `inbox:diagnosis:${input.messageId}`;
    const gateId = `gate:${input.sagaId}`; const diagnosisId = `diagnosis:${input.osId}`;
    for (let attempt = 0; attempt < 5; attempt++) {
      const inbox = await this.read<RecordDoc>(inboxId, input.filialId);
      if (inbox && (inbox.value.operationId !== input.operationId || inbox.value.hash !== hash)) throw new Error('Mensagem reutilizada com outro conteúdo');
      const previous = await this.read<RecordDoc>(operationId, input.filialId);
      if (previous) {
        if (previous.value.hash !== hash) throw new Error('Operação reutilizada com outro conteúdo');
        if (!inbox && !await this.commit([{ operationType: 'Create', resourceBody: { id: inboxId, filialId: input.filialId, operationId: input.operationId, hash } }], input.filialId)) continue;
        return;
      }
      if (inbox) throw new Error('Mensagem sem operação correspondente');
      const gate = await this.read<Gate>(gateId, input.filialId);
      if (gate?.value.cancelled) throw new Error('Saga cancelada');
      if (await this.read(diagnosisId, input.filialId)) throw new Error('Diagnóstico já existente');
      const gateBody = { id: gateId, filialId: input.filialId, cancelled: false };
      const operations: OperationInput[] = [
        gate ? { operationType: 'Replace', id: gateId, ifMatch: gate.etag, resourceBody: gateBody } : { operationType: 'Create', resourceBody: gateBody },
        { operationType: 'Create', resourceBody: { id: diagnosisId, filialId: input.filialId, osId: input.osId, sagaId: input.sagaId, veiculoId: input.veiculoId, solicitacoes: input.solicitacoes ?? [], correlationId: input.correlationId, ...(input.traceparent ? { traceparent: input.traceparent } : {}), ...(input.tracestate ? { tracestate: input.tracestate } : {}), status: 'PENDING', version: 1, createdAt: now.toISOString() } },
        { operationType: 'Create', resourceBody: { id: operationId, filialId: input.filialId, hash } },
        { operationType: 'Create', resourceBody: { id: inboxId, filialId: input.filialId, operationId: input.operationId, hash } },
      ];
      if (await this.commit(operations, input.filialId)) return;
      await new Promise((resolve) => setTimeout(resolve, 10 * 2 ** attempt));
    }
    throw new Error('Conflito de concorrência Cosmos');
  }
}
