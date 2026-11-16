import { randomUUID } from 'node:crypto';
import { Container, ItemDefinition, OperationInput } from '@azure/cosmos';

export interface OutboxEvent extends ItemDefinition {
  id: string; filialId: string; messageId: string; status: 'PENDING' | 'PROCESSING' | 'SENT' | 'FAILED';
  payload: unknown; createdAt: string; attempts?: number; claimToken?: string; leaseUntil?: number; nextAttemptAt?: number;
}
export interface EventPublisher { publish(event: { messageId: string; payload: unknown }): Promise<void> }

type Stored = { document: OutboxEvent; etag: string };
export type DispatchResult = 'SENT' | 'RETRY' | 'FAILED' | 'CONFLICT';

/** At-least-once delivery: a crash after sending can result in a duplicate. */
export class CosmosOutboxDispatcher {
  constructor(private readonly container: Pick<Container, 'item' | 'items'>, private readonly publisher: EventPublisher,
    private readonly clock: () => number = Date.now, private readonly leaseMs = 60000, private readonly maxAttempts = 5) {}

  private async read(id: string, filialId: string): Promise<Stored | null> {
    try {
      const result = await this.container.item(id, filialId).read<OutboxEvent>();
      if (!result.resource) return null;
      if (!result.etag) throw new Error('Outbox sem ETag');
      return { document: result.resource, etag: result.etag };
    } catch (error) {
      if (typeof error === 'object' && error !== null && 'code' in error && Number(error.code) === 404) return null;
      throw error;
    }
  }

  private async replace(stored: Stored, document: OutboxEvent): Promise<boolean> {
    const { _rid, _self, _etag, _attachments, _ts, ...body } = document;
    void _rid; void _self; void _etag; void _attachments; void _ts;
    const operation: OperationInput = { operationType: 'Replace', id: document.id, ifMatch: stored.etag, resourceBody: body };
    try {
      const response = await this.container.items.batch([operation], document.filialId);
      const status = response.result?.[0]?.statusCode;
      if ([409, 412].includes(response.code ?? 0) || [409, 412].includes(status ?? 0)) return false;
      if ((response.code ?? 200) >= 400 || status === undefined || status < 200 || status >= 300) throw new Error('Falha ao atualizar outbox');
      return true;
    } catch (error) {
      if (typeof error === 'object' && error !== null && 'code' in error && [409, 412].includes(Number(error.code))) return false;
      throw error;
    }
  }

  private async candidates(now: number, take: number): Promise<Array<{ id: string; filialId: string }>> {
    const query = { query: `SELECT TOP @take c.id, c.filialId FROM c WHERE STARTSWITH(c.id, 'outbox:') AND ((c.status = 'PENDING' AND (NOT IS_DEFINED(c.nextAttemptAt) OR c.nextAttemptAt <= @now)) OR (c.status = 'PROCESSING' AND c.leaseUntil <= @now)) ORDER BY c.createdAt`, parameters: [{ name: '@take', value: take }, { name: '@now', value: now }] };
    const { resources } = await this.container.items.query(query).fetchAll();
    return resources as Array<{ id: string; filialId: string }>;
  }

  /** Reivindica e publica um candidato; `null` quando outro worker já o assumiu ou ele deixou de ser elegível. */
  private async dispatch(candidate: { id: string; filialId: string }): Promise<DispatchResult | null> {
    const now = this.clock();
    const current = await this.read(candidate.id, candidate.filialId);
    if (!current) return null;
    const doc = current.document;
    if (!(doc.status === 'PENDING' && (doc.nextAttemptAt ?? 0) <= now) && !(doc.status === 'PROCESSING' && (doc.leaseUntil ?? Infinity) <= now)) return null;
    const token = randomUUID();
    const claim = { ...doc, status: 'PROCESSING' as const, claimToken: token, leaseUntil: now + this.leaseMs, attempts: (doc.attempts ?? 0) + 1 };
    if (!await this.replace(current, claim)) return null;
    let failed = false;
    try { await this.publisher.publish({ messageId: doc.messageId, payload: doc.payload }); } catch { failed = true; }
    const latest = await this.read(doc.id, doc.filialId);
    if (!latest || latest.document.status !== 'PROCESSING' || latest.document.claimToken !== token) return 'CONFLICT';
    const attempts = latest.document.attempts ?? 1;
    const status = failed ? (attempts >= this.maxAttempts ? 'FAILED' : 'PENDING') : 'SENT';
    const completed: OutboxEvent = { ...latest.document, status, leaseUntil: 0, claimToken: '', ...(failed && status === 'PENDING' ? { nextAttemptAt: this.clock() + Math.min(60000, 1000 * 2 ** (attempts - 1)) } : {}) };
    if (!await this.replace(latest, completed)) return 'CONFLICT';
    return status === 'SENT' ? 'SENT' : status === 'FAILED' ? 'FAILED' : 'RETRY';
  }

  async dispatchOne(): Promise<DispatchResult | 'EMPTY'> {
    for (const candidate of await this.candidates(this.clock(), 20)) {
      const result = await this.dispatch(candidate);
      if (result) return result;
    }
    return 'EMPTY';
  }

  /**
   * Drena até `limit` eventos elegíveis em ordem de criação, um por vez. Interrompe o lote na primeira falha
   * de publicação (RETRY/FAILED) para não insistir com o broker indisponível nem adiantar eventos posteriores.
   * `full` indica que o lote foi inteiramente publicado e provavelmente há mais pendências.
   */
  async dispatchBatch(limit = 20): Promise<{ sent: number; full: boolean }> {
    if (!Number.isSafeInteger(limit) || limit < 1 || limit > 100) throw new Error('Tamanho de lote inválido');
    let sent = 0;
    for (const candidate of await this.candidates(this.clock(), limit)) {
      const result = await this.dispatch(candidate);
      if (result === 'SENT') sent++;
      else if (result === 'RETRY' || result === 'FAILED') break;
    }
    return { sent, full: sent === limit };
  }
}
