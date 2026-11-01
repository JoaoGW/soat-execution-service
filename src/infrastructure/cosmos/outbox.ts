import { randomUUID } from 'node:crypto';
import { Container, ItemDefinition, OperationInput } from '@azure/cosmos';

export interface OutboxEvent extends ItemDefinition {
  id: string; filialId: string; messageId: string; status: 'PENDING' | 'PROCESSING' | 'SENT' | 'FAILED';
  payload: unknown; createdAt: string; attempts?: number; claimToken?: string; leaseUntil?: number; nextAttemptAt?: number;
}
export interface EventPublisher { publish(event: { messageId: string; payload: unknown }): Promise<void> }

type Stored = { document: OutboxEvent; etag: string };

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

  async dispatchOne(): Promise<'EMPTY' | 'SENT' | 'RETRY' | 'FAILED' | 'CONFLICT'> {
    const now = this.clock();
    const query = { query: `SELECT TOP 20 c.id, c.filialId FROM c WHERE STARTSWITH(c.id, 'outbox:') AND ((c.status = 'PENDING' AND (NOT IS_DEFINED(c.nextAttemptAt) OR c.nextAttemptAt <= @now)) OR (c.status = 'PROCESSING' AND c.leaseUntil <= @now)) ORDER BY c.createdAt`, parameters: [{ name: '@now', value: now }] };
    const { resources } = await this.container.items.query(query).fetchAll();
    for (const candidate of resources as Array<{ id: string; filialId: string }>) {
      const current = await this.read(candidate.id, candidate.filialId);
      if (!current) continue;
      const doc = current.document;
      if (!(doc.status === 'PENDING' && (doc.nextAttemptAt ?? 0) <= now) && !(doc.status === 'PROCESSING' && (doc.leaseUntil ?? Infinity) <= now)) continue;
      const token = randomUUID();
      const claim = { ...doc, status: 'PROCESSING' as const, claimToken: token, leaseUntil: now + this.leaseMs, attempts: (doc.attempts ?? 0) + 1 };
      if (!await this.replace(current, claim)) continue;
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
    return 'EMPTY';
  }
}
