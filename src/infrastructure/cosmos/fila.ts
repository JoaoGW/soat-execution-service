import { Container } from '@azure/cosmos';

type QueueItem = { osId: string; sagaId: string; executionId: string; status: string; createdAt: string; queueKey: string };
const keyPattern = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z\|[a-z0-9_-]{1,128}$/i;

export class CosmosQueueQuery {
  constructor(private readonly container: Pick<Container, 'items'>) {}
  async execute(input: { filialId: string; limit: number; continuationToken?: string }) {
    if (typeof input.filialId !== 'string' || !/^[a-z0-9_-]{1,128}$/i.test(input.filialId) || !Number.isSafeInteger(input.limit) || input.limit < 1 || input.limit > 100 || (input.continuationToken !== undefined && (typeof input.continuationToken !== 'string' || !/^[a-z0-9_-]{1,512}$/i.test(input.continuationToken)))) throw new Error('Consulta inválida');
    let after = '';
    if (input.continuationToken) {
      try {
        const decoded: unknown = JSON.parse(Buffer.from(input.continuationToken, 'base64url').toString('utf8'));
        if (!decoded || typeof decoded !== 'object' || !('filialId' in decoded) || decoded.filialId !== input.filialId || !('key' in decoded) || typeof decoded.key !== 'string' || !keyPattern.test(decoded.key)) throw new Error();
        after = decoded.key;
      } catch { throw new Error('Consulta inválida'); }
    }
    const page = await this.container.items.query<QueueItem>({
      query: 'SELECT TOP @take c.osId, c.sagaId, c.executionId, c.status, c.createdAt, c.queueKey FROM c WHERE c.filialId = @filialId AND STARTSWITH(c.id, @prefix) AND c.status = @status AND c.queueKey > @after ORDER BY c.queueKey ASC',
      parameters: [{ name: '@take', value: input.limit + 1 }, { name: '@filialId', value: input.filialId }, { name: '@prefix', value: 'task:' }, { name: '@status', value: 'QUEUED' }, { name: '@after', value: after }],
    }, { partitionKey: input.filialId, maxItemCount: input.limit + 1 }).fetchAll();
    const items = page.resources.slice(0, input.limit);
    const continuationToken = page.resources.length > input.limit ? Buffer.from(JSON.stringify({ filialId: input.filialId, key: items[items.length - 1].queueKey })).toString('base64url') : null;
    return { items: items.map(({ queueKey: _queueKey, ...item }) => { void _queueKey; return item; }), continuationToken };
  }
}
