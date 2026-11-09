import { Container } from '@azure/cosmos';

/** Reindexa apenas tarefas da filial; o ETag impede sobrescrever trabalho concorrente. */
export class BackfillQueueKeys {
  constructor(private readonly container: Pick<Container, 'items' | 'item'>) {}
  async execute(filialId: string, apply: boolean): Promise<{ found: number; updated: number }> {
    if (!/^[a-z0-9_-]{1,128}$/i.test(filialId) || typeof apply !== 'boolean') throw new Error('Parâmetros inválidos');
    const result = await this.container.items.query<{ id: string; osId: string; createdAt: string; _etag: string }>({ query: 'SELECT c.id, c.osId, c.createdAt, c._etag FROM c WHERE c.filialId = @filialId AND STARTSWITH(c.id, @prefix) AND c.status = @status AND NOT IS_DEFINED(c.queueKey)', parameters: [{ name: '@filialId', value: filialId }, { name: '@prefix', value: 'task:' }, { name: '@status', value: 'QUEUED' }] }, { partitionKey: filialId }).fetchAll();
    let updated = 0;
    for (const item of result.resources) {
      const when = new Date(item.createdAt);
      if (!/^[a-z0-9_-]{1,128}$/i.test(item.osId) || item.id !== `task:${item.osId}` || !item._etag || !Number.isFinite(when.getTime()) || when.toISOString() !== item.createdAt) throw new Error('Tarefa inválida; reconciliação manual necessária');
      if (apply) {
        await this.container.item(item.id, filialId).patch([{ op: 'add', path: '/queueKey', value: `${item.createdAt}|${item.osId}` }], { accessCondition: { type: 'IfMatch', condition: item._etag } });
        updated++;
      }
    }
    return { found: result.resources.length, updated };
  }
}
