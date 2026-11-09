import { createHash, randomUUID } from 'node:crypto';
import { Container, ItemDefinition, OperationInput } from '@azure/cosmos';
import { OutboxEvent } from './outbox';

export class RecoverExecutionOutbox {
  constructor(private readonly container: Pick<Container, 'item' | 'items'>, private readonly validate: (type: string, payload: unknown) => boolean) {}

  async execute(input: { filialId: string; messageId: string; actor: string; reasonCode: string }, now = new Date()): Promise<string> {
    if (!input || typeof input.filialId !== 'string' || !/^[a-z0-9_-]{1,128}$/i.test(input.filialId) || typeof input.messageId !== 'string' || !/^[a-z0-9_-]{1,128}$/i.test(input.messageId) || typeof input.actor !== 'string' || !/^[a-z0-9_@.-]{1,128}$/i.test(input.actor) || typeof input.reasonCode !== 'string' || !/^[A-Z0-9_]{3,80}$/.test(input.reasonCode) || !Number.isFinite(now.getTime())) throw new Error('Dados de recuperação inválidos');
    const id = `outbox:${input.messageId}`;
    const current = await this.container.item(id, input.filialId).read<OutboxEvent>();
    const event = current.resource;
    if (!event || !current.etag || event.id !== id || event.filialId !== input.filialId || event.messageId !== input.messageId || event.status !== 'FAILED' || (event.claimToken !== undefined && event.claimToken !== '') || (event.leaseUntil !== undefined && event.leaseUntil !== 0)) throw new Error('Evento não elegível para recuperação');
    const payload = event.payload;
    if (!payload || typeof payload !== 'object' || Array.isArray(payload) || !('messageId' in payload) || payload.messageId !== input.messageId || !('filialId' in payload) || payload.filialId !== input.filialId || !('type' in payload) || typeof payload.type !== 'string' || !('producer' in payload) || payload.producer !== 'soat-execution-service' || !this.validate(payload.type, payload)) throw new Error('Contrato do evento inválido');
    const { _rid, _self, _etag, _attachments, _ts, ...clean } = event;
    void _rid; void _self; void _etag; void _attachments; void _ts;
    const auditId = `recovery:${randomUUID()}`;
    const operations: OperationInput[] = [
      { operationType: 'Replace', id, ifMatch: current.etag, resourceBody: { ...clean, status: 'PENDING', attempts: 0, claimToken: '', leaseUntil: 0, nextAttemptAt: now.getTime() } as ItemDefinition },
      { operationType: 'Create', resourceBody: { id: auditId, filialId: input.filialId, messageId: input.messageId, eventType: payload.type, actor: input.actor, reasonCode: input.reasonCode, payloadHash: createHash('sha256').update(JSON.stringify(payload)).digest('hex'), occurredAt: now.toISOString() } },
    ];
    const result = await this.container.items.batch(operations, input.filialId);
    if ((result.code ?? 200) >= 400 || result.result?.length !== 2 || result.result.some(item => item.statusCode < 200 || item.statusCode >= 300)) throw new Error('Conflito ou falha ao recuperar evento');
    return auditId;
  }
}
