import { ServiceBusMessage, ServiceBusSender } from '@azure/service-bus';
import { EventPublisher } from '../cosmos/outbox';

export class ServiceBusEventPublisher implements EventPublisher {
  constructor(private readonly sender: Pick<ServiceBusSender, 'sendMessages'>) {}
  async publish(event: { messageId: string; payload: unknown }): Promise<void> {
    const p = event.payload;
    if (!p || typeof p !== 'object' || !('messageId' in p) || p.messageId !== event.messageId || !('type' in p) || typeof p.type !== 'string') throw new Error('Envelope da outbox inválido');
    const correlationId = 'correlationId' in p && typeof p.correlationId === 'string' ? p.correlationId : undefined;
    const traceparent = 'traceparent' in p && typeof p.traceparent === 'string' ? p.traceparent : undefined;
    const tracestate = 'tracestate' in p && typeof p.tracestate === 'string' ? p.tracestate : undefined;
    const message: ServiceBusMessage = { messageId: event.messageId, subject: p.type, contentType: 'application/json', correlationId, body: p, applicationProperties: { ...(traceparent ? { traceparent } : {}), ...(tracestate ? { tracestate } : {}) } };
    await this.sender.sendMessages(message);
  }
}
