import { ServiceBusSender } from '@azure/service-bus';
import { ServiceBusEventPublisher } from './event-publisher';

describe('ServiceBusEventPublisher', () => {
  const sender = { sendMessages: jest.fn() };
  const publisher = new ServiceBusEventPublisher(sender as unknown as ServiceBusSender);
  beforeEach(() => jest.resetAllMocks());
  it('preserva identificador, correlação e trace', async () => {
    const payload = { messageId: 'm1', type: 'event.v1', correlationId: 'c1', traceparent: 'trace', tracestate: 'state' };
    await publisher.publish({ messageId: 'm1', payload });
    expect(sender.sendMessages).toHaveBeenCalledWith(expect.objectContaining({ messageId: 'm1', subject: 'event.v1', correlationId: 'c1', body: payload, applicationProperties: { traceparent: 'trace', tracestate: 'state' } }));
  });
  it('rejeita divergência e propaga falha de transporte', async () => {
    await expect(publisher.publish({ messageId: 'm1', payload: { messageId: 'm2', type: 'event' } })).rejects.toThrow('Envelope');
    sender.sendMessages.mockRejectedValue(new Error('offline'));
    await expect(publisher.publish({ messageId: 'm1', payload: { messageId: 'm1', type: 'event' } })).rejects.toThrow('offline');
  });
});
