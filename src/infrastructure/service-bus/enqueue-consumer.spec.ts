import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { ServiceBusReceivedMessage, ServiceBusReceiver } from '@azure/service-bus';
import { EnqueueCommandConsumer, createEnqueueValidator } from './enqueue-consumer';

describe('EnqueueCommandConsumer', () => {
  const dir = resolve(process.cwd(), 'docs/contracts/messages');
  const validator = createEnqueueValidator(dir);
  const body = JSON.parse(readFileSync(resolve(dir, 'examples/execution.enqueue-work.v1.json'), 'utf8')) as { messageId: string; type: string; data: { executionId: string } };
  const message = { body, messageId: body.messageId, subject: body.type } as ServiceBusReceivedMessage;
  const receiver = { completeMessage: jest.fn(), abandonMessage: jest.fn(), deadLetterMessage: jest.fn() }; const store = { enqueue: jest.fn() };
  const consumer = new EnqueueCommandConsumer(store, validator); const broker = receiver as unknown as ServiceBusReceiver;
  beforeEach(() => jest.resetAllMocks());
  it('grava execução antes de confirmar', async () => {
    await consumer.process(message, broker);
    expect(store.enqueue).toHaveBeenCalledWith(expect.objectContaining({ messageId: body.messageId, executionId: body.data.executionId }));
    expect(receiver.completeMessage).toHaveBeenCalledWith(message);
  });
  it('envia contrato incompatível à DLQ', async () => {
    await consumer.process({ ...message, messageId: 'wrong' }, broker);
    await consumer.process({ ...message, body: { ...body, producer: 'other' } }, broker);
    expect(receiver.deadLetterMessage).toHaveBeenCalledTimes(2); expect(store.enqueue).not.toHaveBeenCalled();
  });
  it('abandona falha de gravação e propaga erro de ACK', async () => {
    store.enqueue.mockRejectedValueOnce(new Error('offline')); await consumer.process(message, broker);
    expect(receiver.abandonMessage).toHaveBeenCalledWith(message);
    receiver.completeMessage.mockRejectedValueOnce(new Error('lock'));
    await expect(consumer.process(message, broker)).rejects.toThrow('lock');
  });
});
