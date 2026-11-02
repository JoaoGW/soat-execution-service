import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { ServiceBusReceivedMessage, ServiceBusReceiver } from '@azure/service-bus';
import { ReleaseCommandConsumer, createReleaseValidator } from './release-consumer';

describe('ReleaseCommandConsumer', () => {
  const dir = resolve(process.cwd(), 'docs/contracts/messages'); const validate = createReleaseValidator(dir);
  const body = JSON.parse(readFileSync(resolve(dir, 'examples/execution.release-parts.v1.json'), 'utf8')) as { messageId: string; type: string; data: { reservationId: string } };
  const message = { body, messageId: body.messageId, subject: body.type } as ServiceBusReceivedMessage;
  const store = { release: jest.fn() }; const receiver = { completeMessage: jest.fn(), abandonMessage: jest.fn(), deadLetterMessage: jest.fn() };
  const consumer = new ReleaseCommandConsumer(store, validate); const broker = receiver as unknown as ServiceBusReceiver;
  beforeEach(() => jest.resetAllMocks());
  it('confirma após gravar barreira e resultado', async () => {
    await consumer.process(message, broker); expect(store.release).toHaveBeenCalledWith(expect.objectContaining({ reservationId: body.data.reservationId })); expect(receiver.completeMessage).toHaveBeenCalledWith(message);
  });
  it('rejeita contrato inválido e retenta falha do banco', async () => {
    await consumer.process({ ...message, messageId: 'other' }, broker); expect(receiver.deadLetterMessage).toHaveBeenCalled();
    store.release.mockRejectedValueOnce(new Error('db')); await consumer.process(message, broker); expect(receiver.abandonMessage).toHaveBeenCalledWith(message);
  });
});
