import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { ServiceBusReceivedMessage, ServiceBusReceiver } from '@azure/service-bus';
import { createReserveValidator, ReserveCommandConsumer } from './reserve-consumer';

describe('ReserveCommandConsumer', () => {
  const dir = resolve(process.cwd(), 'docs/contracts/messages');
  const validate = createReserveValidator(dir);
  const fixture = JSON.parse(readFileSync(resolve(dir, 'examples/execution.reserve-parts.v1.json'), 'utf8')) as { messageId: string; type: string; data: Record<string, unknown> };
  const message = { body: fixture, messageId: fixture.messageId, subject: fixture.type } as ServiceBusReceivedMessage;
  const store = { reserve: jest.fn() };
  const receiver = { completeMessage: jest.fn(), abandonMessage: jest.fn(), deadLetterMessage: jest.fn() };
  const consumer = new ReserveCommandConsumer(store, validate);
  const broker = receiver as unknown as ServiceBusReceiver;
  beforeEach(() => jest.resetAllMocks());

  it('persiste os dados mapeados antes de confirmar a mensagem', async () => {
    let release!: () => void;
    store.reserve.mockReturnValue(new Promise<void>((resolve) => { release = resolve; }));
    const pending = consumer.process(message, broker);
    expect(store.reserve).toHaveBeenCalledWith(expect.objectContaining({ messageId: fixture.messageId, quoteId: fixture.data.quoteId, items: fixture.data.items }));
    expect(receiver.completeMessage).not.toHaveBeenCalled();
    release();
    await pending;
    expect(receiver.completeMessage).toHaveBeenCalledWith(message);
  });
  it('rejeita contrato, data, subject e id incompatíveis', async () => {
    await consumer.process({ ...message, messageId: 'outro' }, broker);
    await consumer.process({ ...message, subject: 'outra-versao' }, broker);
    await consumer.process({ ...message, body: { ...fixture, schemaVersion: 2 } }, broker);
    await consumer.process({ ...message, body: { ...fixture, data: { ...fixture.data, deadline: 'inválido' } } }, broker);
    expect(receiver.deadLetterMessage).toHaveBeenCalledTimes(4);
    expect(store.reserve).not.toHaveBeenCalled();
  });
  it('não deixa dados adicionais do comando substituir o envelope', async () => {
    await consumer.process({ ...message, body: { ...fixture, data: { ...fixture.data, messageId: 'forjado' } } }, broker);
    expect(store.reserve).toHaveBeenCalledWith(expect.objectContaining({ messageId: fixture.messageId }));
  });
  it('abandona sem confirmar em caso de falha da reserva', async () => {
    store.reserve.mockRejectedValue(new Error('falha'));
    await consumer.process(message, broker);
    expect(receiver.abandonMessage).toHaveBeenCalledWith(message);
    expect(receiver.completeMessage).not.toHaveBeenCalled();
  });
  it('propaga falha do ACK sem tentar abandonar depois', async () => {
    receiver.completeMessage.mockRejectedValue(new Error('lock expirado'));
    await expect(consumer.process(message, broker)).rejects.toThrow('lock expirado');
    expect(receiver.abandonMessage).not.toHaveBeenCalled();
  });
});
