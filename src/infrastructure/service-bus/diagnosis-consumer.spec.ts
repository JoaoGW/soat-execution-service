import { ServiceBusReceivedMessage, ServiceBusReceiver } from '@azure/service-bus';
import { resolve } from 'node:path';
import { readFileSync } from 'node:fs';
import { createDiagnosisValidator, DiagnosisCommandConsumer } from './diagnosis-consumer';

describe('consumidor de diagnóstico', () => {
  const dir = resolve(process.cwd(), 'docs/contracts/messages');
  const body = JSON.parse(readFileSync(resolve(dir, 'examples/execution.request-diagnosis.v1.json'), 'utf8')) as { messageId: string };
  const receiver = { completeMessage: jest.fn(), abandonMessage: jest.fn(), deadLetterMessage: jest.fn() };
  const store = { request: jest.fn() }; const validate = createDiagnosisValidator(dir);
  const consumer = new DiagnosisCommandConsumer(store, validate);
  const deliver = (value: unknown, id = body.messageId) => consumer.process({ body: value, messageId: id } as ServiceBusReceivedMessage, receiver as unknown as ServiceBusReceiver);
  beforeEach(() => { jest.resetAllMocks(); store.request.mockResolvedValue(undefined); });
  it('valida contrato e só confirma após persistir', async () => {
    await deliver(body); expect(store.request).toHaveBeenCalledWith(expect.objectContaining({ veiculoId: 'veiculo-001' })); expect(receiver.completeMessage).toHaveBeenCalledTimes(1);
  });
  it('persiste contexto W3C junto à solicitação autenticada', async () => {
    const traceparent = '00-0123456789abcdef0123456789abcdef-0123456789abcdef-01';
    await deliver({ ...body, traceparent, tracestate: 'vendor=state' });
    expect(store.request).toHaveBeenCalledWith(expect.objectContaining({ traceparent, tracestate: 'vendor=state' }));
  });
  it('rejeita contrato inválido e não confirma erro de persistência', async () => {
    await deliver(body, 'wrong'); expect(receiver.deadLetterMessage).toHaveBeenCalledTimes(1);
    store.request.mockRejectedValue(new Error('falha')); await deliver(body); expect(receiver.abandonMessage).toHaveBeenCalledTimes(1); expect(receiver.completeMessage).not.toHaveBeenCalled();
  });
});
