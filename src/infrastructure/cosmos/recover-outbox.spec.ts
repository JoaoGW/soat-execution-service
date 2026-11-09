import { Container } from '@azure/cosmos';
import { RecoverExecutionOutbox } from './recover-outbox';

describe('recuperação de outbox Execution', () => {
  const payload = { messageId: 'msg-1', filialId: 'f1', type: 'execution.work-cancelled.v1', producer: 'soat-execution-service' };
  const doc = { id: 'outbox:msg-1', messageId: 'msg-1', filialId: 'f1', status: 'FAILED', payload, createdAt: '2030-01-01', claimToken: '', leaseUntil: 0 };
  const read = jest.fn(); const batch = jest.fn();
  const container = { item: jest.fn(() => ({ read })), items: { batch } };
  const validate = jest.fn();
  const service = new RecoverExecutionOutbox(container as unknown as Container, validate);
  const input = { filialId: 'f1', messageId: 'msg-1', actor: 'operator', reasonCode: 'BROKER_RESTORED' };
  const now = new Date('2030-01-01T00:00:00Z');
  beforeEach(() => { jest.clearAllMocks(); read.mockResolvedValue({ resource: doc, etag: 'etag-1' }); batch.mockResolvedValue({ code: 200, result: [{ statusCode: 200 }, { statusCode: 201 }] }); validate.mockReturnValue(true); });
  it('rearma e audita em um batch com ETag e mesma partição', async () => {
    const id = await service.execute(input, now);
    expect(batch).toHaveBeenCalledWith([expect.objectContaining({ operationType: 'Replace', id: doc.id, ifMatch: 'etag-1', resourceBody: expect.objectContaining({ status: 'PENDING', payload, attempts: 0 }) }), expect.objectContaining({ operationType: 'Create', resourceBody: expect.objectContaining({ id, filialId: 'f1', actor: 'operator', payloadHash: expect.stringMatching(/^[a-f0-9]{64}$/) }) })], 'f1');
  });
  it('recusa argumentos inválidos', async () => {
    for (const bad of [{ ...input, filialId: '../x' }, { ...input, actor: '' }, { ...input, reasonCode: 'texto livre' }, { ...input, messageId: undefined }]) await expect(service.execute(bad as typeof input, now)).rejects.toThrow();
    await expect(service.execute(input, new Date('invalid'))).rejects.toThrow();
    expect(read).not.toHaveBeenCalled();
  });
  it.each([undefined, { ...doc, status: 'SENT' }, { ...doc, claimToken: 'token' }, { ...doc, leaseUntil: 1 }, { ...doc, filialId: 'other' }])('recusa estado não elegível', async resource => {
    read.mockResolvedValue({ resource, etag: 'etag-1' });
    await expect(service.execute(input, now)).rejects.toThrow('não elegível');
    expect(batch).not.toHaveBeenCalled();
  });
  it('recusa ETag ausente e contrato divergente', async () => {
    read.mockResolvedValueOnce({ resource: doc }); await expect(service.execute(input, now)).rejects.toThrow();
    read.mockResolvedValueOnce({ resource: { ...doc, payload: { ...payload, filialId: 'other' } }, etag: 'e' }); await expect(service.execute(input, now)).rejects.toThrow('Contrato');
    read.mockResolvedValueOnce({ resource: doc, etag: 'e' }); validate.mockReturnValueOnce(false); await expect(service.execute(input, now)).rejects.toThrow('Contrato');
  });
  it('recusa batch incompleto ou conflito de ETag', async () => {
    batch.mockResolvedValueOnce({ code: 412, result: [{ statusCode: 412 }, { statusCode: 424 }] }).mockResolvedValueOnce({ code: 200 });
    await expect(service.execute(input, now)).rejects.toThrow('Conflito');
    await expect(service.execute(input, now)).rejects.toThrow('Conflito');
  });
});
