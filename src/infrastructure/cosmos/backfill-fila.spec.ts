import { Container } from '@azure/cosmos';
import { BackfillQueueKeys } from './backfill-fila';

describe('backfill de tarefas em fila', () => {
  const fetchAll = jest.fn(), query = jest.fn(), patch = jest.fn(), item = jest.fn();
  const service = new BackfillQueueKeys({ items: { query }, item } as unknown as Container);
  const legacy = { id: 'task:os', osId: 'os', createdAt: '2030-01-01T00:00:00.000Z', _etag: 'etag' };
  beforeEach(() => { jest.resetAllMocks(); query.mockReturnValue({ fetchAll }); fetchAll.mockResolvedValue({ resources: [legacy] }); item.mockReturnValue({ patch }); patch.mockResolvedValue({}); });
  it('inspeciona sem alterar e aplica com ETag', async () => {
    await expect(service.execute('branch', false)).resolves.toEqual({ found: 1, updated: 0 }); expect(patch).not.toHaveBeenCalled();
    await expect(service.execute('branch', true)).resolves.toEqual({ found: 1, updated: 1 });
    expect(item).toHaveBeenCalledWith('task:os', 'branch');
    expect(patch).toHaveBeenCalledWith([{ op: 'add', path: '/queueKey', value: '2030-01-01T00:00:00.000Z|os' }], { accessCondition: { type: 'IfMatch', condition: 'etag' } });
  });
  it('recusa tarefa inválida e propaga conflito de concorrência', async () => {
    fetchAll.mockResolvedValueOnce({ resources: [{ ...legacy, createdAt: 'inválido' }] });
    await expect(service.execute('branch', true)).rejects.toThrow('Tarefa inválida'); expect(patch).not.toHaveBeenCalled();
    patch.mockRejectedValueOnce(new Error('etag conflict'));
    await expect(service.execute('branch', true)).rejects.toThrow('etag conflict');
  });
});
