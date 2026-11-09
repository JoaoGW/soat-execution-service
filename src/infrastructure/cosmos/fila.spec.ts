import { Container } from '@azure/cosmos';
import { CosmosQueueQuery } from './fila';

describe('consulta da fila Cosmos', () => {
  const fetchAll = jest.fn(); const query = jest.fn(() => ({ fetchAll })); const service = new CosmosQueueQuery({ items: { query } } as unknown as Container);
  const key = '2030-01-01T00:00:01.000Z|os'; const token = Buffer.from(JSON.stringify({ filialId: 'branch', key })).toString('base64url');
  beforeEach(() => { jest.resetAllMocks(); query.mockReturnValue({ fetchAll }); fetchAll.mockResolvedValue({ resources: [{ osId: 'next', status: 'QUEUED', queueKey: '2030-01-01T00:00:02.000Z|next' }] }); });
  it('filtra na partição da filial e usa o cursor somente na mesma filial', async () => {
    await expect(service.execute({ filialId: 'branch', limit: 10, continuationToken: token })).resolves.toEqual({ items: [{ osId: 'next', status: 'QUEUED' }], continuationToken: null });
    expect(query).toHaveBeenCalledWith(expect.objectContaining({ parameters: expect.arrayContaining([{ name: '@filialId', value: 'branch' }, { name: '@status', value: 'QUEUED' }, { name: '@after', value: key }]) }), { partitionKey: 'branch', maxItemCount: 11 });
    await expect(service.execute({ filialId: 'other', limit: 10, continuationToken: token })).rejects.toThrow('Consulta inválida');
  });
  it('recusa limite, filial e token inválidos', async () => {
    for (const input of [{ filialId: '../', limit: 10 }, { filialId: 'branch', limit: 101 }, { filialId: 'branch', limit: 1, continuationToken: '' }]) await expect(service.execute(input)).rejects.toThrow('Consulta inválida');
    expect(query).not.toHaveBeenCalled();
  });
});
