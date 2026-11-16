import { generateKeyPairSync } from 'node:crypto';
import { CosmosClient } from '@azure/cosmos';
import { WorkloadIdentityCredential } from '@azure/identity';
import { ServiceBusClient } from '@azure/service-bus';
import { CosmosOutboxDispatcher } from '../cosmos/outbox';
import { ExecutionOutboxWorker } from './outbox-worker';

jest.mock('@azure/cosmos');
jest.mock('@azure/identity');
jest.mock('@azure/service-bus');
jest.mock('../cosmos/outbox');

describe('ExecutionOutboxWorker', () => {
  const key = generateKeyPairSync('ed25519').privateKey.export({ type: 'pkcs8', format: 'pem' }).toString();
  const env = process.env;
  const sender = { close: jest.fn().mockResolvedValue(undefined) };
  const bus = { createSender: jest.fn(), close: jest.fn().mockResolvedValue(undefined) };
  const cosmos = { database: jest.fn(), dispose: jest.fn() };
  const dispatcher = { dispatchBatch: jest.fn() };
  const enable = () => Object.assign(process.env, { ENABLE_EXECUTION_OUTBOX: 'true', COSMOS_ENDPOINT: 'https://example.documents.azure.com/', COSMOS_DATABASE: 'db', COSMOS_CONTAINER: 'operations', SERVICE_BUS_NAMESPACE: 'example.servicebus.windows.net', SERVICE_BUS_EVENT_TOPIC: 'events', AZURE_TENANT_ID: 'tenant', AZURE_CLIENT_ID: 'client', AZURE_FEDERATED_TOKEN_FILE: '/tmp/token' });
  beforeEach(() => {
    jest.clearAllMocks(); dispatcher.dispatchBatch.mockReset().mockResolvedValue({ sent: 0, full: false }); process.env = { ...env, EXECUTION_MESSAGE_PRIVATE_KEY: key }; delete process.env.ENABLE_EXECUTION_OUTBOX;
    bus.createSender.mockReturnValue(sender);
    cosmos.database.mockReturnValue({ container: jest.fn().mockReturnValue({}) });
    jest.mocked(ServiceBusClient).mockImplementation(() => bus as unknown as ServiceBusClient);
    jest.mocked(CosmosClient).mockImplementation(() => cosmos as unknown as CosmosClient);
    jest.mocked(CosmosOutboxDispatcher).mockImplementation(() => dispatcher as unknown as CosmosOutboxDispatcher);
  });
  afterEach(() => { process.env = env; jest.useRealTimers(); });
  it('não acessa serviços por padrão', async () => {
    const worker = new ExecutionOutboxWorker(); worker.onModuleInit(); await worker.onApplicationShutdown();
    expect(CosmosClient).not.toHaveBeenCalled(); expect(ServiceBusClient).not.toHaveBeenCalled();
  });
  it('exige configuração completa se habilitado', () => {
    process.env.ENABLE_EXECUTION_OUTBOX = 'true';
    expect(() => new ExecutionOutboxWorker().onModuleInit()).toThrow('Configuração incompleta');
  });
  it('usa identidade de workload e encerra recursos', async () => {
    enable();
    const worker = new ExecutionOutboxWorker(); worker.onModuleInit();
    expect(WorkloadIdentityCredential).toHaveBeenCalledTimes(1);
    expect(dispatcher.dispatchBatch).toHaveBeenCalledWith(20);
    await worker.onApplicationShutdown();
    expect(sender.close).toHaveBeenCalled(); expect(bus.close).toHaveBeenCalled(); expect(cosmos.dispose).toHaveBeenCalled();
  });
  it('drena lotes completos sem espera e aguarda somente quando ocioso ou após falha', async () => {
    jest.useFakeTimers(); enable();
    dispatcher.dispatchBatch.mockResolvedValueOnce({ sent: 20, full: true }).mockResolvedValueOnce({ sent: 20, full: true }).mockResolvedValueOnce({ sent: 3, full: false }).mockRejectedValueOnce(new Error('offline'));
    const worker = new ExecutionOutboxWorker(); worker.onModuleInit();
    await jest.advanceTimersByTimeAsync(10); expect(dispatcher.dispatchBatch).toHaveBeenCalledTimes(3);
    await jest.advanceTimersByTimeAsync(980); expect(dispatcher.dispatchBatch).toHaveBeenCalledTimes(3);
    await jest.advanceTimersByTimeAsync(20); expect(dispatcher.dispatchBatch).toHaveBeenCalledTimes(4);
    await jest.advanceTimersByTimeAsync(980); expect(dispatcher.dispatchBatch).toHaveBeenCalledTimes(4);
    await jest.advanceTimersByTimeAsync(20); expect(dispatcher.dispatchBatch).toHaveBeenCalledTimes(5);
    await worker.onApplicationShutdown();
    await jest.advanceTimersByTimeAsync(5000); expect(dispatcher.dispatchBatch).toHaveBeenCalledTimes(5);
  });
  it('não agenda novo ciclo quando o encerramento ocorre durante um lote', async () => {
    jest.useFakeTimers(); enable();
    let finish: (value: { sent: number; full: boolean }) => void = () => undefined;
    dispatcher.dispatchBatch.mockReturnValueOnce(new Promise((resolve) => { finish = resolve; }));
    const worker = new ExecutionOutboxWorker(); worker.onModuleInit();
    const shutdown = worker.onApplicationShutdown(); finish({ sent: 20, full: true }); await shutdown;
    await jest.advanceTimersByTimeAsync(5000); expect(dispatcher.dispatchBatch).toHaveBeenCalledTimes(1);
  });
});
