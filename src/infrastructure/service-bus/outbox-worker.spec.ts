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
  const dispatcher = { dispatchOne: jest.fn().mockResolvedValue('EMPTY') };
  beforeEach(() => {
    jest.clearAllMocks(); process.env = { ...env, EXECUTION_MESSAGE_PRIVATE_KEY: key }; delete process.env.ENABLE_EXECUTION_OUTBOX;
    bus.createSender.mockReturnValue(sender);
    cosmos.database.mockReturnValue({ container: jest.fn().mockReturnValue({}) });
    jest.mocked(ServiceBusClient).mockImplementation(() => bus as unknown as ServiceBusClient);
    jest.mocked(CosmosClient).mockImplementation(() => cosmos as unknown as CosmosClient);
    jest.mocked(CosmosOutboxDispatcher).mockImplementation(() => dispatcher as unknown as CosmosOutboxDispatcher);
  });
  afterEach(() => { process.env = env; });
  it('não acessa serviços por padrão', async () => {
    const worker = new ExecutionOutboxWorker(); worker.onModuleInit(); await worker.onApplicationShutdown();
    expect(CosmosClient).not.toHaveBeenCalled(); expect(ServiceBusClient).not.toHaveBeenCalled();
  });
  it('exige configuração completa se habilitado', () => {
    process.env.ENABLE_EXECUTION_OUTBOX = 'true';
    expect(() => new ExecutionOutboxWorker().onModuleInit()).toThrow('Configuração incompleta');
  });
  it('usa identidade de workload e encerra recursos', async () => {
    Object.assign(process.env, { ENABLE_EXECUTION_OUTBOX: 'true', COSMOS_ENDPOINT: 'https://example.documents.azure.com/', COSMOS_DATABASE: 'db', COSMOS_CONTAINER: 'operations', SERVICE_BUS_NAMESPACE: 'example.servicebus.windows.net', SERVICE_BUS_EVENT_TOPIC: 'events', AZURE_TENANT_ID: 'tenant', AZURE_CLIENT_ID: 'client', AZURE_FEDERATED_TOKEN_FILE: '/tmp/token' });
    const worker = new ExecutionOutboxWorker(); worker.onModuleInit();
    expect(WorkloadIdentityCredential).toHaveBeenCalledTimes(1);
    expect(dispatcher.dispatchOne).toHaveBeenCalled();
    await worker.onApplicationShutdown();
    expect(sender.close).toHaveBeenCalled(); expect(bus.close).toHaveBeenCalled(); expect(cosmos.dispose).toHaveBeenCalled();
  });
});
