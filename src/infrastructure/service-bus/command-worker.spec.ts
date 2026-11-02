import { CosmosClient } from '@azure/cosmos';
import { WorkloadIdentityCredential } from '@azure/identity';
import { ServiceBusClient } from '@azure/service-bus';
import { ReserveCommandConsumer } from './reserve-consumer';
import { EnqueueCommandConsumer } from './enqueue-consumer';
import { CancelCommandConsumer } from './cancel-consumer';
import { ReleaseCommandConsumer } from './release-consumer';
import { DiagnosisCommandConsumer } from './diagnosis-consumer';
import { ExecutionCommandWorker } from './command-worker';

jest.mock('@azure/cosmos'); jest.mock('@azure/identity'); jest.mock('@azure/service-bus');
jest.mock('./reserve-consumer', () => ({ ReserveCommandConsumer: jest.fn(), createReserveValidator: jest.fn(() => () => true) }));
jest.mock('./enqueue-consumer', () => ({ EnqueueCommandConsumer: jest.fn(), createEnqueueValidator: jest.fn(() => () => true) }));
jest.mock('./cancel-consumer', () => ({ CancelCommandConsumer: jest.fn(), createCancelValidator: jest.fn(() => () => true) }));
jest.mock('./release-consumer', () => ({ ReleaseCommandConsumer: jest.fn(), createReleaseValidator: jest.fn(() => () => true) }));
jest.mock('./diagnosis-consumer', () => ({ DiagnosisCommandConsumer: jest.fn(), createDiagnosisValidator: jest.fn(() => () => true) }));

describe('ExecutionCommandWorker', () => {
  const original = process.env; const subscription = { close: jest.fn().mockResolvedValue(undefined) };
  const receiver = { subscribe: jest.fn(), abandonMessage: jest.fn().mockResolvedValue(undefined), close: jest.fn().mockResolvedValue(undefined) };
  const bus = { createReceiver: jest.fn(), close: jest.fn().mockResolvedValue(undefined) };
  const cosmos = { database: jest.fn(), dispose: jest.fn() };
  const reserve = { process: jest.fn().mockResolvedValue(undefined) }; const enqueue = { process: jest.fn().mockResolvedValue(undefined) }; const cancel = { process: jest.fn().mockResolvedValue(undefined) }; const release = { process: jest.fn().mockResolvedValue(undefined) };
  const diagnosis = { process: jest.fn().mockResolvedValue(undefined) };
  beforeEach(() => {
    jest.clearAllMocks(); process.env = { ...original }; delete process.env.ENABLE_EXECUTION_COMMANDS;
    receiver.subscribe.mockReturnValue(subscription); bus.createReceiver.mockReturnValue(receiver); cosmos.database.mockReturnValue({ container: jest.fn().mockReturnValue({}) });
    jest.mocked(CosmosClient).mockImplementation(() => cosmos as unknown as CosmosClient);
    jest.mocked(ServiceBusClient).mockImplementation(() => bus as unknown as ServiceBusClient);
    jest.mocked(ReserveCommandConsumer).mockImplementation(() => reserve as unknown as ReserveCommandConsumer);
    jest.mocked(EnqueueCommandConsumer).mockImplementation(() => enqueue as unknown as EnqueueCommandConsumer);
    jest.mocked(CancelCommandConsumer).mockImplementation(() => cancel as unknown as CancelCommandConsumer);
    jest.mocked(ReleaseCommandConsumer).mockImplementation(() => release as unknown as ReleaseCommandConsumer);
    jest.mocked(DiagnosisCommandConsumer).mockImplementation(() => diagnosis as unknown as DiagnosisCommandConsumer);
  });
  afterEach(() => { process.env = original; });
  it('não conecta por padrão', async () => {
    const worker = new ExecutionCommandWorker(); worker.onModuleInit(); await worker.onApplicationShutdown();
    expect(ServiceBusClient).not.toHaveBeenCalled(); expect(CosmosClient).not.toHaveBeenCalled();
  });
  it('falha ao habilitar sem configuração completa', () => {
    process.env.ENABLE_EXECUTION_COMMANDS = 'true'; delete process.env.COSMOS_ENDPOINT;
    expect(() => new ExecutionCommandWorker().onModuleInit()).toThrow('Configuração incompleta');
  });
  it('roteia comandos conhecidos e não confirma tipo ainda não implementado', async () => {
    Object.assign(process.env, { ENABLE_EXECUTION_COMMANDS: 'true', COSMOS_ENDPOINT: 'https://example.documents.azure.com/', COSMOS_DATABASE: 'db', COSMOS_CONTAINER: 'operations', SERVICE_BUS_NAMESPACE: 'example.servicebus.windows.net', SERVICE_BUS_COMMAND_QUEUE: 'execution.commands', AZURE_TENANT_ID: 'tenant', AZURE_CLIENT_ID: 'client', AZURE_FEDERATED_TOKEN_FILE: '/tmp/token' });
    const worker = new ExecutionCommandWorker(); worker.onModuleInit();
    expect(WorkloadIdentityCredential).toHaveBeenCalledTimes(1);
    expect(receiver.subscribe).toHaveBeenCalledWith(expect.any(Object), { autoCompleteMessages: false, maxConcurrentCalls: 1 });
    const handlers = receiver.subscribe.mock.calls[0][0] as { processMessage(message: unknown): Promise<void> };
    const a = { body: { type: 'execution.reserve-parts.v1' } }; const b = { body: { type: 'execution.enqueue-work.v1' } }; const c = { body: { type: 'execution.request-diagnosis.v1' } }; const d = { body: { type: 'execution.cancel-work.v1' } }; const e = { body: { type: 'execution.release-parts.v1' } }; const unknown = { body: { type: 'unsupported' } };
    await handlers.processMessage(a); await handlers.processMessage(b); await handlers.processMessage(c); await handlers.processMessage(d); await handlers.processMessage(e);
    await handlers.processMessage(unknown);
    expect(reserve.process).toHaveBeenCalledWith(a, receiver); expect(enqueue.process).toHaveBeenCalledWith(b, receiver); expect(diagnosis.process).toHaveBeenCalledWith(c, receiver); expect(receiver.abandonMessage).toHaveBeenCalledWith(unknown); expect(cancel.process).toHaveBeenCalledWith(d, receiver); expect(release.process).toHaveBeenCalledWith(e, receiver);
    await worker.onApplicationShutdown();
    expect(subscription.close).toHaveBeenCalled(); expect(receiver.close).toHaveBeenCalled(); expect(bus.close).toHaveBeenCalled(); expect(cosmos.dispose).toHaveBeenCalled();
  });
});
