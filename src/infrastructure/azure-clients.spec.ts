import { CosmosClient } from '@azure/cosmos';
import { WorkloadIdentityCredential } from '@azure/identity';
import { ServiceBusClient } from '@azure/service-bus';
import { cosmosConfigured, createCosmosClient, createWorkerClients, serviceBusConfigured } from './azure-clients';

jest.mock('@azure/cosmos');
jest.mock('@azure/identity');
jest.mock('@azure/service-bus');

describe('clientes Azure', () => {
  const originalEnv = process.env;
  const emulator = 'Endpoint=sb://servicebus;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;';
  beforeEach(() => {
    jest.clearAllMocks();
    process.env = { ...originalEnv };
    for (const name of ['ALLOW_LOCAL_EMULATORS', 'COSMOS_KEY', 'SERVICE_BUS_CONNECTION_STRING', 'AZURE_TENANT_ID', 'AZURE_CLIENT_ID', 'AZURE_FEDERATED_TOKEN_FILE']) delete process.env[name];
  });
  afterEach(() => { process.env = originalEnv; });

  it('exige identidade de workload e endpoints Azure por padrão', () => {
    process.env.COSMOS_KEY = 'chave'; process.env.SERVICE_BUS_CONNECTION_STRING = emulator;
    expect(cosmosConfigured('http://cosmos:8081')).toBe(false);
    expect(serviceBusConfigured(undefined)).toBe(false);
    expect(cosmosConfigured('https://conta.documents.azure.com:443/')).toBe(false);
    Object.assign(process.env, { AZURE_TENANT_ID: 'tenant', AZURE_CLIENT_ID: 'client', AZURE_FEDERATED_TOKEN_FILE: '/tmp/token' });
    expect(cosmosConfigured('https://conta.documents.azure.com:443/')).toBe(true);
    expect(serviceBusConfigured('conta.servicebus.windows.net')).toBe(true);
    const { cosmos, bus } = createWorkerClients('https://conta.documents.azure.com:443/', 'conta.servicebus.windows.net');
    expect(WorkloadIdentityCredential).toHaveBeenCalledTimes(1);
    expect(CosmosClient).toHaveBeenCalledWith({ endpoint: 'https://conta.documents.azure.com:443/', aadCredentials: expect.any(WorkloadIdentityCredential) });
    expect(ServiceBusClient).toHaveBeenCalledWith('conta.servicebus.windows.net', expect.any(WorkloadIdentityCredential));
    expect(cosmos).toBeInstanceOf(CosmosClient); expect(bus).toBeInstanceOf(ServiceBusClient);
  });

  it('aceita emuladores somente com a trava explícita', () => {
    Object.assign(process.env, { ALLOW_LOCAL_EMULATORS: 'true', COSMOS_KEY: 'chave', SERVICE_BUS_CONNECTION_STRING: emulator });
    expect(cosmosConfigured('http://cosmos:8081')).toBe(true);
    expect(serviceBusConfigured(undefined)).toBe(true);
    createWorkerClients('http://cosmos:8081', undefined);
    expect(WorkloadIdentityCredential).not.toHaveBeenCalled();
    expect(CosmosClient).toHaveBeenCalledWith({ endpoint: 'http://cosmos:8081', key: 'chave', connectionPolicy: { enableEndpointDiscovery: false } });
    expect(ServiceBusClient).toHaveBeenCalledWith(emulator);
    process.env.SERVICE_BUS_CONNECTION_STRING = 'Endpoint=sb://conta.servicebus.windows.net/;SharedAccessKeyName=x;SharedAccessKey=y';
    expect(serviceBusConfigured(undefined)).toBe(false);
    expect(cosmosConfigured('ftp://cosmos')).toBe(false);
  });

  it('cria cliente Cosmos por identidade quando não há chave local', () => {
    createCosmosClient('https://conta.documents.azure.com/');
    expect(CosmosClient).toHaveBeenCalledWith({ endpoint: 'https://conta.documents.azure.com/', aadCredentials: expect.any(WorkloadIdentityCredential) });
  });
});
