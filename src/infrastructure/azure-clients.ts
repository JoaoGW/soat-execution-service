import { CosmosClient } from '@azure/cosmos';
import { WorkloadIdentityCredential } from '@azure/identity';
import { ServiceBusClient } from '@azure/service-bus';

// Em produção o acesso é exclusivo por identidade de workload. Emuladores só
// são aceitos com ALLOW_LOCAL_EMULATORS=true, para o ambiente kind local e o CI.
const emulators = () => process.env.ALLOW_LOCAL_EMULATORS === 'true';
const workload = () => !!process.env.AZURE_TENANT_ID && !!process.env.AZURE_CLIENT_ID && !!process.env.AZURE_FEDERATED_TOKEN_FILE;
const azureCosmos = (endpoint: string) => /^https:\/\/[a-z0-9-]+\.documents\.azure\.com(?::443)?\/?$/i.test(endpoint);
const localCosmos = (endpoint: string) => emulators() && !!process.env.COSMOS_KEY && /^https?:\/\/[a-z0-9.-]+(?::\d{1,5})?\/?$/i.test(endpoint);
const localServiceBus = () => {
  const connection = process.env.SERVICE_BUS_CONNECTION_STRING;
  return emulators() && connection && /(^|;)\s*UseDevelopmentEmulator=true\s*(;|$)/i.test(connection) ? connection : undefined;
};

export function cosmosConfigured(endpoint: string | undefined): endpoint is string {
  return !!endpoint && (localCosmos(endpoint) || (azureCosmos(endpoint) && workload()));
}

export function serviceBusConfigured(namespace: string | undefined): boolean {
  return !!localServiceBus() || (!!namespace && /^[a-z0-9-]+\.servicebus\.windows\.net$/i.test(namespace) && workload());
}

export function createCosmosClient(endpoint: string, identity?: WorkloadIdentityCredential): CosmosClient {
  // O emulador anuncia o próprio endereço interno; a descoberta mantém o endpoint configurado.
  if (localCosmos(endpoint)) return new CosmosClient({ endpoint, key: process.env.COSMOS_KEY as string, connectionPolicy: { enableEndpointDiscovery: false } });
  return new CosmosClient({ endpoint, aadCredentials: identity ?? new WorkloadIdentityCredential() });
}

/** Cosmos e Service Bus do mesmo worker compartilham uma única identidade de workload. */
export function createWorkerClients(endpoint: string, namespace: string | undefined): { cosmos: CosmosClient; bus: ServiceBusClient } {
  const connection = localServiceBus();
  const identity = localCosmos(endpoint) && connection ? undefined : new WorkloadIdentityCredential();
  return {
    cosmos: createCosmosClient(endpoint, identity),
    bus: connection ? new ServiceBusClient(connection) : new ServiceBusClient(namespace as string, identity ?? new WorkloadIdentityCredential()),
  };
}
