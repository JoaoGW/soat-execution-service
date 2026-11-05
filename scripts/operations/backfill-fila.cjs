const { CosmosClient } = require('@azure/cosmos');
const { WorkloadIdentityCredential } = require('@azure/identity');
const { BackfillQueueKeys } = require('../../dist/infrastructure/cosmos/backfill-fila');
const env = process.env; const branch = env.QUEUE_BACKFILL_FILIAL;
if (!branch || !/^[a-z0-9_-]{1,128}$/i.test(branch) || !env.COSMOS_ENDPOINT || !/^https:\/\/[a-z0-9-]+\.documents\.azure\.com(?::443)?\/?$/i.test(env.COSMOS_ENDPOINT) || !env.COSMOS_DATABASE || !env.COSMOS_CONTAINER || !env.AZURE_TENANT_ID || !env.AZURE_CLIENT_ID || !env.AZURE_FEDERATED_TOKEN_FILE) throw Error('Configuração incompleta');
const args = process.argv.slice(2); if (args.length > 1 || (args.length && args[0] !== '--apply')) throw Error('Use --apply para alterar; sem argumentos executa apenas leitura');
const client = new CosmosClient({ endpoint: env.COSMOS_ENDPOINT, aadCredentials: new WorkloadIdentityCredential() });
new BackfillQueueKeys(client.database(env.COSMOS_DATABASE).container(env.COSMOS_CONTAINER)).execute(branch, args[0] === '--apply').then((result) => process.stdout.write(`${JSON.stringify(result)}\n`)).catch((error) => { console.error(error.message); process.exitCode = 1; }).finally(() => client.dispose());
