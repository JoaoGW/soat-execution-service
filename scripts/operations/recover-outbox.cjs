const fs = require('node:fs');
const path = require('node:path');
const Ajv = require('ajv');
const addFormats = require('ajv-formats').default;
const { CosmosClient } = require('@azure/cosmos');
const { WorkloadIdentityCredential } = require('@azure/identity');
const { RecoverExecutionOutbox } = require('../../dist/infrastructure/cosmos/recover-outbox');

const allowed = ['execution.diagnosis-started.v1', 'execution.diagnosis-completed.v1', 'execution.parts-reserved.v1', 'execution.parts-reservation-failed.v1', 'execution.parts-released.v1', 'execution.work-enqueued.v1', 'execution.work-started.v1', 'execution.work-completed.v1', 'execution.work-cancelled.v1'];
async function main() {
  const env = process.env;
  if (env.ENABLE_EXECUTION_OUTBOX_RECOVERY !== 'true' || !env.COSMOS_ENDPOINT || !/^https:\/\/[a-z0-9-]+\.documents\.azure\.com(?::443)?\/?$/i.test(env.COSMOS_ENDPOINT) || !env.COSMOS_DATABASE || !env.COSMOS_CONTAINER || !env.AZURE_TENANT_ID || !env.AZURE_CLIENT_ID || !env.AZURE_FEDERATED_TOKEN_FILE) throw new Error('Configuração incompleta');
  const directory = path.resolve(__dirname, '../../docs/contracts/messages');
  const ajv = addFormats(new Ajv({ allErrors: true }));
  ajv.addSchema(JSON.parse(fs.readFileSync(path.join(directory, 'envelope.schema.json'), 'utf8')));
  const validators = new Map(allowed.map(type => [type, ajv.compile(JSON.parse(fs.readFileSync(path.join(directory, `${type}.schema.json`), 'utf8')))]));
  const client = new CosmosClient({ endpoint: env.COSMOS_ENDPOINT, aadCredentials: new WorkloadIdentityCredential() });
  try {
    const service = new RecoverExecutionOutbox(client.database(env.COSMOS_DATABASE).container(env.COSMOS_CONTAINER), (type, payload) => validators.get(type)?.(payload) === true);
    const auditId = await service.execute({ filialId: env.EXECUTION_RECOVERY_FILIAL, messageId: env.EXECUTION_RECOVERY_MESSAGE_ID, actor: env.EXECUTION_RECOVERY_ACTOR, reasonCode: env.EXECUTION_RECOVERY_REASON });
    process.stdout.write(`Evento rearmado; auditoria ${auditId}\n`);
  } finally { client.dispose(); }
}
main().catch(() => { process.stderr.write('Recuperação recusada ou indisponível; consulte configuração e estado do evento.\n'); process.exitCode = 1; });
