# Recuperação de eventos de Execution

Um evento em `FAILED` pode ter sido entregue antes de uma falha de confirmação.
Antes de rearmá-lo, restaure o broker e confira o tipo, estado físico e Saga,
assinaturas e deduplicação no destinatário. Não edite o payload ou seus IDs e
não use o procedimento para inferir que reparo, reserva ou liberação ocorreu.

Um operador com identidade de workload autorizada no Cosmos pode executar a
versão compilada com `npm run ops:recover-outbox`. Configure
`ENABLE_EXECUTION_OUTBOX_RECOVERY=true`, `EXECUTION_RECOVERY_FILIAL`,
`EXECUTION_RECOVERY_MESSAGE_ID`, `EXECUTION_RECOVERY_ACTOR` e
`EXECUTION_RECOVERY_REASON`. O motivo é um código com letras maiúsculas, números
ou `_`, por exemplo `BROKER_RESTORED`. Também são necessárias as variáveis
`COSMOS_ENDPOINT`, `COSMOS_DATABASE`, `COSMOS_CONTAINER`, `AZURE_TENANT_ID`,
`AZURE_CLIENT_ID` e `AZURE_FEDERATED_TOKEN_FILE` da identidade de workload.

O ator é informado pelo ambiente, não autenticado pela ferramenta. Restrinja a
execução e o acesso ao Cosmos, e correlacione o ator com os registros de acesso
da plataforma. O comando recusa eventos que não sejam próprios e conhecidos,
estejam fora de `FAILED`, mantenham claim ou não passem pelo schema. Ele usa
ETag para atualizar o mesmo evento e criar documento de auditoria `recovery:`
em um único batch na partição da filial. A auditoria registra ator, motivo,
IDs, tipo e hash; duas tentativas concorrentes não geram duas recuperações.

Aguarde a publicação e confira o processamento e a deduplicação na Saga. O
rearme não confirma entrega nem altera diretamente execução, estoque ou estado
financeiro. O batch foi testado no emulador, mas a identidade, permissões e
garantias operacionais do Cosmos real ainda precisam ser validadas.
