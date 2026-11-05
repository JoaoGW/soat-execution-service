# Execution Service — Fase 4

`GET /execucoes/fila?filialId=...&limit=20` exige JWT administrativo e lista
trabalhos `QUEUED` daquela filial em ordem de entrada, com no máximo 100 itens.
A resposta inclui `continuationToken` para a próxima página; o cursor pertence
à mesma filial. O teste `npm run test:cosmos-queue` exige
`COSMOS_TEST_ENDPOINT=http://127.0.0.1:8081` e usa um banco temporário no
emulador. A indexação do caminho `queueKey` é necessária no Cosmos real.
A referência REST das rotas implementadas está em [docs/openapi.yaml](docs/openapi.yaml).

`GET /estoque/:filialId/:sku` consulta saldo, reservado e disponível; o
`PUT` no mesmo caminho ajusta o saldo absoluto e exige JWT administrativo,
`Idempotency-Key` e corpo `{"balance":10,"reason":"INVENTORY"}`. A peça
precisa existir no catálogo da filial e o novo saldo não pode ser menor que a
quantidade reservada. O ajuste, sua chave, motivo e ator são gravados em batch
transacional; uma repetição retorna o resultado original sem reverter ajustes
posteriores. O teste no emulador não substitui a validação de atomicidade e
concorrência no Cosmos real.

Tarefas enfileiradas por versões anteriores podem não ter `queueKey`. Antes de
ativar a consulta em ambiente com dados existentes, execute por filial
`QUEUE_BACKFILL_FILIAL=<filial> npm run ops:backfill-queue` com identidade e
configuração Cosmos do workload. Sem argumentos o comando apenas inspeciona;
`npm run ops:backfill-queue -- --apply` grava as chaves com ETag, sem alterar
outros campos. Em conflito, reavalie e execute novamente. Tarefas antigas sem
a chave não aparecem na listagem até esse procedimento.

Serviço em desenvolvimento; ainda não está pronto para receber tráfego ou
para deploy. As funções de domínio calculam reserva, consumo e liberação de
estoque, agregando quantidades por SKU e limitando a reserva a 20 SKUs.

O adapter de reserva prepara um batch transacional do Cosmos na partição da
filial, com ETags para concorrência e gravação de estoque, reserva, operação,
inbox e outbox. Repetições não duplicam a reserva. Existe um bloqueio durável
para impedir reserva posterior ao cancelamento; esse bloqueio por si só não
comprova que o trabalho físico não começou e não autoriza estorno ou liberação.
O consumidor preparado valida o contrato, identifica mensagens inválidas para
DLQ e confirma o processamento somente após a persistência. Prazo expirado
e estoque insuficiente geram falhas duráveis com inbox e outbox; outras falhas
seguem sujeitas a retry/DLQ. O worker precisa ser testado com Service Bus real.

O enfileiramento prepara uma transação na mesma partição para conferir a
reserva, disputar ETags com o bloqueio de cancelamento e gravar tarefa, inbox,
operação e evento. A tarefa fica `QUEUED`; não inicia reparo nem consome peças.
Os consumidores de reserva e enfileiramento estão ligados ao runtime,
mas desativados por padrão. Outros comandos ainda não são suportados e podem
chegar à DLQ após as tentativas; não habilitar até completar os handlers e
validar o isolamento de produtores. Cancelamento concorrente e idempotência
foram testados em simulação; a operação real ainda precisa de validação.

Os testes atuais usam uma simulação em memória e não comprovam o comportamento
real de atomicidade, concorrência ou limites do Cosmos. Os workers de comandos e outbox estão conectados, mas desativados por padrão.

Com Node 24: `npm ci`, `npm run lint`, `npm run build`, `npm run test:cov` e
`npm run test:contracts`.

Pendentes: validar Cosmos e identidade com infraestrutura real, integrar
reconciliação e compensação à Saga, CRUD completo e listagem do catálogo,
Sonar, deploy automatizado,
observabilidade e testes distribuídos.

O worker da outbox só inicia com `ENABLE_EXECUTION_OUTBOX=true` e exige
`COSMOS_ENDPOINT`, `COSMOS_DATABASE`, `COSMOS_CONTAINER`,
`SERVICE_BUS_NAMESPACE`, `SERVICE_BUS_EVENT_TOPIC` e as variáveis de identidade
federada do Azure. As credenciais devem conceder acesso ao container Cosmos e
publicação no topic. Ele usa lease e ETag, retenta cinco vezes e mantém eventos
em `FAILED` para intervenção. A publicação é pelo menos uma vez: após falha de
confirmação, o mesmo `messageId` pode ser enviado novamente. Nenhum serviço real
é contatado se o worker estiver desativado; a integração real permanece pendente.

O consumidor exige `ENABLE_EXECUTION_COMMANDS=true`, a configuração do Cosmos,
da fila Service Bus e a identidade de workload. A ligação real e o isolamento
de produtores ainda não foram validados.

O adapter de reparo prepara início e conclusão com ETags, operação, inbox e
outbox. O início consome a reserva de peças atomicamente. Cancelamento antes do
início grava uma barreira; depois do início retorna `ALREADY_STARTED` e não
reverte trabalho físico. A liberação exige a operação de cancelamento
confirmada e nenhuma tarefa iniciada; não verifica a situação financeira, que
continua sendo responsabilidade da Saga. Cancelamento e liberação têm
consumidores preparados. Os testes de concorrência usam Cosmos simulado.

A solicitação de diagnóstico é recebida pelo worker e gravada como pendente,
com inbox, idempotência e barreira de cancelamento na mesma partição. As rotas
administrativas `POST /execucoes/:osId/iniciar-diagnostico`,
`/concluir-diagnostico`, `/iniciar` e `/finalizar` exigem JWT administrativo
HS256, expiração e `Idempotency-Key`. Recebem `sagaId`, `filialId` e,
nas ações de reparo, `executionId`; a conclusão de diagnóstico recebe também
`items` com `sourceId`, `itemType` e `quantity`. O servidor consulta o catálogo
da filial e fixa os preços em centavos antes de publicar os eventos. As rotas
`PUT` e `GET /catalogo/:filialId/:itemType/:sourceId` administram e consultam
um item. Configurar `JWT_SECRET` com a chave administrativa do OS (mínimo de
32 bytes), distinta de `JWT_CLIENT_SECRET`, e as variáveis de Cosmos e workload
identity. A entrega desses segredos e os acessos precisam ser configurados
antes do uso. O consumo dos eventos de diagnóstico e a geração do comando de
orçamento foram preparados no OS; o fluxo distribuído ainda requer validação.

O transporte exige assinatura Ed25519 para autenticar o OS. Ao habilitar os
workers, configure `OS_MESSAGE_PUBLIC_KEY` e a chave privada exclusiva de
Execution em `EXECUTION_MESSAGE_PRIVATE_KEY`; eventos são assinados e comandos
sem assinatura válida seguem à DLQ antes do processamento. Não compartilhe
chaves privadas entre serviços. Distribuição e rotação das chaves ainda precisam
ser configuradas e testadas no ambiente real.
