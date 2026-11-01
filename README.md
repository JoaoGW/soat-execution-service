# Execution Service — Fase 4

Serviço em desenvolvimento; ainda não está pronto para receber tráfego ou
para deploy. As funções de domínio calculam reserva, consumo e liberação de
estoque, agregando quantidades por SKU e limitando a reserva a 20 SKUs.

O adapter de reserva prepara um batch transacional do Cosmos na partição da
filial, com ETags para concorrência e gravação de estoque, reserva, operação,
inbox e outbox. Repetições não duplicam a reserva. Existe um bloqueio durável
para impedir reserva posterior ao cancelamento; esse bloqueio por si só não
comprova que o trabalho físico não começou e não autoriza estorno ou liberação.
O consumidor preparado valida o contrato, identifica mensagens inválidas para
DLQ e confirma o processamento somente após a persistência. Prazo expirado e estoque insuficiente geram falhas duráveis com inbox e
outbox; outras falhas seguem sujeitas a retry/DLQ. O
consumidor ainda precisa ser ligado ao worker e testado com Service Bus real.

Os testes atuais usam uma simulação em memória e não comprovam o comportamento
real de atomicidade, concorrência ou limites do Cosmos. O consumidor de comandos ainda não está conectado ao runtime. O worker da
outbox está conectado, mas desativado por padrão.

Com Node 24: `npm ci`, `npm run lint`, `npm run build`, `npm run test:cov` e
`npm run test:contracts`.

Pendentes: validar Cosmos e identidade com infraestrutura real, integrar
Service Bus e outbox, implementar cancelamento seguro e reconciliação, catálogo,
diagnóstico, fila/reparo, APIs e autorização, Sonar, deploy automatizado,
observabilidade e testes distribuídos.

O worker da outbox só inicia com `ENABLE_EXECUTION_OUTBOX=true` e exige
`COSMOS_ENDPOINT`, `COSMOS_DATABASE`, `COSMOS_CONTAINER`,
`SERVICE_BUS_NAMESPACE`, `SERVICE_BUS_EVENT_TOPIC` e as variáveis de identidade
federada do Azure. As credenciais devem conceder acesso ao container Cosmos e
publicação no topic. Ele usa lease e ETag, retenta cinco vezes e mantém eventos
em `FAILED` para intervenção. A publicação é pelo menos uma vez: após falha de
confirmação, o mesmo `messageId` pode ser enviado novamente. Nenhum serviço real
é contatado se o worker estiver desativado; a integração real permanece pendente.
