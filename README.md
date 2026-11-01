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
DLQ e confirma o processamento somente após a persistência. A falha de reserva
ainda não produz evento de negócio durável e segue sujeita a retry/DLQ. O
consumidor ainda precisa ser ligado ao worker e testado com Service Bus real.

Os testes atuais usam uma simulação em memória e não comprovam o comportamento
real de atomicidade, concorrência ou limites do Cosmos. O adapter e o consumidor ainda não estão
conectados ao runtime do serviço.

Com Node 24: `npm ci`, `npm run lint`, `npm run build`, `npm run test:cov` e
`npm run test:contracts`.

Pendentes: validar Cosmos e identidade com infraestrutura real, integrar
Service Bus e outbox, implementar cancelamento seguro e reconciliação, catálogo,
diagnóstico, fila/reparo, APIs e autorização, Sonar, deploy automatizado,
observabilidade e testes distribuídos.
