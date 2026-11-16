# Contratos de mensagens v1

Todos os comandos e eventos trocados por Execution têm JSON Schema (draft-07)
nesta pasta e um exemplo válido em [`examples/`](examples). Cada schema estende o
[`envelope.schema.json`](envelope.schema.json), que define `messageId`, `type`,
produtor, agregado e versão local, contexto da OS (`osId`, `sagaId`,
`filialId`), IDs de correlação, causa e operação, e o contexto W3C opcional
(`traceparent`, `tracestate`).

As mensagens são assinadas com Ed25519 pelo produtor (propriedade `signature`) e
publicadas com `subject` igual ao `type`. Comandos chegam pela fila
`execution.commands`; eventos de Execution saem pela outbox transacional para o
tópico `soat.domain.events`.

`npm run test:contracts` valida todos os exemplos e rejeita produtor incorreto,
`data` incompleto e casos específicos (pagamento confirmado e falha de execução).
Os consumidores validam o schema e a assinatura antes de processar.

## Comandos recebidos (produtor: OS)

| Tipo | Consumidor | Efeito em Execution |
| --- | --- | --- |
| `execution.request-diagnosis.v1` | Execution | Abre diagnóstico pendente da OS |
| `execution.reserve-parts.v1` | Execution | Reserva peças do orçamento aprovado |
| `execution.enqueue-work.v1` | Execution | Enfileira o reparo após pagamento confirmado |
| `execution.cancel-work.v1` | Execution | Grava barreira de cancelamento ou responde `ALREADY_STARTED` |
| `execution.release-parts.v1` | Execution | Devolve a reserva após cancelamento confirmado |

## Eventos publicados (produtor: Execution)

| Tipo | Origem | Consumidores |
| --- | --- | --- |
| `execution.diagnosis-started.v1` | Rota `iniciar-diagnostico` | OS |
| `execution.diagnosis-completed.v1` | Rota `concluir-diagnostico` (itens e preços fixados) | OS |
| `execution.parts-reserved.v1` | Comando `reserve-parts` | OS |
| `execution.parts-reservation-failed.v1` | Comando `reserve-parts` sem saldo | OS |
| `execution.work-enqueued.v1` | Comando `enqueue-work` | OS |
| `execution.work-started.v1` | Rota `iniciar` (consome a reserva) | OS |
| `execution.work-completed.v1` | Rota `finalizar` | OS |
| `execution.work-failed.v1` | Rota `falhar` (reparo iniciado inviável; peças já consumidas) | OS (estorno e cancelamento da OS) e Billing (evidência do reembolso) |
| `execution.work-cancelled.v1` | Comando `cancel-work` (`CANCELLED` ou `ALREADY_STARTED`) | OS e Billing (evidência de não início) |
| `execution.parts-released.v1` | Comando `release-parts` | OS |

## Contratos de referência de outros serviços

| Tipo | Produtor | Consumidor | Uso aqui |
| --- | --- | --- | --- |
| `billing.generate-quote.v1` | OS | Billing | Referência do fluxo de orçamento a partir do diagnóstico |
| `billing.payment-confirmed.v1` | Billing | OS | Origem dos identificadores de pagamento levados por `enqueue-work` |

## Regras de evolução

Não enviar CPF, secrets ou connection strings. Alterações aditivas devem
preservar v1; mudanças incompatíveis exigem nova versão e convivência. Os
schemas compartilhados (como `execution.work-failed.v1`) são cópias idênticas
nos repositórios que produzem ou consomem a mensagem.
