# Execution Service — Fase 4

Esqueleto em desenvolvimento. Este repositório ainda não implementa o serviço
de execução nem está pronto para receber tráfego ou efetuar deploy.

As funções de domínio calculam reserva, consumo e liberação de estoque, com
agregação por SKU e limite de 20 SKUs. Elas não fazem persistência: a atomicidade
em Cosmos, concorrência por ETag, inbox/outbox e impedimento de trabalho após
cancelamento ainda precisam ser implementados e testados em ambiente real.

Com Node 24: `npm ci`, `npm run lint`, `npm run build`, `npm run test:cov`.
Pendentes: Cosmos e identidade de dados, catálogo, diagnóstico, fila/reparo,
APIs e autorização, Service Bus, Sonar, deploy automatizado, observabilidade e
testes distribuídos.
