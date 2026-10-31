# Infraestrutura Execution em HML

State e permissões próprios. Este rascunho usa Cosmos DB NoSQL serverless,
container particionado por `/filialId` e identidade do workload. O custo,
quota, conectividade, RBAC de dados, batch com ETag e limites precisam ser
validados no spike antes de `apply`. Os arquivos de exemplo não são credenciais.
Não usar a chave mestra do Cosmos no workload.
