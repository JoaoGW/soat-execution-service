# Infraestrutura Execution em HML

State e permissões próprios do Execution Service. Provisiona o banco NoSQL do
serviço:

- Cosmos DB for NoSQL serverless (`local_authentication_enabled=false`, TLS 1.2);
- banco `execution_hml` e container `operations` particionado por `/filialId`;
- papel interno *Cosmos DB Built-in Data Contributor* atribuído somente à
  identidade de workload do Execution e com escopo no container
  (`<account>/dbs/execution_hml/colls/operations`). A chave mestra nunca é
  distribuída ao workload.

## Pipeline

O workflow `.github/workflows/infra.yml` roda `terraform fmt`, `init
-backend=false` e `validate` em todo PR e push que altere `infra/`. O `apply`
em HML só acontece em push para `development` (ou disparo manual) quando a
variável `TF_APPLY_ENABLED` do Environment `hml` for `true` e a identidade OIDC
estiver configurada; sem a trava, o job registra o bloqueio e não tenta login
no Azure.

## Uso local

```bash
cp backend.hcl.example backend.hcl
cp terraform.tfvars.example terraform.tfvars
terraform init -backend-config=backend.hcl
terraform plan
```

Os arquivos de exemplo não são credenciais. O `execution_workload_principal_id`
é o `principal_id` da identidade `execution` exportado pelo módulo
`fase4-messaging` do `soat-aks-infra`.
