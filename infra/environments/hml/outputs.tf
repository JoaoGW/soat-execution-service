output "cosmos_endpoint" { value = azurerm_cosmosdb_account.execution.endpoint }
output "database_name" { value = azurerm_cosmosdb_sql_database.execution.name }
output "container_name" { value = azurerm_cosmosdb_sql_container.operations.name }
