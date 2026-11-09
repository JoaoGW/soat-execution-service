terraform {
  required_version = "~> 1.9.8"
  backend "azurerm" {}

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = ">= 4.81.0, < 5.0.0"
    }
  }
}

provider "azurerm" {
  features {}
}

resource "azurerm_cosmosdb_account" "execution" {
  name                         = var.cosmos_account_name
  location                     = var.location
  resource_group_name          = var.resource_group_name
  offer_type                   = "Standard"
  kind                         = "GlobalDocumentDB"
  local_authentication_enabled = false
  minimal_tls_version          = "Tls12"

  capabilities {
    name = "EnableServerless"
  }

  consistency_policy {
    consistency_level = "Session"
  }

  geo_location {
    location          = var.location
    failover_priority = 0
  }
}

resource "azurerm_cosmosdb_sql_database" "execution" {
  name                = "execution_hml"
  resource_group_name = var.resource_group_name
  account_name        = azurerm_cosmosdb_account.execution.name
}

resource "azurerm_cosmosdb_sql_container" "operations" {
  name                  = "operations"
  resource_group_name   = var.resource_group_name
  account_name          = azurerm_cosmosdb_account.execution.name
  database_name         = azurerm_cosmosdb_sql_database.execution.name
  partition_key_paths   = ["/filialId"]
  partition_key_version = 2
}

# Built-in Cosmos DB Data Contributor, scoped to the container; no master key
# is distributed to the workload. The principal comes from the AKS identity.
resource "azurerm_cosmosdb_sql_role_assignment" "runtime" {
  resource_group_name = var.resource_group_name
  account_name        = azurerm_cosmosdb_account.execution.name
  role_definition_id  = "${azurerm_cosmosdb_account.execution.id}/sqlRoleDefinitions/00000000-0000-0000-0000-000000000002"
  principal_id        = var.execution_workload_principal_id
  scope               = azurerm_cosmosdb_sql_container.operations.id
}
