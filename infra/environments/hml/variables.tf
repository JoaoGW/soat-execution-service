variable "resource_group_name" { type = string }
variable "cosmos_account_name" { type = string }
variable "execution_workload_principal_id" { type = string }
variable "location" {
  type    = string
  default = "brazilsouth"
}
