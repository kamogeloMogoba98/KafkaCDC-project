resource "azurerm_mssql_server" "sql_server" {
  name                         = "${var.prefix}-sql-server"
  resource_group_name          = azurerm_resource_group.rg.name
  location                     = azurerm_resource_group.rg.location
  version                      = "12.0"
  administrator_login          = "sqladmin"
  administrator_login_password = var.sql_admin_password
}

resource "azurerm_mssql_database" "db" {
  name      = "TransactionAnalyticsDB"
  server_id = azurerm_mssql_server.sql_server.id
  sku_name  = "S0"
}