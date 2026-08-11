output "kafka_bootstrap_server" {
  value       = "${azurerm_public_ip.pip.ip_address}:9094"
  description = "Kafka external endpoint for producer/consumer scripts."
}

output "sql_server_fqdn" {
  value       = azurerm_mssql_server.sql_server.fully_qualified_domain_name
  description = "Azure SQL Server endpoint for Table 1 and Table 2."
}