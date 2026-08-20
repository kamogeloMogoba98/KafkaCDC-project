variable "location" {
  default = "South Africa North" # Or your preferred region
}

variable "prefix" {
  default = "kafka-demo"
}

variable "sql_admin_password" {
  type      = string
  sensitive = true
  default   = "" # Change this or inject via environment variables
}
