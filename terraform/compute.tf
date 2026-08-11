resource "azurerm_linux_virtual_machine" "kafka_vm" {
  name                  = "${var.prefix}-kafka-vm"
  location              = azurerm_resource_group.rg.location
  resource_group_name   = azurerm_resource_group.rg.name
  size                  = "Standard_D2s_v5" # Modern v5-generation SKU with active capacity
  admin_username        = "azureuser"
  admin_password        = "P@ssw0rd12345!"
  disable_password_authentication = false
  network_interface_ids = [azurerm_network_interface.nic.id]

  os_disk {
    caching              = "ReadWrite"
    storage_account_type = "Standard_LRS"
  }

  source_image_reference {
    publisher = "Canonical"
    offer     = "0001-com-ubuntu-server-jammy"
    sku       = "22_04-lts"
    version   = "latest"
  }

  custom_data = base64encode(file("${path.module}/sh/user-data.sh"))
}