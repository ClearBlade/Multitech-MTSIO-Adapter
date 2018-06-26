Instructions for use:

1. Copy mtsIoAdapter.etc.default file into /etc/default, name the file "mtsIoAdapter"
2. Copy mtsIoAdapter.etc.initd file into /etc/init.d, name the file "mtsIoAdapter"
3. From a terminal prompt, execute the following commands:
	3a. chmod 755 /etc/init.d/mtsIoAdapter
	3b. chown root:root /etc/init.d/mtsIoAdapter
	3c. update-rc.d mtsIoAdapter defaults

If you wish to start the adapter, rather than reboot, issue the following command from a terminal prompt:

	/etc/init.d/mtsIoAdapter start