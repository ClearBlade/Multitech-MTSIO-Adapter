#!/bin/bash

#Copy binary to /usr/local/bin
mv mtsIoAdapter /usr/bin

#Ensure binary is executable
chmod +x /usr/bin/mtsIoAdapter

#Set up init.d resources so that mtsIoAdapter is started when the gateway starts
mv mtsIoAdapter.etc.initd /etc/init.d/mtsIoAdapter
mv mtsIoAdapter.etc.default /etc/default/mtsIoAdapter

#Ensure init.d script is executable
chmod +x /etc/init.d/mtsIoAdapter

#Add the adapter to the startup script
update-rc.d mtsIoAdapter defaults

echo "mtsIoAdapter Deployed"
