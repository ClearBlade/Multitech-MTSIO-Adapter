#!/bin/bash

#Remove the init.d script
rm /etc/init.d/mtsIoAdapter

#Remove the default variables file
rm /etc/default/mtsIoAdapter

#Remove mtsIoAdapter from the startup script
update-rc.d -f mtsIoAdapter remove

#Remove the binary
rm /usr/bin/mtsIoAdapter