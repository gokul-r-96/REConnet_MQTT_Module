#!/bin/bash

HOST=""
if [ -z "$1" ]
then
        echo "Usage : do_ftp.sh <IP>"
        exit 1
else
        # echo "$1"
        HOST=$1
        echo "HOST IP : $HOST"
fi

# HOST=192.168.10.123
USER=root
PASSWORD=softel
ftp -inv $HOST <<EOF
user $USER $PASSWORD
cd /home/root
put re_mqtt_proc
chmod 777 re_mqtt_proc
bye
EOF
