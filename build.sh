#!/bin/bash

mvn clean package kafka-connect:kafka-connect
open ./target/components/packages/
# ssh kconnect-1.bnh.vn "rm -fr /usr/share/java/connect_plugins/BNH*.zip"
# scp ./target/components/packages/*.zip kconnect-1.bnh.vn:/usr/share/java/connect_plugins/