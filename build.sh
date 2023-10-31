#!/bin/bash

mvn clean package kafka-connect:kafka-connect

scp ./target/components/packages/*.zip kconnect-1.bnh.vn:/usr/share/java/connect_plugins/
scp ./target/components/packages/*.zip kconnect-2.bnh.vn:/usr/share/java/connect_plugins/