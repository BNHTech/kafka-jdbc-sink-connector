#!/bin/bash
set -e
mvn clean package kafka-connect:kafka-connect
# VERSION_NUMBER="$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)"
#open ./target/components/packages/
ssh kconnect-1.bnh.vn "rm -f /usr/share/java/connect_plugins/BNHTech-kafka-jdbc-sink-connector-*/lib/kafka-jdbc-sink-connector*"
JAR_REMOTE_PATH="$(ssh kconnect-1.bnh.vn "find /usr/share/java/connect_plugins/BNHTech-kafka-jdbc-sink-connector-*/ -maxdepth 1 -mindepth 1 -type d")"
scp ./target/components/kafka-jdbc-sink-connector*.jar "kconnect-1.bnh.vn:${JAR_REMOTE_PATH}"
# ssh kconnect-1.bnh.vn "unzip /usr/share/java/connect_plugins/BNHTech-kafka-jdbc-sink-connector-${VERSION_NUMBER}.zip -d /usr/share/java/connect_plugins/"
ssh kconnect-1.bnh.vn "systemctl restart confluent-kafka-connect"
