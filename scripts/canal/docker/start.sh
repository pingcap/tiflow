#!/bin/sh

# This script file uses enviroment variable to create config of canal adapter. The format of config file `application.yml`
# is only suitable for canal adapter release version 1.15, if you want build from lastest master branch, please rewrite the
# config file from canal/client-adapter/launcher/src/main/bin/conf/application.yml

KAFKA_SERVER=${KAFKA_SERVER:-localhost:9092}
ZOOKEEPER_SERVER=${ZOOKEEPER_SERVER:-localhost:2181}
DB_NAME=${DB_NAME:-testdb}
DOWNSTREAM_DB_HOST=${DOWNSTREAM_DB_HOST:-localhost}
DOWNSTREAM_DB_PORT=${DOWNSTREAM_DB_PORT:-4000}
echo "zookeeper server ${ZOOKEEPER_SERVER}"
echo "kafka server ${KAFKA_SERVER}"
echo "db name ${DB_NAME}"
echo "downstream db host ${DOWNSTREAM_DB_HOST}"
echo "downstream db port ${DOWNSTREAM_DB_PORT}"

WORK_DIR=$(pwd)
cat - >"${WORK_DIR}/conf/application.yml" <<EOF
server:
  port: 8081
spring:
  jackson:
    date-format: yyyy-MM-dd HH:mm:ss
    time-zone: GMT+8
    default-property-inclusion: non_null

canal.conf:
  mode: kafka #rocketMQ
  zookeeperHosts: ${ZOOKEEPER_SERVER}
  mqServers: ${KAFKA_SERVER} #or rocketmq
  flatMessage: false
  batchSize: 500
  syncBatchSize: 1000
  retries: 0
  timeout:
  accessKey:
  secretKey:
  username:
  password:
  vhost:
  canalAdapters:
  - instance: ${DB_NAME} # canal instance Name or mq topic name
    groups:
    - groupId: g1
      outerAdapters:
      - name: rdb
        key: mysql1
        properties:
          jdbc.driverClassName: com.mysql.jdbc.Driver
          jdbc.url: jdbc:mysql://${DOWNSTREAM_DB_HOST}:${DOWNSTREAM_DB_PORT}/${DB_NAME}
          jdbc.username: root
          jdbc.password:
EOF

cat - >"$WORK_DIR/conf/rdb/mytest_user.yml" <<EOF
# Mirror schema synchronize config
dataSourceKey: defaultDS
destination: ${DB_NAME}
groupId: g1
outerAdapterKey: mysql1
concurrent: true
dbMapping:
  mirrorDb: true
  database: ${DB_NAME}
EOF

bash ./bin/startup.sh

while true; do
  sleep 30000
done
