#!/bin/bash
echo $0
KAFKA_SERVER=${KAFKA_SERVER:-localhost:9092}
ZOOKEEPER_SERVER=${ZOOKEEPER_SERVER:-localhost:2181}
DB_NAME=${DB_NAME:-testdb}
DOWNSTREAM_DB_HOST=${DOWNSTREAM_DB_HOST:-localhost}
DOWNSTREAM_DB_PORT=${DOWNSTREAM_DB_PORT:-4000}
UPSTREAM_DB_HOST=${UPSTREAM_DB_HOST:-localhost}
UPSTREAM_DB_PORT=${UPSTREAM_DB_PORT:-4000}

echo "zookeeper server ${ZOOKEEPER_SERVER}"
echo "kafka server ${KAFKA_SERVER}"
echo "db name ${DB_NAME}"
echo "upstream db host ${UPSTREAM_DB_HOST}"
echo "upstream db port ${UPSTREAM_DB_PORT}"
echo "downstream db host ${DOWNSTREAM_DB_HOST}"
echo "downstream db port ${DOWNSTREAM_DB_PORT}"

sql="drop database if exists ${DB_NAME}; create database ${DB_NAME};"
echo "[$(date)] Executing SQL: ${sql}"
mysql -uroot -h ${UPSTREAM_DB_HOST} -P ${UPSTREAM_DB_PORT}  -E -e "${sql}"
mysql -uroot -h ${DOWNSTREAM_DB_HOST} -P ${DOWNSTREAM_DB_PORT}   -E -e "${sql}" --protocol=tcp

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
#  canalServerHost: localhost:11111
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
#  srcDataSources:
#    defaultDS:
#      url: jdbc:mysql://localhost:3306/mytest?useUnicode=true
#      username: root
#      password: 121212
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