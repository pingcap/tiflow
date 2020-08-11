#!/bin/bash
db_name="testdb"
sql="drop database ${db_name}; create database ${db_name};"
host=127.0.0.1
upstream_port=5000
downstream_port=4000
echo "[$(date)] Executing SQL: ${sql}"
mysql -uroot -h${host} -P${upstream_port} --default-character-set utf8mb4 -E -e "${sql}"
mysql -uroot -h${host} -P${downstream_port} --default-character-set utf8mb4 -E -e "${sql}"

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
#  canalServerHost: 127.0.0.1:11111
  zookeeperHosts: slave1:2181
  mqServers: 127.0.0.1:9092 #or rocketmq
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
#      url: jdbc:mysql://127.0.0.1:3306/mytest?useUnicode=true
#      username: root
#      password: 121212
  canalAdapters:
  - instance: ${db_name} # canal instance Name or mq topic name
    groups:
    - groupId: g1
      outerAdapters:
      - name: logger
      - name: rdb
        key: mysql1
        properties:
          jdbc.driverClassName: com.mysql.jdbc.Driver
          jdbc.url: jdbc:mysql://127.0.0.1:5000/${db_name}
          jdbc.username: root
          jdbc.password:
EOF

cat - >"$WORK_DIR/conf/rdb/mytest_user.yml" <<EOF
# Mirror schema synchronize config
dataSourceKey: defaultDS
destination: ${db_name}
groupId: g1
outerAdapterKey: mysql1
concurrent: true
dbMapping:
  mirrorDb: true
  database: ${db_name}
EOF
mkdir ./logs/adapter
bash ./bin/startup.sh

while true; do
  sleep 30000
done