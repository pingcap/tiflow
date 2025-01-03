#!/bin/sh

set -ex

cd "$(dirname "$0")"

CONF_PATH=`cd ../../conf && pwd`
CA_PATH="$CONF_PATH/root.crt"
CERT_PATH="$CONF_PATH/client.crt"
KEY_PATH="$CONF_PATH/client.key"
OUT_DIR=/tmp/tidb_tools_test/sync_diff_inspector/output
rm -rf $OUT_DIR
mkdir -p $OUT_DIR

# create user for test tls
mysql -uroot -h 127.0.0.1 -P 4000 -e "create user 'root_tls'@'%' identified by '' require X509;"
mysql -uroot -h 127.0.0.1 -P 4000 -e "grant all privileges on *.* to 'root_tls'@'%';"
mysql -uroot_tls -h 127.0.0.1 -P 4000 --ssl-ca "$CA_PATH" --ssl-cert "$CERT_PATH" --ssl-key "$KEY_PATH"  -e "SHOW STATUS LIKE \"%Ssl%\";"

echo "use sync_diff_inspector to compare data"
# sync diff tidb-tidb
CA_PATH_REG=$(echo ${CA_PATH} | sed 's/\//\\\//g')
CERT_PATH_REG=$(echo ${CERT_PATH} | sed 's/\//\\\//g')
KEY_PATH_REG=$(echo ${KEY_PATH} | sed 's/\//\\\//g')
sed "s/\"ca-path\"#CAPATH/\"${CA_PATH_REG}\"/g" config.toml | sed "s/\"cert-path\"#CERTPATH/\"${CERT_PATH_REG}\"/g" | sed "s/\"key-path\"#KEYPATH/\"${KEY_PATH_REG}\"/g" > config_.toml
sync_diff_inspector --config=./config_.toml > $OUT_DIR/diff.output || (cat $OUT_DIR/diff.output && exit 1)
check_contains "check pass!!!" $OUT_DIR/sync_diff.log
