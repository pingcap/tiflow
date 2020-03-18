#!/bin/bash

set -eu

OUT_DIR=/tmp/tidb_cdc_test
CUR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
export PATH=$PATH:$CUR/_utils:$CUR/../bin

mkdir -p $OUT_DIR || true

if [ "${1-}" = '--debug' ]; then
    WORK_DIR=$OUT_DIR/debug
    trap stop_tidb_cluster EXIT

    rm -rf $WORK_DIR && mkdir -p $WORK_DIR

    PATH="$CUR/../bin:$CUR/_utils:$PATH" \
    OUT_DIR=$OUT_DIR \
    TEST_NAME="debug" \
    start_tidb_cluster $WORK_DIR

    cdc server --log-file $WORK_DIR/cdc.log --log-level debug --status-addr 0.0.0.0:8300 > $WORK_DIR/stdout.log 2>&1 &
    sleep 1
    cdc cli changefeed create --sink-uri="mysql://root@127.0.0.1:3306/"

    echo 'You may now debug from another terminal. Press [ENTER] to exit.'
    read line
    exit 0
fi

generate_tls_keys() {
    # Ref: https://docs.microsoft.com/en-us/azure/application-gateway/self-signed-certificates
    # gRPC only supports P-256 curves, see https://github.com/grpc/grpc/issues/6722
    echo "Generate TLS keys..."
    target="$OUT_DIR/tls
    mkdir -p $target || true

    cat - > "$target/ipsan.cnf" <<EOF
[dn]
CN = localhost
[req]
distinguished_name = dn
[EXT]
subjectAltName = @alt_names
keyUsage = digitalSignature,keyEncipherment
extendedKeyUsage = clientAuth,serverAuth
[alt_names]
DNS.1 = localhost
IP.1 = 127.0.0.1
EOF
    openssl ecparam -out "$target/ca.key" -name prime256v1 -genkey
    openssl req -new -batch -sha256 -subj '/CN=localhost' -key "$target/ca.key" -out "$target/ca.csr"
    openssl x509 -req -sha256 -days 2 -in "$target/ca.csr" -signkey "$target/ca.key" -out "$target/ca.pem" 2> /dev/null

    for name in tidb pd tikv cdc cli curl; do
        openssl ecparam -out "$target/$name.key" -name prime256v1 -genkey
        openssl req -new -batch -sha256 -subj '/CN=localhost' -key "$target/$name.key" -out "$target/$name.csr"
        openssl x509 -req -sha256 -days 1 -extensions EXT -extfile "$target/ipsan.cnf" -in "$target/$name.csr" -CA "$target/ca.pem" -CAkey "$target/ca.key" -CAcreateserial -out "$target/$name.pem" 2> /dev/null
    done
}

run_case() {
    local case=$1
    local script=$2
    echo "Running test $script..."
    PATH="$CUR/../bin:$CUR/_utils:$PATH" \
    OUT_DIR=$OUT_DIR \
    TEST_NAME=$case \
    bash "$script"
}

if [ "$#" -ge 1 ]; then
    test_case=$1
else
    test_case="*"
fi

generate_tls_keys
if [ "$test_case" == "*" ]; then
    for script in $CUR/*/run.sh; do
        test_name="$(basename "$(dirname "$script")")"
        run_case $test_name $script
    done
else
    for name in $test_case; do
        script="$CUR/$name/run.sh"
        run_case $name $script
    done
fi

# with color
echo "\033[0;36m<<< Run all test success >>>\033[0m"
