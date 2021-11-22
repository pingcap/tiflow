#!/usr/bin/env bash
set -euo pipefail

rm -rf /tmp/api-before
cp -r ./api /tmp/api-before

go get github.com/swaggo/gin-swagger@v1.3.2
go get github.com/swaggo/swag/cmd/swag@v1.6.6
swag init --generalInfo ./cdc/http_router.go --output ./api

if [ "$(diff ./api /tmp/api-before | wc -l)" -ne 0 ]; then
	echo "Please run \`make apidoc\` to update cdc http api docs"
	diff ./api /tmp/api-before
	exit 1
fi

echo "http api docs check pass"
