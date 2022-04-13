all: failpoint-ctl golangci-lint mockgen \
	protoc-gen-gogofaster protoc-gen-grpc-gateway gofumports shfmt goimports

failpoint-ctl:
	go build -o bin/$@ github.com/pingcap/failpoint/failpoint-ctl

golangci-lint:
	go build -o bin/$@ github.com/golangci/golangci-lint/cmd/golangci-lint

mockgen:
	go build -o bin/$@ github.com/golang/mock/mockgen

protoc-gen-gogofaster:
	go build -o bin/$@ github.com/gogo/protobuf/protoc-gen-gogofaster

protoc-gen-grpc-gateway:
	go build -o bin/$@ github.com/grpc-ecosystem/grpc-gateway/protoc-gen-grpc-gateway

gofumports:
	go build -o bin/$@ mvdan.cc/gofumpt

shfmt:
	go build -o bin/$@ mvdan.cc/sh/v3/cmd/shfmt

goimports:
	go build -o bin/$@ golang.org/x/tools/cmd/goimports
