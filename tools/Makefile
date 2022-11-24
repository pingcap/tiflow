tools/bin/failpoint-ctl: tools/check/go.mod
	cd tools/check && $(GO) build -mod=mod -o ../bin/failpoint-ctl github.com/pingcap/failpoint/failpoint-ctl

tools/bin/gocovmerge: tools/check/go.mod
	cd tools/check && $(GO) build -mod=mod -o ../bin/gocovmerge github.com/zhouqiang-cl/gocovmerge

tools/bin/goveralls: tools/check/go.mod
	cd tools/check && $(GO) build -mod=mod -o ../bin/goveralls github.com/mattn/goveralls

tools/bin/golangci-lint: tools/check/go.mod
	cd tools/check && $(GO) build -mod=mod -o ../bin/golangci-lint github.com/golangci/golangci-lint/cmd/golangci-lint

tools/bin/mockgen: tools/check/go.mod
	cd tools/check && $(GO) build -mod=mod -o ../bin/mockgen github.com/golang/mock/mockgen

tools/bin/protoc-gen-go: tools/check/go.mod
	cd tools/check && $(GO) build -mod=mod -o ../bin/protoc-gen-go google.golang.org/protobuf/cmd/protoc-gen-go

tools/bin/protoc-gen-go-grpc: tools/check/go.mod
	cd tools/check && $(GO) build -mod=mod -o ../bin/protoc-gen-go-grpc google.golang.org/grpc/cmd/protoc-gen-go-grpc

tools/bin/protoc-gen-gogofaster: tools/check/go.mod
	cd tools/check && $(GO) build -mod=mod -o ../bin/protoc-gen-gogofaster github.com/gogo/protobuf/protoc-gen-gogofaster

tools/bin/protoc-gen-grpc-gateway: tools/check/go.mod
	cd tools/check && $(GO) build -mod=mod -o ../bin/protoc-gen-grpc-gateway github.com/grpc-ecosystem/grpc-gateway/protoc-gen-grpc-gateway

tools/bin/protoc-gen-grpc-gateway-v2: tools/check/go.mod
	cd tools/check && $(GO) build -mod=mod -o ../bin/protoc-gen-grpc-gateway-v2 github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-grpc-gateway

tools/bin/gofumports: tools/check/go.mod
	cd tools/check && $(GO) build -mod=mod -o ../bin/gofumports mvdan.cc/gofumpt

tools/bin/shfmt: tools/check/go.mod
	cd tools/check && $(GO) build -mod=mod -o ../bin/shfmt mvdan.cc/sh/v3/cmd/shfmt

tools/bin/oapi-codegen: tools/check/go.mod
	cd tools/check && $(GO) build -mod=mod -o ../bin/oapi-codegen github.com/deepmap/oapi-codegen/cmd/oapi-codegen

tools/bin/gocov: tools/check/go.mod
	cd tools/check && $(GO) build -mod=mod -o ../bin/gocov  github.com/axw/gocov/gocov

tools/bin/gocov-xml: tools/check/go.mod
	cd tools/check && $(GO) build -mod=mod -o ../bin/gocov-xml github.com/AlekSi/gocov-xml

tools/bin/gotestsum: tools/check/go.mod
	cd tools/check && $(GO) build -mod=mod -o ../bin/gotestsum gotest.tools/gotestsum

tools/bin/errdoc-gen: tools/check/go.mod
	cd tools/check && $(GO) build -mod=mod -o ../bin/errdoc-gen github.com/pingcap/errors/errdoc-gen

tools/bin/swag: tools/check/go.mod
	cd tools/check && $(GO) build -mod=mod -o ../bin/swag github.com/swaggo/swag/cmd/swag

tools/bin/msgp: tools/check/go.mod
	cd tools/check && $(GO) build -mod=mod -o ../bin/msgp github.com/tinylib/msgp

tools/bin/protoc:
	./scripts/download-protoc.sh

tools/bin/goimports: tools/check/go.mod
	cd tools/check && $(GO) build -mod=mod -o ../bin/goimports golang.org/x/tools/cmd/goimports

tools/bin/protoc-gen-openapiv2: tools/check/go.mod
	cd tools/check && $(GO) build -mod=mod -o ../bin/protoc-gen-openapiv2 github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-openapiv2

tools/bin/gci: tools/check/go.mod
	cd tools/check && $(GO) build -mod=mod -o ../bin/gci github.com/daixiang0/gci

tools/bin/stringer: tools/check/go.mod
	cd tools/check && $(GO) build -mod=mod -o ../bin/stringer golang.org/x/tools/cmd/stringer

tools/bin/mockery: tools/check/go.mod
	cd tools/check && $(GO) build -mod=mod -o ../bin/mockery github.com/vektra/mockery/v2
