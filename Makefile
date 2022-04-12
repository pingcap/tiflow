TEST_DIR := /tmp/dataflow_engine_test
PARALLEL=3
GOTEST := CGO_ENABLED=1 go test -p $(PARALLEL) --race
FAIL_ON_STDOUT := awk '{ print  } END { if (NR > 0) { exit 1  }  }'

PACKAGE_LIST := go list ./... | grep -vE 'proto|pb' | grep -v 'e2e'
PACKAGES := $$($(PACKAGE_LIST))
GOFILES := $$(find . -name '*.go' -type f | grep -vE 'proto|pb\.go')

all: df-proto build

build: df-master df-executor df-master-client df-demo

df-proto:
	./generate-proto.sh

df-master:
	go build -o bin/master ./cmd/master
	cp ./bin/master ./ansible/roles/common/files/master.bin

df-executor:
	go build -o bin/executor ./cmd/executor
	cp ./bin/executor ./ansible/roles/common/files/executor.bin

df-master-client:
	go build -o bin/master-client ./cmd/master-client

df-demo:
	go build -o bin/demoserver ./cmd/demoserver
	cp ./bin/demoserver ./ansible/roles/common/files/demoserver.bin

unit_test:
	mkdir -p "$(TEST_DIR)"
	$(GOTEST) -cover -covermode=atomic -coverprofile="$(TEST_DIR)/cov.unit.out" $(PACKAGES)

tools_setup:
	@echo "setup build and check tools"
	@cd tools && make

check: tools_setup lint fmt tidy

fmt:
	@echo "gofmt (simplify)"
	tools/bin/gofumports -l -w $(GOFILES) 2>&1 | $(FAIL_ON_STDOUT)
	@echo "run shfmt"
	tools/bin/shfmt -d -w .

tidy:
	@echo "check go mod tidy"
	go mod tidy

lint:
	echo "golangci-lint"; \
	tools/bin/golangci-lint run --config=./.golangci.yml --timeout 10m0s --skip-files "pb"

kvmock: tools_setup
	tools/bin/mockgen github.com/hanfei1991/microcosm/pkg/meta/metaclient KVClient \
	> pkg/meta/kvclient/mock/mockclient.go
