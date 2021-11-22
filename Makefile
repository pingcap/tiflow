TEST_DIR := /tmp/dataflow_engine_test
PARALLEL=3
GOTEST := CGO_ENABLED=1 go test -p $(PARALLEL) --race

PACKAGE_LIST := go list ./... | grep -vE 'proto|pb'
PACKAGES := $$($(PACKAGE_LIST))

all: df-proto df-master df-executor df-master-client producer

df-proto:
	./generate-proto.sh

df-master:
	go build -o bin/master ./cmd/master

df-executor:
	go build -o bin/executor ./cmd/executor

df-master-client:
	go build -o bin/master-client ./cmd/master-client

producer:
	go build -o bin/producer ./cmd/producer

unit_test:
	mkdir -p "$(TEST_DIR)"
	$(GOTEST) -cover -covermode=atomic -coverprofile="$(TEST_DIR)/cov.unit.out" $(PACKAGES)
