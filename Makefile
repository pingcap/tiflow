### Makefile for ticdc
.PHONY: build test check clean fmt cdc kafka_consumer coverage \
	integration_test_build integration_test integration_test_mysql integration_test_kafka

PROJECT=ticdc

FAIL_ON_STDOUT := awk '{ print  } END { if (NR > 0) { exit 1  }  }'

CURDIR := $(shell pwd)
path_to_add := $(addsuffix /bin,$(subst :,/bin:,$(GOPATH)))
export PATH := $(path_to_add):$(PATH)

TEST_DIR := /tmp/tidb_cdc_test
SHELL	 := /usr/bin/env bash

GO       := GO111MODULE=on go
GOBUILD  := CGO_ENABLED=0 $(GO) build $(BUILD_FLAG) -trimpath
GOTEST   := CGO_ENABLED=1 $(GO) test -p 3 --race

ARCH  := "`uname -s`"
LINUX := "Linux"
MAC   := "Darwin"
PACKAGE_LIST := go list ./...| grep -vE 'vendor|proto|ticdc\/tests'
PACKAGES  := $$($(PACKAGE_LIST))
PACKAGE_DIRECTORIES := $(PACKAGE_LIST) | sed 's|github.com/pingcap/$(PROJECT)/||'
FILES := $$(find . -name '*.go' -type f | grep -vE 'vendor')
CDC_PKG := github.com/pingcap/ticdc
FAILPOINT_DIR := $$(for p in $(PACKAGES); do echo $${p\#"github.com/pingcap/$(PROJECT)/"}|grep -v "github.com/pingcap/$(PROJECT)"; done)
FAILPOINT := bin/failpoint-ctl

FAILPOINT_ENABLE  := $$(echo $(FAILPOINT_DIR) | xargs $(FAILPOINT) enable >/dev/null)
FAILPOINT_DISABLE := $$(find $(FAILPOINT_DIR) | xargs $(FAILPOINT) disable >/dev/null)

LDFLAGS += -X "$(CDC_PKG)/pkg/util.BuildTS=$(shell date -u '+%Y-%m-%d %H:%M:%S')"
LDFLAGS += -X "$(CDC_PKG)/pkg/util.GitHash=$(shell git rev-parse HEAD)"
LDFLAGS += -X "$(CDC_PKG)/pkg/util.ReleaseVersion=$(shell git describe --tags --dirty="-dev")"
LDFLAGS += -X "$(CDC_PKG)/pkg/util.GitBranch=$(shell git rev-parse --abbrev-ref HEAD)"
LDFLAGS += -X "$(CDC_PKG)/pkg/util.GoVersion=$(shell go version)"

default: build buildsucc

buildsucc:
	@echo Build TiDB CDC successfully!

all: dev install

dev: check test

test: unit_test

build: cdc

cdc:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/cdc ./main.go

kafka_consumer:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/cdc_kafka_consumer ./kafka_consumer/main.go

install:
	go install ./...

unit_test: check_failpoint_ctl
	mkdir -p "$(TEST_DIR)"
	$(FAILPOINT_ENABLE)
	@export log_level=error;\
	$(GOTEST) -cover -covermode=atomic -coverprofile="$(TEST_DIR)/cov.unit.out" $(PACKAGES) \
	|| { $(FAILPOINT_DISABLE); exit 1; }
	$(FAILPOINT_DISABLE)

check_failpoint_ctl:
	which $(FAILPOINT) >/dev/null 2>&1 || $(GOBUILD) -o $(FAILPOINT) github.com/pingcap/failpoint/failpoint-ctl

check_third_party_binary:
	@which bin/tidb-server
	@which bin/tikv-server
	@which bin/pd-server
	@which bin/pd-ctl
	@which bin/sync_diff_inspector
	@which bin/go-ycsb

integration_test_build: check_failpoint_ctl
	$(FAILPOINT_ENABLE)
	$(GOTEST) -c -cover -covemode=atomic \
		-coverpkg=github.com/pingcap/ticdc/... \
		-o bin/cdc.test github.com/pingcap/ticdc \
	|| { $(FAILPOINT_DISABLE); exit 1; }
	$(FAILPOINT_DISABLE)

integration_test: integration_test_mysql

integration_test_mysql: check_third_party_binary
	tests/run.sh $(CASE) mysql

integration_test_kafka: check_third_party_binary
	tests/run.sh $(CASE) kafka

fmt:
	@echo "gofmt (simplify)"
	@gofmt -s -l -w $(FILES) 2>&1 | $(FAIL_ON_STDOUT)

lint:tools/bin/revive
	@echo "linting"
	@tools/bin/revive -formatter friendly -config tools/check/revive.toml $(FILES)

vet:
	@echo "vet"
	$(GO) vet $(PACKAGES) 2>&1 | $(FAIL_ON_STDOUT)

tidy:
	@echo "go mod tidy"
	./tools/check/check-tidy.sh

check: fmt lint check-static tidy

coverage:
	GO111MODULE=off go get github.com/zhouqiang-cl/gocovmerge
	gocovmerge "$(TEST_DIR)"/cov.* | grep -vE "$(CDC_PKG)/cdc/kv/testing.go|.*.__failpoint_binding__.go" > "$(TEST_DIR)/all_cov.out"
	grep -vE "$(CDC_PKG)/cdc/kv/testing.go|.*.__failpoint_binding__.go" "$(TEST_DIR)/cov.unit.out" > "$(TEST_DIR)/unit_cov.out"
ifeq ("$(JenkinsCI)", "1")
	GO111MODULE=off go get github.com/mattn/goveralls
	@goveralls -coverprofile=$(TEST_DIR)/all_cov.out -service=jenkins-ci -repotoken $(COVERALLS_TOKEN)
	@bash <(curl -s https://codecov.io/bash) -f $(TEST_DIR)/unit_cov.out -t $(CODECOV_TOKEN)
else
	go tool cover -html "$(TEST_DIR)/all_cov.out" -o "$(TEST_DIR)/all_cov.html"
	go tool cover -html "$(TEST_DIR)/unit_cov.out" -o "$(TEST_DIR)/unit_cov.html"
	go tool cover -func="$(TEST_DIR)/unit_cov.out"
endif

check-static: tools/bin/golangci-lint
	$(GO) mod vendor
	tools/bin/golangci-lint \
		run ./... # $$($(PACKAGE_DIRECTORIES))

clean:
	go clean -i ./...
	rm -rf *.out

tools/bin/revive: tools/check/go.mod
	cd tools/check; \
	$(GO) build -o ../bin/revive github.com/mgechev/revive

tools/bin/golangci-lint: tools/check/go.mod
	cd tools/check; \
	$(GO) build -o ../bin/golangci-lint github.com/golangci/golangci-lint/cmd/golangci-lint

failpoint-enable:
	$(FAILPOINT_ENABLE)

failpoint-disable:
	$(FAILPOINT_DISABLE)
