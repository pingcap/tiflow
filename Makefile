### Makefile for ticdc
.PHONY: build test check clean fmt cdc kafka_consumer coverage \
	integration_test_build integration_test integration_test_mysql integration_test_kafka

PROJECT=tiflow

FAIL_ON_STDOUT := awk '{ print  } END { if (NR > 0) { exit 1  }  }'

CURDIR := $(shell pwd)
path_to_add := $(addsuffix /bin,$(subst :,/bin:,$(GOPATH)))
export PATH := $(path_to_add):$(PATH)

TEST_DIR := /tmp/tidb_cdc_test
SHELL	 := /usr/bin/env bash

GO       := GO111MODULE=on go
ifeq (${CDC_ENABLE_VENDOR}, 1)
GOVENDORFLAG := -mod=vendor
endif

GOBUILD  := CGO_ENABLED=0 $(GO) build $(BUILD_FLAG) -trimpath $(GOVENDORFLAG)
GOBUILDNOVENDOR  := CGO_ENABLED=0 $(GO) build $(BUILD_FLAG) -trimpath
ifeq ($(GOVERSION114), 1)
GOTEST   := CGO_ENABLED=1 $(GO) test -p 3 --race -gcflags=all=-d=checkptr=0
else
GOTEST   := CGO_ENABLED=1 $(GO) test -p 3 --race
endif
GOVERSIONGE116 := $(shell expr $$(go version|cut -f3 -d' '|tr -d "go"|cut -f2 -d.) \>= 16)

ARCH  := "`uname -s`"
LINUX := "Linux"
MAC   := "Darwin"
PACKAGE_LIST := go list ./...| grep -vE 'vendor|proto|ticdc\/tests|integration|testing_utils'
PACKAGES  := $$($(PACKAGE_LIST))
PACKAGE_DIRECTORIES := $(PACKAGE_LIST) | sed 's|github.com/pingcap/$(PROJECT)/||'
FILES := $$(find . -name '*.go' -type f | grep -vE 'vendor|kv_gen|proto')
TEST_FILES := $$(find . -name '*_test.go' -type f | grep -vE 'vendor|kv_gen|integration|testing_utils')
CDC_PKG := github.com/pingcap/tiflow
FAILPOINT_DIR := $$(for p in $(PACKAGES); do echo $${p\#"github.com/pingcap/$(PROJECT)/"}|grep -v "github.com/pingcap/$(PROJECT)"; done)
FAILPOINT := bin/failpoint-ctl

FAILPOINT_ENABLE  := $$(echo $(FAILPOINT_DIR) | xargs $(FAILPOINT) enable >/dev/null)
FAILPOINT_DISABLE := $$(find $(FAILPOINT_DIR) | xargs $(FAILPOINT) disable >/dev/null)

RELEASE_VERSION =
ifeq ($(RELEASE_VERSION),)
	RELEASE_VERSION := v5.0.0-master
	release_version_regex := ^v5\..*$$
	release_branch_regex := "^release-[0-9]\.[0-9].*$$|^HEAD$$|^.*/*tags/v[0-9]\.[0-9]\..*$$"
	ifneq ($(shell git rev-parse --abbrev-ref HEAD | egrep $(release_branch_regex)),)
		# If we are in release branch, try to use tag version.
		ifneq ($(shell git describe --tags --dirty | egrep $(release_version_regex)),)
			RELEASE_VERSION := $(shell git describe --tags --dirty)
		endif
	else ifneq ($(shell git status --porcelain),)
		# Add -dirty if the working tree is dirty for non release branch.
		RELEASE_VERSION := $(RELEASE_VERSION)-dirty
	endif
endif

LDFLAGS += -X "$(CDC_PKG)/pkg/version.ReleaseVersion=$(RELEASE_VERSION)"
LDFLAGS += -X "$(CDC_PKG)/pkg/version.BuildTS=$(shell date -u '+%Y-%m-%d %H:%M:%S')"
LDFLAGS += -X "$(CDC_PKG)/pkg/version.GitHash=$(shell git rev-parse HEAD)"
LDFLAGS += -X "$(CDC_PKG)/pkg/version.GitBranch=$(shell git rev-parse --abbrev-ref HEAD)"
LDFLAGS += -X "$(CDC_PKG)/pkg/version.GoVersion=$(shell go version)"

default: build buildsucc

buildsucc:
	@echo Build TiDB CDC successfully!

all: dev install

dev: check test

test: unit_test

build: cdc

build-failpoint:
	$(FAILPOINT_ENABLE)
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/cdc ./cmd/cdc/main.go
	$(FAILPOINT_DISABLE)

cdc:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/cdc ./cmd/cdc/main.go

kafka_consumer:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/cdc_kafka_consumer ./cmd/kafka-consumer/main.go

install:
	go install ./...

unit_test: check_failpoint_ctl
	./scripts/fix_lib_zstd.sh
	mkdir -p "$(TEST_DIR)"
	$(FAILPOINT_ENABLE)
	@export log_level=error;\
	$(GOTEST) -cover -covermode=atomic -coverprofile="$(TEST_DIR)/cov.unit.out" $(PACKAGES) \
	|| { $(FAILPOINT_DISABLE); exit 1; }
	$(FAILPOINT_DISABLE)

leak_test: check_failpoint_ctl
	./scripts/fix_lib_zstd.sh
	$(FAILPOINT_ENABLE)
	@export log_level=error;\
	$(GOTEST) -count=1 --tags leak $(PACKAGES) || { $(FAILPOINT_DISABLE); exit 1; }
	$(FAILPOINT_DISABLE)

check_failpoint_ctl:
ifeq "$(GOVERSIONGE116)" "1"
	# use -mod=mod to avoid error: missing go.sum entry for module providing package
	# ref: https://github.com/golang/go/issues/44129
	which $(FAILPOINT) >/dev/null 2>&1 || $(GOBUILDNOVENDOR) -mod=mod -o $(FAILPOINT) github.com/pingcap/failpoint/failpoint-ctl && go mod tidy
else
	which $(FAILPOINT) >/dev/null 2>&1 || $(GOBUILDNOVENDOR) -o $(FAILPOINT) github.com/pingcap/failpoint/failpoint-ctl
endif

check_third_party_binary:
	@which bin/tidb-server
	@which bin/tikv-server
	@which bin/pd-server
	@which bin/tiflash
	@which bin/pd-ctl
	@which bin/sync_diff_inspector
	@which bin/go-ycsb
	@which bin/etcdctl
	@which bin/jq
	@which bin/minio

integration_test_build: check_failpoint_ctl
	./scripts/fix_lib_zstd.sh
	$(FAILPOINT_ENABLE)
	$(GOTEST) -ldflags '$(LDFLAGS)' -c -cover -covermode=atomic \
		-coverpkg=github.com/pingcap/tiflow/... \
		-o bin/cdc.test github.com/pingcap/tiflow/cmd/cdc \
	|| { $(FAILPOINT_DISABLE); exit 1; }
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/cdc ./cmd/cdc/main.go \
	|| { $(FAILPOINT_DISABLE); exit 1; }
	$(FAILPOINT_DISABLE)

integration_test: integration_test_mysql

integration_test_mysql:
	tests/integration_tests/run.sh mysql "$(CASE)"

integration_test_kafka: check_third_party_binary
	tests/integration_tests/run.sh kafka "$(CASE)"

fmt: tools/bin/gofumports tools/bin/shfmt
	@echo "gofmt (simplify)"
	tools/bin/gofumports -s -l -w $(FILES) 2>&1 | $(FAIL_ON_STDOUT)
	@echo "run shfmt"
	tools/bin/shfmt -d -w .

lint: tools/bin/revive
	@echo "linting"
	@tools/bin/revive -formatter friendly -config tools/check/revive.toml $(FILES)

errdoc: tools/bin/errdoc-gen
	@echo "generator errors.toml"
	./tools/check/check-errdoc.sh

check-copyright:
	@echo "check-copyright"
	@./scripts/check-copyright.sh

check-merge-conflicts:
	@echo "check-merge-conflicts"
	@./scripts/check-merge-conflicts.sh

check-leaktest-added: tools/bin/gofumports
	@echo "check leak test added in all unit tests"
	./scripts/add-leaktest.sh $(TEST_FILES)

vet:
	@echo "vet"
	$(GO) vet $(PACKAGES) 2>&1 | $(FAIL_ON_STDOUT)

tidy:
	@echo "go mod tidy"
	./tools/check/check-tidy.sh

check: check-copyright fmt lint check-static tidy errdoc check-leaktest-added check-merge-conflicts

integration_test_coverage:
	GO111MODULE=off go get github.com/wadey/gocovmerge
	gocovmerge "$(TEST_DIR)"/cov.* | grep -vE ".*.pb.go|$(CDC_PKG)/testing_utils/.*|$(CDC_PKG)/cdc/kv/testing.go|$(CDC_PKG)/cdc/entry/schema_test_helper.go|$(CDC_PKG)/cdc/sink/simple_mysql_tester.go|.*.__failpoint_binding__.go" > "$(TEST_DIR)/all_cov.out"
ifeq ("$(JenkinsCI)", "1")
	GO111MODULE=off go get github.com/mattn/goveralls
	@goveralls -coverprofile=$(TEST_DIR)/all_cov.out -service=jenkins-ci -repotoken $(COVERALLS_TOKEN)
else
	go tool cover -html "$(TEST_DIR)/all_cov.out" -o "$(TEST_DIR)/all_cov.html"
endif

unit_test_coverage:
	grep -vE ".*.pb.go|$(CDC_PKG)/testing_utils/.*|$(CDC_PKG)/cdc/kv/testing.go|$(CDC_PKG)/cdc/sink/simple_mysql_tester.go|.*.__failpoint_binding__.go" "$(TEST_DIR)/cov.unit.out" > "$(TEST_DIR)/unit_cov.out"
ifeq ("$(JenkinsCI)", "1")
	@bash <(curl -s https://codecov.io/bash) -f $(TEST_DIR)/unit_cov.out -t $(CODECOV_TOKEN)
else
	go tool cover -html "$(TEST_DIR)/unit_cov.out" -o "$(TEST_DIR)/unit_cov.html"
	go tool cover -func="$(TEST_DIR)/unit_cov.out"
endif

check-static: tools/bin/golangci-lint
	tools/bin/golangci-lint run --timeout 10m0s --skip-files kv_gen

data-flow-diagram: docs/data-flow.dot
	dot -Tsvg docs/data-flow.dot > docs/data-flow.svg

clean:
	go clean -i ./...
	rm -rf *.out
	rm -f bin/cdc
	rm -f bin/cdc_kafka_consumer

tools/bin/gofumports: tools/check/go.mod
	cd tools/check; test -e ../bin/gofumports || \
	$(GO) build -o ../bin/gofumports mvdan.cc/gofumpt

tools/bin/revive: tools/check/go.mod
	cd tools/check; test -e ../bin/revive || \
	$(GO) build -o ../bin/revive github.com/mgechev/revive

tools/bin/errdoc-gen: tools/check/go.mod
	cd tools/check; test -e ../bin/errdoc-gen || \
	$(GO) build -o ../bin/errdoc-gen github.com/pingcap/errors/errdoc-gen

tools/bin/golangci-lint:
	cd tools/check; test -e ../bin/golangci-lint || \
	curl -sfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh| sh -s -- -b ../bin v1.30.0

tools/bin/shfmt: tools/check/go.mod
	cd tools/check; test -e ../bin/shfmt || \
	$(GO) build -o ../bin/shfmt mvdan.cc/sh/v3/cmd/shfmt

failpoint-enable: check_failpoint_ctl
	$(FAILPOINT_ENABLE)

failpoint-disable: check_failpoint_ctl
	$(FAILPOINT_DISABLE)
