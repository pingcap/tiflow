LDFLAGS += -X "github.com/pingcap/dm/pkg/utils.ReleaseVersion=$(shell git describe --tags --dirty="-dev")"
LDFLAGS += -X "github.com/pingcap/dm/pkg/utils.BuildTS=$(shell date -u '+%Y-%m-%d %H:%M:%S')"
LDFLAGS += -X "github.com/pingcap/dm/pkg/utils.GitHash=$(shell git rev-parse HEAD)"
LDFLAGS += -X "github.com/pingcap/dm/pkg/utils.GitBranch=$(shell git rev-parse --abbrev-ref HEAD)"
LDFLAGS += -X "github.com/pingcap/dm/pkg/utils.GoVersion=$(shell go version)"

CURDIR   := $(shell pwd)
GO       := GO111MODULE=on go
GOBUILD  := CGO_ENABLED=0 $(GO) build
GOTEST   := CGO_ENABLED=1 $(GO) test -gcflags="all=-N -l"
PACKAGE_NAME := github.com/pingcap/dm
PACKAGES  := $$(go list ./... | grep -vE 'tests|cmd|vendor|pb|pbmock|_tools')
PACKAGE_DIRECTORIES := $$(echo "$(PACKAGES)" | sed 's/github.com\/pingcap\/dm\/*//')
PACKAGES_RELAY := $$(go list ./... | grep 'github.com/pingcap/dm/relay')
PACKAGES_SYNCER := $$(go list ./... | grep 'github.com/pingcap/dm/syncer')
PACKAGES_PKG_BINLOG := $$(go list ./... | grep 'github.com/pingcap/dm/pkg/binlog')
PACKAGES_OTHERS  := $$(go list ./... | grep -vE 'tests|cmd|vendor|pbmock|_tools|github.com/pingcap/dm/relay|github.com/pingcap/dm/syncer|github.com/pingcap/dm/pkg/binlog')
FILES    := $$(find . -name "*.go" | grep -vE "vendor|_tools")
TOPDIRS  := $$(ls -d */ | grep -vE "vendor|_tools")
SHELL    := /usr/bin/env bash
TEST_DIR := /tmp/dm_test
FAILPOINT_DIR := $$(for p in $(PACKAGES); do echo $${p\#"github.com/pingcap/dm/"}; done)
FAILPOINT := tools/bin/failpoint-ctl

RACE_FLAG =
TEST_RACE_FLAG = -race
ifeq ("$(WITH_RACE)", "1")
	RACE_FLAG = -race
	GOBUILD   = CGO_ENABLED=1 $(GO) build
endif
ifeq ("$(WITH_RACE)", "0")
	TEST_RACE_FLAG =
	GOTEST         = CGO_ENABLED=0 $(GO) test
endif

FAILPOINT_ENABLE  := $$(echo $(FAILPOINT_DIR) | xargs $(FAILPOINT) enable >/dev/null)
FAILPOINT_DISABLE := $$(find $(FAILPOINT_DIR) | xargs $(FAILPOINT) disable >/dev/null)

ARCH      := "$(shell uname -s)"
LINUX     := "Linux"
MAC       := "Darwin"

ifeq ($(ARCH), $(LINUX))
	LDFLAGS += -X "github.com/pingcap/dm/dm/worker.SampleConfigFile=$(shell cat dm/worker/dm-worker.toml | base64 -w 0)"
	LDFLAGS += -X "github.com/pingcap/dm/dm/master.SampleConfigFile=$(shell cat dm/master/dm-master.toml | base64 -w 0)"
	LDFLAGS += -X "github.com/pingcap/dm/dm/config.SampleConfigFile=$(shell cat dm/master/source.yaml | base64 -w 0)"
else
	LDFLAGS += -X "github.com/pingcap/dm/dm/worker.SampleConfigFile=$(shell cat dm/worker/dm-worker.toml | base64)"
	LDFLAGS += -X "github.com/pingcap/dm/dm/master.SampleConfigFile=$(shell cat dm/master/dm-master.toml | base64)"
	LDFLAGS += -X "github.com/pingcap/dm/dm/config.SampleConfigFile=$(shell cat dm/master/source.yaml | base64)"
endif

.PHONY: build tools_setup test unit_test dm_integration_test_build integration_test \
	coverage check dm-worker dm-master chaos-case dmctl debug-tools

build: check dm-worker dm-master dmctl dm-portal dm-syncer

dm-worker:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/dm-worker ./cmd/dm-worker

dm-master:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/dm-master ./cmd/dm-master

chaos-case:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/chaos-case ./chaos/cases

dmctl:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/dmctl ./cmd/dm-ctl

dm-portal:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/dm-portal ./cmd/dm-portal

dm-syncer:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/dm-syncer ./cmd/dm-syncer

debug-tools:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/binlog-event-blackhole ./debug-tools/binlog-event-blackhole

dm-portal-frontend:
	# TODO: build frontend
	tools/bin/statik -src=./dm/portal/frontend/build -dest=./dm/portal/

tools_setup:
	@echo "setup tools"
	@cd tools && make

install_test_python_dep:
	@echo "install python requirments for test"
	pip install --user -r tests/requirements.txt

generate_proto: tools_setup
	./generate-dm.sh

generate_mock: tools_setup
	./tests/generate-mock.sh

generate_openapi: tools_setup
	@echo "generate_openapi"
	tools/bin/oapi-codegen --config=openapi/spec/server-gen-cfg.yaml openapi/spec/dm.yaml
	tools/bin/oapi-codegen --config=openapi/spec/types-gen-cfg.yaml openapi/spec/dm.yaml

test: unit_test integration_test

define run_unit_test
	@echo "running unit test for packages:" $(1)
	mkdir -p $(TEST_DIR)
	$(FAILPOINT_ENABLE)
	@export log_level=error; \
	$(GOTEST)  -timeout 5m -covermode=atomic -coverprofile="$(TEST_DIR)/cov.$(2).out" $(TEST_RACE_FLAG) $(1) \
	|| { $(FAILPOINT_DISABLE); exit 1; }
	$(FAILPOINT_DISABLE)
endef

define run_unit_test_in_verify_ci
	@echo "running unit test for packages:" $(1)
	mkdir -p $(TEST_DIR)
	$(FAILPOINT_ENABLE)
	@export log_level=error; \
	$(GOTEST)  -timeout 5m -covermode=atomic -coverprofile="$(TEST_DIR)/cov.$(2).out" $(TEST_RACE_FLAG) $(1) | tools/bin/go-junit-report > test.xml \
	|| { $(FAILPOINT_DISABLE); exit 1; }
	tools/bin/gocov convert "$(TEST_DIR)/cov.$(2).out" | tools/bin/gocov-xml > coverage.xml
	$(FAILPOINT_DISABLE)
endef

unit_test: tools_setup
	$(call run_unit_test,$(PACKAGES),unit_test)

unit_test_in_verify_ci: tools_setup
	$(call run_unit_test_in_verify_ci,$(PACKAGES),unit_test)

# run unit test for the specified pkg only, like `make unit_test_pkg PKG=github.com/pingcap/dm/dm/master`
unit_test_pkg: tools_setup
	$(call run_unit_test,$(PKG),unit_test_syncer)

unit_test_relay: tools_setup
	$(call run_unit_test,$(PACKAGES_RELAY),unit_test_relay)

unit_test_syncer: tools_setup
	$(call run_unit_test,$(PACKAGES_SYNCER),unit_test_syncer)

unit_test_pkg_binlog: tools_setup
	$(call run_unit_test,$(PACKAGES_PKG_BINLOG),unit_test_pkg_binlog)

unit_test_others: tools_setup
	$(call run_unit_test,$(PACKAGES_OTHERS),unit_test_others)

check: check_merge_conflicts tools_setup lint fmt terror_check tidy_mod

check_merge_conflicts:
	@echo "check-merge-conflicts"
	@! git --no-pager grep -E '^<<<<<<< '

fmt: tools_setup
	@if [[ "${nolint}" != "true" ]]; then\
		echo "shfmt"; \
		tools/bin/shfmt -l -w -d "tests/" ; \
		echo "gofumports"; \
		tools/bin/gofumports -w -d -local $(PACKAGE_NAME) $(PACKAGE_DIRECTORIES) 2>&1 | awk "{print} END{if(NR>0) {exit 1}}" ;\
		echo "golangci-lint"; \
		tools/bin/golangci-lint run --config=$(CURDIR)/.golangci.yml --issues-exit-code=1 $(PACKAGE_DIRECTORIES) ;\
	fi

lint: tools_setup
	@if [[ "${nolint}" != "true" ]]; then\
		echo "golangci-lint"; \
		tools/bin/golangci-lint run --config=$(CURDIR)/.golangci.yml --issues-exit-code=1 $(PACKAGE_DIRECTORIES); \
		echo "run shfmt"; \
		tools/bin/shfmt -d "tests/"; \
	fi

terror_check:
	@echo "check terror conflict"
	_utils/terror_gen/check.sh

tidy_mod:
	@echo "tidy go.mod"
	$(GO) mod tidy
	git diff --exit-code go.mod go.sum

dm_integration_test_build: tools_setup
	$(FAILPOINT_ENABLE)
	$(GOTEST) -ldflags '$(LDFLAGS)' -c $(TEST_RACE_FLAG) -cover -covermode=atomic \
		-coverpkg=github.com/pingcap/dm/... \
		-o bin/dm-worker.test github.com/pingcap/dm/cmd/dm-worker \
		|| { $(FAILPOINT_DISABLE); exit 1; }
	$(GOTEST) -ldflags '$(LDFLAGS)' -c $(TEST_RACE_FLAG) -cover -covermode=atomic \
		-coverpkg=github.com/pingcap/dm/... \
		-o bin/dm-master.test github.com/pingcap/dm/cmd/dm-master \
		|| { $(FAILPOINT_DISABLE); exit 1; }
	$(GOTEST) -ldflags '$(LDFLAGS)' -c -cover -covermode=count \
		-coverpkg=github.com/pingcap/dm/... \
		-o bin/dmctl.test github.com/pingcap/dm/cmd/dm-ctl \
		|| { $(FAILPOINT_DISABLE); exit 1; }
	$(GOTEST) -ldflags '$(LDFLAGS)' -c $(TEST_RACE_FLAG) -cover -covermode=atomic \
		-coverpkg=github.com/pingcap/dm/... \
		-o bin/dm-syncer.test github.com/pingcap/dm/cmd/dm-syncer \
		|| { $(FAILPOINT_DISABLE); exit 1; }
	$(FAILPOINT_DISABLE)
	tests/prepare_tools.sh

dm_integration_test_build_master: tools_setup
	$(FAILPOINT_ENABLE)
	$(GOTEST) -ldflags '$(LDFLAGS)' -c $(TEST_RACE_FLAG) -cover -covermode=atomic \
		-coverpkg=github.com/pingcap/dm/... \
		-o bin/dm-master.test github.com/pingcap/dm/cmd/dm-master \
		|| { $(FAILPOINT_DISABLE); exit 1; }
	$(FAILPOINT_DISABLE)
	tests/prepare_tools.sh

dm_integration_test_build_worker: tools_setup
	$(FAILPOINT_ENABLE)
	$(GOTEST) -ldflags '$(LDFLAGS)' -c $(TEST_RACE_FLAG) -cover -covermode=atomic \
		-coverpkg=github.com/pingcap/dm/... \
		-o bin/dm-worker.test github.com/pingcap/dm/cmd/dm-worker \
		|| { $(FAILPOINT_DISABLE); exit 1; }
	$(FAILPOINT_DISABLE)
	tests/prepare_tools.sh

check_third_party_binary:
	@which bin/tidb-server
	@which bin/sync_diff_inspector

integration_test: check_third_party_binary
	@which bin/dm-master.test
	@which bin/dm-worker.test
	@which bin/dm-syncer.test
	tests/run.sh $(CASE)

compatibility_test: check_third_party_binary
	@which bin/dm-master.test.current
	@which bin/dm-worker.test.current
	@which bin/dm-master.test.previous
	@which bin/dm-worker.test.previous
	tests/compatibility_run.sh ${CASE}

# unify cover mode in coverage files, more details refer to tests/_utils/run_dm_ctl
coverage_fix_cover_mode:
	sed -i "s/mode: count/mode: atomic/g" $(TEST_DIR)/cov.*.dmctl.*.out

coverage: coverage_fix_cover_mode tools_setup
	tools/bin/gocovmerge "$(TEST_DIR)"/cov.* | grep -vE ".*.pb.go|.*.pb.gw.go|.*.__failpoint_binding__.go|.*debug-tools.*|.*portal.*|.*chaos.*" > "$(TEST_DIR)/all_cov.out"
	tools/bin/gocovmerge "$(TEST_DIR)"/cov.unit_test*.out | grep -vE ".*.pb.go|.*.pb.gw.go|.*.__failpoint_binding__.go|.*debug-tools.*|.*portal.*|.*chaos.*" > $(TEST_DIR)/unit_test.out
ifeq ("$(JenkinsCI)", "1")
	@bash <(curl -s https://codecov.io/bash) -f $(TEST_DIR)/unit_test.out -t $(CODECOV_TOKEN)
	tools/bin/goveralls -coverprofile=$(TEST_DIR)/all_cov.out -service=jenkins-ci -repotoken $(COVERALLS_TOKEN)
else
	go tool cover -html "$(TEST_DIR)/all_cov.out" -o "$(TEST_DIR)/all_cov.html"
	go tool cover -html "$(TEST_DIR)/unit_test.out" -o "$(TEST_DIR)/unit_test_cov.html"
endif

failpoint-enable: tools_setup
	$(FAILPOINT_ENABLE)

failpoint-disable: tools_setup
	$(FAILPOINT_DISABLE)
