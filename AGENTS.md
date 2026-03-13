# Repository Guidelines

## Project Structure & Module Organization
This guide targets DM contributions in the TiFlow monorepo. Core DM code lives in `dm/`, shared libraries commonly touched by DM live in `pkg/`, and DM binaries are launched from `cmd/dm-master`, `cmd/dm-worker`, `cmd/dm-ctl`, and `cmd/dm-syncer`. Integration and compatibility cases live in `dm/tests/`; reusable helpers are under `dm/tests/_utils/`. The DM web UI is in `dm/ui/`. `sync_diff_inspector/` is also relevant because DM integration tests depend on it.

## Build, Test, and Development Commands
Run `make help` for the full target list. Common DM commands:

- `make dm`: build `dm-master`, `dm-worker`, `dmctl`, and `dm-syncer` into `bin/`.
- `make dm_unit_test`: run all DM Go unit tests with race detection.
- `make dm_unit_test_pkg PKG=github.com/pingcap/tiflow/dm/master`: run one package locally.
- `make dm_integration_test_build`: build the `.test` binaries required by `dm/tests/`.
- `make dm_integration_test CASE=sharding`: run one DM integration case.
- `npm --prefix dm/ui run lint`, `npm --prefix dm/ui run type-check`, `npm --prefix dm/ui run build`: validate the DM web UI.

Before DM integration tests, make sure `bin/tidb-server`, `mysql`, and `bin/minio` are available; Python requirements from `dm/tests/requirements.txt` will be installed automatically.

## Coding Style & Naming Conventions
Go is the primary language. Follow [Go Code Review Comments](https://github.com/golang/go/wiki/CodeReviewComments) and keep Go files tab-indented as defined in `.editorconfig`. Run `make fmt` before sending changes; it applies `gci`, `gofumports`, `shfmt`, and repository checks. Keep unit tests in `*_test.go`. Name shell cases by directory, for example `dm/tests/sharding/run.sh`. Do not hand-edit generated outputs; regenerate with `make go-generate`, `make generate-protobuf`, or `make dm_generate_openapi` when relevant.

## Testing Guidelines
Add unit tests for package-level logic and integration cases for end-to-end replication behavior. New DM shell cases should be added under `dm/tests/<case>/run.sh` and registered in `dm/tests/run_group.sh`. Coverage artifacts are written to `/tmp/dm_test`. If a change affects compatibility behavior, use the existing compatibility workflow instead of only adding a unit test.

## Commit & Pull Request Guidelines
Use the established DM commit style: `<subsystem>(dm): <what changed>`, for example `worker(dm): improve retry logging`. Keep the subject within 70 characters and explain the reason in the body. PRs should follow [the pull request template](.github/pull_request_template.md): link the issue with `Issue Number: close #12345`, list the DM tests you ran, note compatibility impact, mention doc updates, and provide a release note or `None`. DM fixes and features are expected to include tests, and PRs normally require two maintainer LGTMs.
