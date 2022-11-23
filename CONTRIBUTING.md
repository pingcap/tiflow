# How to contribute

This document outlines some of the conventions on development workflow, commit
message formatting, contact points and other resources to make it easier to get
your contribution accepted.

## Getting started

- Fork the repository on GitHub.
- Read the README.md for build instructions.
- Play with the project, submit bugs, submit patches!

## Building TiDB-CDC

Developing TiDB-CDC requires:

* [Go 1.19+](https://go.dev/doc/code)
* An internet connection to download the dependencies

Simply run `make` to build the program.

```sh
make
```

### Running tests

This project contains unit tests and integration tests with coverage collection.
See [tests/integration_tests/README.md](./tests/integration_tests/README.md) for how to execute and add tests.

For more information on how to trigger these tests, please see the [command help](./docs/ci/command.md).

### Debug with [delve](https://github.com/go-delve/delve)
```shell
dlv debug --build-flags="-ldflags='-X github.com/pingcap/tiflow/pkg/version.ReleaseVersion=$(git describe --tags)'" ./cmd/cdc -- server
```

### Updating dependencies

TiDB-CDC uses [Go Modules](https://github.com/golang/go/wiki/Modules) to manage dependencies. To add or update a
dependency: use the `go mod edit` command to change the dependency.

## Contribution flow

This is a rough outline of what a contributor's workflow looks like:

- Create a topic branch from where you want to base your work. This is usually `master`.
- Make commits of logical units and add test case if the change fixes a bug or adds new functionality.
- Run tests and make sure all the tests are passed.
- Make sure your commit messages are in the proper format (see below).
- Push your changes to a topic branch in your fork of the repository.
- Submit a pull request.
- Your PR must receive LGTMs from two maintainers.

Thanks for your contributions!

### Code style

The coding style suggested by the Golang community is used in TiDB-CDC.
See the [style doc](https://github.com/golang/go/wiki/CodeReviewComments) for details.

Please follow this style to make TiDB-CDC easy to review, maintain and develop.

### Format of the Commit Message

We follow a rough convention for commit messages that is designed to answer two
questions: what changed and why. The subject line should feature the what and
the body of the commit should describe the why.

```
capture(ticdc): add comment for variable declaration

Improve documentation.
```

The format can be described more formally as follows:

```
<subsystem>(ticdc|dm|engine|all): <what changed>
<BLANK LINE>
<why this change was made>
<BLANK LINE>
<footer>(optional)
```

The first line is the subject and should be no longer than 70 characters, the second line is always blank, and other
lines should be wrapped at 80 characters. This allows the message to be easier to read on GitHub as well as in various
git tools.

If the change affects more than one subsystem, you can use comma to separate them like ```capture,puller:```. If the
change affects many subsystems, you can use ```*``` instead, like ```*:```.

If this change only affects ticdc, fill in ```<subsystem>(ticdc)```,
if it only affects dm, fill in ```<subsystem>(dm)```,
if it only affects dataflow engine, fill in ```<subsystem>(engine)```.
If it involves a code that is used by all products, fill in ```<subsystem>(all)```.

For the why part, if no specific reason for the change, you can use one of some generic reasons like "Improve
documentation.",
"Improve performance.", "Improve robustness.", "Improve test coverage."
