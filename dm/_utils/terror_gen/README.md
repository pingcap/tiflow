# Terror check tool

## Principle

* gen.go: read and parse the ast of error_list file to generate current error information, and generate checker_generated.go
* checker_template.go: code template for checker
* checker_generated.go: check whether current errors have conflict with the previous version, checklist as follows:
  - check each error that is not newly added in develop version has the same error code with the release version
  - check each error in develop version has a unique error code
* errors_release.txt: generated error information list

## Run

Run `check.sh` will automatically check the valid of error list, if no conflict is detected, errors_release.txt will be updated with latest error list. Each time we change errors definition in error_list, we must run `make terror_check`, which will call `check.sh` directly.
