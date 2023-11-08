#!/bin/bash
# Copyright 2022 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# See the License for the specific language governing permissions and
# limitations under the License.

# Run this scripts via `make limit-line-width`.

set -e

# The hash of the latest commit with a commit message matching
# the pattern `\(#[0-9]+\)$`. It's usually a master branch commit.
BASE_HASH=$(git --no-pager log -E --grep='\(#[0-9]+\)$' -n 1 --format=format:%H)
# Please contact TiFlow maintainers before changing following settings.
WARN_THRESHOLD=100
ERROR_THRESHOLD=140

git --no-pager diff $BASE_HASH -U0 -- cdc pkg cmd \
	-- ':(exclude)*_gen.go' \
	-- ':(exclude)*_gen_test.go' \
	-- ':(exclude)*_mock.go' \
	-- ':(exclude)*_test_data.go' \
	-- ':(exclude)*.pb.go' |
	grep -E '^\+' | grep -vE '^\+\+\+' | grep -vE 'json:' | grep -vE 'toml:' |
	sed 's/\t/    /g' |
	awk "
{
    # Minus 1 for '+'
    width = length(\$0) - 1;
    if (width > $ERROR_THRESHOLD) {
        print \"\033[0;31m[ERROR]\033[0m width too long, \" length \": \" \$0 ;
        fail=1 ;
    }
}
END {
    if (fail != 0) {
        printf \"\033[0;33mNote\033[0m please keep line width with in $WARN_THRESHOLD, \" ;
        print \"and must not be larger than $ERROR_THRESHOLD.\" ;
        exit 1;
    }
}"
