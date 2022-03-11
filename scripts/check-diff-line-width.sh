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

fail=0
for filename in $(git diff --name-only); do
  # only check files under cdc and pkg folder
  if [[ $filename != cdc* ]] && [[ $filename != pkg* ]]; then
    continue
  fi
  # only check go source files
  if [[ $filename != *.go ]] || [[ $filename == _test.go ]]; then
    continue
  fi
  git --no-pager diff $BASE_HASH -U0 -- $filename |
  	grep -E '^\+' | grep -vE '^\+\+\+' |
  	sed 's/\t/    /g' |
  	awk '
  {
      # Minus 1 for +
      width = length($0) - 1;
      if (width > 100) {
          print "\033[0;31m[ERROR]\033[0m width too long, " length ": " $0 ;
          fail=1 ;
      } else if (width > 80) {
          print "\033[0;33m[WARN]\033[0m  width too long, " length ": " $0 ;
      }
  }
  END { if (fail != 0) { exit 1 } }' || fail=1
done

[[ $fail == 0 ]]
