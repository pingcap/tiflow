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

git diff master -U0 cdc pkg |
	grep -E '^\+' | grep -vE '^\+\+\+' |
	sed 's/\t/    /g' |
	awk '
{
    # Minus 1 for +
    width = length($0) - 1;
    if (width > 100) {
        print "[ERROR] width too long, " length ": " $0 ;
        fail=1 ;
    } else if (width > 80) {
        print "[WARN] width too long, " length ": " $0 ;
    }
}
END { if (fail != 0) { exit 1 } }'
