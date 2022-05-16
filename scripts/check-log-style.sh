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

# zap field name should be camelCase, excepts for idioms and special terms.
grep -RnE "zap.[A-Z][a-zA-Z0-9]+\(\"[0-9A-Za-z]*[-_ ][^\"]*\"(,|\))" cdc tests pkg |
	grep -vE "user-agent" |
	grep -vE "https_proxy|http_proxy|no_proxy" |
	grep -vE "max-message-bytes|max-message-size|replication-factor" |
	grep -vE "release-version|git-hash|git-branch|go-version" |
	grep -vE "failpoint-build|utc-build-time" |
	awk '{ print  } END { if (NR > 0) { exit 1  }  }'
