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

if $(which jq &>/dev/null); then
	dup=$(jq '[.panels[] | .panels[]]| group_by(.id) | .[] | select(length>1) | .[] | { id: .id, title: .title}' metrics/grafana/ticdc.json)
	[[ -n $dup ]] || exit 0
	echo "Find panels with duplicated ID in metrics/grafana/ticdc.json"
	echo "$dup"
	echo "Please choose a new ID that is larger than the max ID:"
	jq '[.panels[] | .panels[] | .id] | max' \
		metrics/grafana/ticdc.json
fi
