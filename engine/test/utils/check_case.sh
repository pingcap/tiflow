#!/bin/bash

set -eu

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

for script in $(ls ${CUR_DIR}/../integration_tests/*/run.sh); do
	validated=$(
		cat $script | grep "adjust_config " | grep -v "^#" &>/dev/null
		echo $?
	)
	if [ $validated -ne 0 ]; then
		echo "[Error] need adjust_config in $script"
		exit 1
	fi
done

echo "all integration tests of engine verified"
