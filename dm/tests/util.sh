#!/bin/bash

wait_for_syncer_alive() {
	i=0
	while [ $i -lt 20 ]; do
		check_syncer_alive
		ret=$?
		if [ "$ret" == 1 ]; then
			echo "syncer is alive"
			break
		fi
		((i++))
		cat syncer_sharding_test.log
		sleep 1
	done
}

check_syncer_alive() {
	pid=$(pgrep syncer)
	if [ -n "$pid" ]; then
		return 1
	else
		return 0
	fi
}

check_db_status() {
	while true; do
		if mysqladmin -h "$1" -P "$2" -u root ping >/dev/null 2>&1; then
			break
		fi
		echo "wait for ready: $1:$2"
		sleep 1
	done
	echo "$3 is alive"
}

stop_syncer() {
	killall -9 syncer || true
}

start_tidb() {
	cd "${TIDB_DIR}" || exit
	killall -9 tidb-server || true
	bin/tidb-server >"$1" 2>&1 &
	cd "${OLDPWD}" || exit
}

stop_tidb() {
	killall -9 tidb-server || true
	rm -r /tmp/tidb || true
}

check_previous_command_success_or_exit() {
	if [ "$?" != 0 ]; then
		exit 1
	fi
}
