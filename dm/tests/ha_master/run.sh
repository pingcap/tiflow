#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME
API_VERSION="v1alpha1"
MASTER_PORT1=8261
MASTER_PORT2=8361
MASTER_PORT3=8461
MASTER_PORT4=8561
MASTER_PORT5=8661

LEADER_NAME="master1"
LEADER_PORT=MASTER_PORT1

function set_leader_port() {
	case $LEADER_NAME in
	"master1")
		LEADER_PORT=$MASTER_PORT1
		;;
	"master2")
		LEADER_PORT=$MASTER_PORT2
		;;
	"master3")
		LEADER_PORT=$MASTER_PORT3
		;;
	"master4")
		LEADER_PORT=$MASTER_PORT4
		;;
	"master5")
		LEADER_PORT=$MASTER_PORT5
		;;
	esac
}

function test_evict_leader() {
	echo "[$(date)] <<<<<< start test_evict_leader >>>>>>"

	master_ports=($MASTER_PORT1 $MASTER_PORT2 $MASTER_PORT3 $MASTER_PORT4 $MASTER_PORT5)

	# evict leader
	for i in $(seq 0 4); do
		LEADER_NAME=$(get_leader $WORK_DIR 127.0.0.1:${MASTER_PORT1})
		echo "leader is $LEADER_NAME"
		set_leader_port

		run_dm_ctl $WORK_DIR "127.0.0.1:$LEADER_PORT" \
			"operate-leader evict" \
			"\"result\": true" 1

		# evict leader twice, and test evict leader from http interface
		curl -X PUT 127.0.0.1:$LEADER_PORT/apis/v1alpha1/leader/1 >$WORK_DIR/evict_leader.log
		check_log_contains $WORK_DIR/evict_leader.log "\"result\": true" 1

		# will get_leader failed because evict leader on all master, so just skip
		if [ $i = 4 ]; then
			continue
		fi
		# Leader evict is not effective immediately, we need to wait for a proper period of time.
		for _ in {0..30}; do
			NEW_LEADER_NAME=$(get_leader "$WORK_DIR" 127.0.0.1:${MASTER_PORT1})
			if [ "$NEW_LEADER_NAME" != "$LEADER_NAME" ]; then
				break
			fi
			sleep 1
		done
		if [ "$NEW_LEADER_NAME" = "$LEADER_NAME" ]; then
			echo "leader evict failed"
			exit 1
		fi
		echo "new leader is $NEW_LEADER_NAME"
	done

	echo "cancel evict leader on master1, and master1 will be the leader"
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT1" \
		"operate-leader cancel-evict" \
		"\"result\": true" 1

	echo "cancel evict leader twice, and test cancel evict leader from http interface"
	curl -X PUT 127.0.0.1:$MASTER_PORT1/apis/v1alpha1/leader/2 >$WORK_DIR/cancel_evict_leader.log
	check_log_contains $WORK_DIR/cancel_evict_leader.log "\"result\": true" 1

	LEADER_NAME=$(get_leader $WORK_DIR 127.0.0.1:${MASTER_PORT1})
	echo "leader is $LEADER_NAME"
	if [ "$LEADER_NAME" != "master1" ]; then
		echo "cancel evict leader failed"
		exit 1
	fi

	echo "cancel evict leader on all masters"
	for i in $(seq 1 4); do
		echo "cancel master port ${master_ports[$i]}"
		run_dm_ctl $WORK_DIR "127.0.0.1:${master_ports[$i]}" \
			"operate-leader cancel-evict" \
			"\"result\": true" 1
	done

	echo "[$(date)] <<<<<< finish test_evict_leader >>>>>>"
}

function test_list_member() {
	echo "[$(date)] <<<<<< start test_list_member_command >>>>>>"

	master_ports=(0 $MASTER_PORT1 $MASTER_PORT2 $MASTER_PORT3 $MASTER_PORT4 $MASTER_PORT5)
	master_peer_ports=(0 $MASTER_PEER_PORT1 $MASTER_PEER_PORT2 $MASTER_PEER_PORT3 $MASTER_PEER_PORT4 $MASTER_PEER_PORT5)

	alive=(1 2 3 4 5)
	leaders=()
	leader_idx=0

	# TODO: when removing 3 masters (use `seq 0 2`), this test sometimes will fail
	# In these cases, DM-master will campaign successfully, but fails to `get` from etcd while starting scheduler. But finally it will recover.
	for i in $(seq 0 1); do
		alive=("${alive[@]/$leader_idx/}")
		leaders=()

		# get leader in all masters
		for idx in ${alive[@]}; do
			leaders+=($(get_leader $WORK_DIR 127.0.0.1:${master_ports[$idx]}))
		done
		leader=${leaders[0]}
		leader_idx=${leader:6}
		echo "current leader is" $leader

		# check leader is same for every master
		for ld in ${leaders[@]}; do
			if [ "$leader" != "$ld" ]; then
				echo "leader not consisent"
				exit 1
			fi
		done

		# check list-member master
		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:${master_ports[$leader_idx]}" \
			"list-member --master" \
			"\"alive\": true" $((5 - i))

		# kill leader
		echo "kill leader" $leader
		ps aux | grep $leader | awk '{print $2}' | xargs kill || true
		check_master_port_offline $leader_idx
	done

	# join master which has been killed
	alive=("${alive[@]/$leader_idx/}")
	for idx in $(seq 1 5); do
		if [[ ! " ${alive[@]} " =~ " ${idx} " ]]; then
			run_dm_master $WORK_DIR/master${idx} ${master_ports[$idx]} $cur/conf/dm-master${idx}.toml
			check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:${master_ports[$idx]}
		fi
	done
	# check leader is same for every master
	alive=(1 2 3 4 5)
	leaders=()
	for idx in ${alive[@]}; do
		leaders+=($(get_leader $WORK_DIR 127.0.0.1:${master_ports[$idx]}))
	done
	leader=${leaders[0]}
	leader_idx=${leader:6}
	echo "current leader is" $leader
	for ld in ${leaders[@]}; do
		if [ "$leader" != "$ld" ]; then
			echo "leader not consisent"
			exit 1
		fi
	done
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"list-member --master" \
		"\"alive\": true" 5

	# restart follower
	for idx in $(seq 1 5); do
		leader=$(get_leader $WORK_DIR 127.0.0.1:${master_ports[$idx]})
		leader_idx=${leader:6}
		echo "current leader is" $leader
		if [[ $idx = $leader_idx ]]; then
			continue
		fi
		echo "kill master$idx"
		ps aux | grep dm-master$idx | awk '{print $2}' | xargs kill || true
		check_master_port_offline $idx
		sleep 5
		run_dm_master $WORK_DIR/master${idx} ${master_ports[$idx]} $cur/conf/dm-master${idx}.toml
		check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:${master_ports[$idx]}
		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"list-member --master" \
			"\"alive\": true" 5
	done

	# check list-member worker
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"list-member --worker --name=worker1,worker2" \
		"\"stage\": \"bound\"" 2

	dmctl_operate_source stop $WORK_DIR/source1.yaml $SOURCE_ID1

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"list-member --worker" \
		"\"stage\": \"bound\"" 1 \
		"\"stage\": \"free\"" 1

	dmctl_operate_source stop $WORK_DIR/source2.yaml $SOURCE_ID2

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"list-member" \
		"\"stage\": \"free\"" 2

	dmctl_operate_source create $WORK_DIR/source1.yaml $SOURCE_ID1
	dmctl_operate_source create $WORK_DIR/source2.yaml $SOURCE_ID2

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"list-member --name=worker1,worker2" \
		"\"stage\": \"bound\"" 2

	# kill worker
	echo "kill worker1"
	ps aux | grep dm-worker1 | awk '{print $2}' | xargs kill || true
	check_port_offline $WORKER1_PORT 20

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"list-member --name=worker1,worker2" \
		"\"stage\": \"bound\"" 1 \
		"\"stage\": \"offline\"" 1

	# kill worker
	echo "kill worker2"
	ps aux | grep dm-worker2 | awk '{print $2}' | xargs kill || true
	check_port_offline $WORKER2_PORT 20

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"list-member" \
		"\"stage\": \"offline\"" 2

	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"list-member --worker" \
		"\"stage\": \"bound\"" 2

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"config master master1" \
		'name = \\"master1\\"' 1

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"config master master2" \
		'name = \\"master2\\"' 1

	echo "[$(date)] <<<<<< finish test_list_member_command >>>>>>"
}

function run() {
	run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	check_contains 'Query OK, 2 rows affected'
	run_sql_file $cur/data/db2.prepare.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
	check_contains 'Query OK, 3 rows affected'

	echo "start DM worker and master"
	# start 5 dm-master
	run_dm_master $WORK_DIR/master1 $MASTER_PORT1 $cur/conf/dm-master1.toml
	run_dm_master $WORK_DIR/master2 $MASTER_PORT2 $cur/conf/dm-master2.toml
	run_dm_master $WORK_DIR/master3 $MASTER_PORT3 $cur/conf/dm-master3.toml
	run_dm_master $WORK_DIR/master4 $MASTER_PORT4 $cur/conf/dm-master4.toml
	run_dm_master $WORK_DIR/master5 $MASTER_PORT5 $cur/conf/dm-master5.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT1
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT2
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT3
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT4
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT5

	# wait for master raft log to catch up
	sleep 5

	# disabled because https://github.com/pingcap/dm/issues/1458
	#    # a valid version should exist after the cluster bootstrapped.
	#    version_key=$(echo -n '/dm-cluster/version' | base64)
	#    curl http://127.0.0.1:$MASTER_PORT1/v3/kv/range -X POST -d '{"key": "'"${version_key}"'"}' > $WORK_DIR/cluster_version.log
	#    check_log_contains $WORK_DIR/cluster_version.log "value" 1 # only check the version exist but do not check the value now

	# kill dm-master1 and dm-master2 to simulate the first two dm-master addr in join config are invalid
	echo "kill dm-master1 and kill dm-master2"
	ps aux | grep dm-master1 | awk '{print $2}' | xargs kill || true
	check_master_port_offline 1
	ps aux | grep dm-master2 | awk '{print $2}' | xargs kill || true
	check_master_port_offline 2

	# wait for master switch leader and re-setup
	get_leader $WORK_DIR 127.0.0.1:$MASTER_PORT3

	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

	# start dm_master1 and dm-master2 again
	echo "start dm-master1 and dm-master2 again"
	run_dm_master $WORK_DIR/master1 $MASTER_PORT1 $cur/conf/dm-master1.toml
	run_dm_master $WORK_DIR/master2 $MASTER_PORT2 $cur/conf/dm-master2.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT1
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT2

	echo "operate mysql config to worker"
	cp $cur/conf/source1.yaml $WORK_DIR/source1.yaml
	cp $cur/conf/source2.yaml $WORK_DIR/source2.yaml
	sed -i "/relay-binlog-name/i\relay-dir: $WORK_DIR/worker1/relay_log" $WORK_DIR/source1.yaml
	sed -i "/relay-binlog-name/i\relay-dir: $WORK_DIR/worker2/relay_log" $WORK_DIR/source2.yaml
	dmctl_operate_source create $WORK_DIR/source1.yaml $SOURCE_ID1
	dmctl_operate_source create $WORK_DIR/source2.yaml $SOURCE_ID2

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"list-member --master" \
		"\"alive\": true" 5

	test_evict_leader
	test_list_member # TICASE-942, 944, 945, 946, 947

	worker1bound=$($PWD/bin/dmctl.test DEVEL --master-addr "127.0.0.1:$MASTER_PORT1" list-member --name worker1 |
		grep 'source' | awk -F: '{print $2}' | cut -d'"' -f 2)
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-relay -s $worker1bound worker1" \
		"\"result\": true" 1

	echo "start DM task"
	dmctl_start_task "$cur/conf/dm-task.yaml" "--remove-meta"

	echo "use sync_diff_inspector to check full dump loader"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	echo "flush logs to force rotate binlog file"
	run_sql "flush logs;" $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql "flush logs;" $MYSQL_PORT2 $MYSQL_PASSWORD2

	echo "kill dm-master1 and kill dm-master2"
	ps aux | grep dm-master1 | awk '{print $2}' | xargs kill || true
	check_master_port_offline 1
	ps aux | grep dm-master2 | awk '{print $2}' | xargs kill || true
	check_master_port_offline 2

	echo "wait and check task running"
	check_http_alive 127.0.0.1:$MASTER_PORT3/apis/${API_VERSION}/status/test '"stage": "Running"' 10
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT3" \
		"query-status test" \
		"\"stage\": \"Running\"" 3

	run_sql_file $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql_file $cur/data/db2.increment.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
	sleep 2

	echo "use sync_diff_inspector to check data now!"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT4" \
		"offline-member --master --name master1" \
		"\"result\": true" 1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT4" \
		"offline-member --master --name master2" \
		"\"result\": true" 1

	echo "kill dm-master3"
	ps aux | grep dm-master3 | awk '{print $2}' | xargs kill || true
	check_master_port_offline 3

	sleep 2
	# the last two masters should elect a new leader and serve service
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT4" \
		"query-status test" \
		"\"stage\": \"Running\"" 3

	# run master3 again
	run_dm_master $WORK_DIR/master3 $MASTER_PORT3 $cur/conf/dm-master3.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT3

	sleep 5

	rm -rf $WORK_DIR/master1/default.master1
	# join master1 after offline, TICASE-933, 943
	run_dm_master $WORK_DIR/master1 $MASTER_PORT1 $cur/conf/dm-master-join1.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT1

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT4" \
		"pause-task test" \
		"\"result\": true" 3 \
		"\"source\": \"$SOURCE_ID1\"" 1 \
		"\"source\": \"$SOURCE_ID2\"" 1
}

cleanup_data ha_master_test
# also cleanup dm processes in case of last run failed
cleanup_process $*
run $*
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
