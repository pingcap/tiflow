#!/bin/bash

set -eu

CLUSTER_NAME="dm-v1"
DM_V2_VER="nightly"

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
PATH=$CUR/../_utils:$PATH # for sync_diff_inspector

source $CUR/lib.sh

# ref https://docs.pingcap.com/zh/tidb-data-migration/stable/deploy-a-dm-cluster-using-ansible
# script following the docs to deploy dm 1.0.7 using ./ansible_data/inventory.ini
function deploy_v1_by_ansible() {
	# step 1
	apt-get update --allow-releaseinfo-change
	apt-get -y install git curl sshpass python-pip sudo

	# step 2
	useradd -m -d /home/tidb tidb
	echo "tidb:tidb" | chpasswd
	echo "tidb ALL=(ALL) NOPASSWD: ALL" >>/etc/sudoers

	# use the same key from root instead of create one.
	mkdir -p /home/tidb/.ssh
	cp ~/.ssh/* /home/tidb/.ssh/
	chown -R tidb:tidb /home/tidb/.ssh/

	# step 3
	su tidb <<EOF
    cd /home/tidb
    wget https://download.pingcap.org/dm-ansible-v1.0.7.tar.gz
EOF

	# step 4
	su tidb <<EOF
    cd /home/tidb
    tar -xzvf dm-ansible-v1.0.7.tar.gz &&
        mv dm-ansible-v1.0.7 dm-ansible &&
        cd /home/tidb/dm-ansible &&
        sudo pip install -r ./requirements.txt
    ansible --version
EOF

	# step 5
	cp $CUR/ansible_data/hosts.ini /home/tidb/dm-ansible/
	cp $CUR/ansible_data/inventory.ini /home/tidb/dm-ansible/

	# not following the docs, use root and without password to run it
	cd /home/tidb/dm-ansible
	sudo ansible-playbook -i hosts.ini create_users.yml -u root
	cd $CUR/../..

	#step 6
	su tidb <<EOF
    cd /home/tidb/dm-ansible
    ansible-playbook local_prepare.yml
EOF

	# skip 7,8

	# step 9
	su tidb <<EOF
    cd /home/tidb/dm-ansible
    ansible -i inventory.ini all -m shell -a 'whoami'
    ansible -i inventory.ini all -m shell -a 'whoami' -b
    ansible-playbook deploy.yml
    ansible-playbook start.yml
EOF

}

function stop_v1_by_ansible() {
	su tidb <<EOF
    cd /home/tidb/dm-ansible
    ansible-playbook stop.yml
EOF
}

function migrate_in_v1 {
	exec_full_stage

	# start v1 task
	/home/tidb/dm-ansible/dmctl/dmctl --master-addr=master1:8261 start-task $CUR/conf/task.yaml

	exec_incremental_stage1

	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml
}

# ref https://docs.pingcap.com/zh/tidb-data-migration/v2.0/deploy-a-dm-cluster-using-tiup
function import_to_v2_by_tiup() {
	# install TiUP-DM
	curl --proto '=https' --tlsv1.2 -sSf https://tiup-mirrors.pingcap.com/install.sh | sh
	source /root/.profile
	tiup install dm

	# import from v1
	# TODO: update `--cluster-version` to the target version later.
	tiup install dmctl:$DM_V2_VER
	tiup dm import --yes --dir=/home/tidb/dm-ansible --cluster-version $DM_V2_VER
	tiup dm start --yes $CLUSTER_NAME
}

function migrate_in_v2 {
	exec_incremental_stage2

	echo "check sources"
	run_dmctl_with_retry $DM_V2_VER "operate-source show" "mysql-replica-01" 1 "mariadb-replica-02" 1
	echo "check workers"
	run_dmctl_with_retry $DM_V2_VER "list-member --worker" "\"stage\": \"bound\"" 2

	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

	# stop v2 task
	tiup dmctl:$DM_V2_VER --master-addr=master1:8261 stop-task $TASK_NAME
}

function destroy_v2_by_tiup() {
	tiup dm destroy --yes $CLUSTER_NAME
}

function test() {
	install_sync_diff

	deploy_v1_by_ansible

	migrate_in_v1

	stop_v1_by_ansible

	import_to_v2_by_tiup

	migrate_in_v2

	destroy_v2_by_tiup
}

test
