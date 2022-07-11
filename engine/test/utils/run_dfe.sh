#!/bin/bash

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
DOCKER_DIR=$(cd $CUR_DIR/../../deployments/docker/ && pwd)
TIFLOW_DIR=$(cd $CUR_DIR/../../../ && pwd)
echo $CUR_DIR

function generate_flag() {
	shift
	while [[ ${1} ]]; do
		flag=${flag}"-f $1 "
		shift
	done
}

case $1 in
"build-local")
	docker build -f $DOCKER_DIR/dev.Dockerfile -t dataflow:test $TIFLOW_DIR
	;;
"build")
	docker build -f $DOCKER_DIR/Dockerfile -t dataflow:test $TIFLOW_DIR
	;;
"deploy")
	flag=
	generate_flag $*
	docker compose $flag up -d --force-recreate

	echo -e "\n\n[$(date)] <<<<<< deploy dfe cluster success! >>>>>>"
	docker container ls
	;;
"stop")
	flag=
	generate_flag $*
	docker compose $flag down

	echo -e "\n\n[$(date)] <<<<<< stop dfe cluster success! >>>>>>"
	docker container ls
	;;
*)
	echo "Unknown parameter: ${1}" >&2
	exit 1
	;;
esac
