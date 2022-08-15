#!/bin/bash

set -e

cd "$(dirname "$0")/../../.."

DOCKER_DIR="./deployments/engine/docker"

function generate_flag() {
	shift
	while [[ ${1} ]]; do
		flag=${flag}"-f $1 "
		shift
	done
}

case $1 in
"build-local")
	docker build -f $DOCKER_DIR/dev.Dockerfile -t dataflow:test .
	;;
"build")
	docker build -f $DOCKER_DIR/Dockerfile -t dataflow:test .
	;;
"deploy")
	flag=
	generate_flag "$@"
	docker compose "$flag" up -d --force-recreate

	echo -e "\n\n[$(date)] <<<<<< deploy engine cluster success! >>>>>>"
	docker container ls
	;;
"stop")
	flag=
	generate_flag "$@"
	docker compose "$flag" down

	echo -e "\n\n[$(date)] <<<<<< stop engine cluster success! >>>>>>"
	docker container ls
	;;
*)
	echo "Unknown parameter: ${1}" >&2
	exit 1
	;;
esac
