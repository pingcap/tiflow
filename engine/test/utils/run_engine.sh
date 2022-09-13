#!/bin/bash

set -e

cd "$(dirname "$0")/../../.."

DOCKER_DIR="./deployments/engine/docker"

if which docker-compose &>/dev/null; then
	COMPOSECMD="docker-compose"
else
	COMPOSECMD="docker compose"
fi

function generate_flag() {
	shift
	while [[ ${1} ]]; do
		flag=("${flag[@]}" -f "$1")
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
	flag=()
	generate_flag "$@"
	$COMPOSECMD "${flag[@]}" up -d --force-recreate

	echo -e "\n\n[$(date)] <<<<<< deploy engine cluster success! >>>>>>"
	docker container ls
	;;
"stop")
	flag=()
	generate_flag "$@"
	$COMPOSECMD "${flag[@]}" kill || true
	$COMPOSECMD "${flag[@]}" down || true

	echo -e "\n\n[$(date)] <<<<<< stop engine cluster success! >>>>>>"
	docker container ls
	;;
"logs")
	shift && WORK_DIR=$1
	flag=()
	generate_flag "$@"
	$COMPOSECMD "${flag[@]}" logs -t >$WORK_DIR/docker_compose.log || echo "fail to save logs"

	echo -e "[$(date)] <<<<<< save docker compose logs success! >>>>>>\n"
	;;
*)
	echo "Unknown parameter: ${1}" >&2
	echo "Only support 'build-local','build','deploy','stop'" >&2
	exit 1
	;;
esac
