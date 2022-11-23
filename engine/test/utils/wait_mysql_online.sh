#!/bin/bash
set -e

host="127.0.0.1"
port=3306
user="root"
password=""
args=""
tryNums=100

while [[ ${1} ]]; do
	case "${1}" in
	--host)
		host=${2}
		shift
		;;
	--port)
		port=${2}
		shift
		;;
	--user)
		user=${2}
		shift
		;;
	--password)
		password=${2}
		shift
		;;
	--try-nums)
		tryNums=${2}
		shift
		;;
	*)
		args="$args ${1}"
		;;
	esac

	if ! shift; then
		echo 'Missing parameter argument.' >&2
		exit 1
	fi
done

echo "Verifying database ${user}@${host}:${port} is started..."
i=0
if [ -z ${password} ]; then
	check_cmd="mysql -u${user} -h${host} -P${port} ${args} --default-character-set utf8mb4 -e 'select version()'"
else
	check_cmd="mysql -u${user} -h${host} -P${port} ${args} -p${password} --default-character-set utf8mb4 -e 'select version()'"
fi
while ! eval $check_cmd; do
	i=$((i + 1))
	if [ "$i" -gt ${tryNums} ]; then
		echo 'Failed to start database'
		exit 2
	fi
	sleep 2
done
