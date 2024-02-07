#!/bin/sh

# call the MySQL image provided entrypoint script
# "$@" is to pass all parameters as they are provided
/entrypoint.sh "$@" &

i=0
while [ "$(netstat -na | grep "LISTEN" | grep "3306" | wc -l)" -eq 0 ]; do
	echo "wait mysql"
	i=$((i + 1))
	if [ "$i" -gt 100 ]; then
		echo "wait for mysql timeout"
		exit 1
	fi
	sleep 1
done

service keepalived start

tail -f /dev/null
