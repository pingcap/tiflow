#!/bin/bash
counter=$(netstat -na | grep "LISTEN" | grep "3306" | wc -l)
if [ "${counter}" -eq 0 ]; then
	/etc/init.d/keepalived stop
else
	exit 0
fi
