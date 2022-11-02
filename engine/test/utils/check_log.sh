#!/bin/bash

grep "Run all test success" /tmp/tiflow_engine_test/engine_it.log || exit 1

grep -rE "\[PANIC\]|panic:" /tmp/tiflow_engine_test

if [ $? -eq 0 ]; then
	echo "found tiflow panic"
	exit 1
else
	exit 0
fi
