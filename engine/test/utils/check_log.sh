#!/bin/bash

grep "Run all test success" /tmp/tiflow_engine_test/engine_it.log && exit 0 || exit 1
