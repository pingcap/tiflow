#!/bin/bash

cd pkg/workerpool || exit # 需要进入到对应测试目录

for ((i = 1; i <= 100; i++)); do
	go test -check.f TestMultiError >>/test-logs/test.log # 指定需要运行的测试
done
