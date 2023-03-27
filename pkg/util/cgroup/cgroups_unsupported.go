// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
//
// ============================================================
// Forked from https://github.com/KimMachineGun/automemlimit
//
// Written by Geon Kim <geon0250@gmail.com>
// limitations under the MIT License.

//go:build !linux
// +build !linux

package cgroup

func FromCgroup() (uint64, error) {
	return 0, ErrCgroupsNotSupported
}

func FromCgroupV1() (uint64, error) {
	return 0, ErrCgroupsNotSupported
}

func FromCgroupV2() (uint64, error) {
	return 0, ErrCgroupsNotSupported
}
