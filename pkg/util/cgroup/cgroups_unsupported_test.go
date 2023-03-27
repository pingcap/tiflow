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

import (
	"testing"
)

func TestFromCgroup(t *testing.T) {
	limit, err := FromCgroup()
	if err != ErrCgroupsNotSupported {
		t.Fatalf("FromCgroup() error = %v, wantErr %v", err, ErrCgroupsNotSupported)
	}
	if limit != 0 {
		t.Fatalf("FromCgroup() got = %v, want %v", limit, 0)
	}
}

func TestFromCgroupV1(t *testing.T) {
	limit, err := FromCgroupV1()
	if err != ErrCgroupsNotSupported {
		t.Fatalf("FromCgroupV1() error = %v, wantErr %v", err, ErrCgroupsNotSupported)
	}
	if limit != 0 {
		t.Fatalf("FromCgroupV1() got = %v, want %v", limit, 0)
	}
}

func TestFromCgroupV2(t *testing.T) {
	limit, err := FromCgroupV2()
	if err != ErrCgroupsNotSupported {
		t.Fatalf("FromCgroupV2() error = %v, wantErr %v", err, ErrCgroupsNotSupported)
	}
	if limit != 0 {
		t.Fatalf("FromCgroupV2() got = %v, want %v", limit, 0)
	}
}
