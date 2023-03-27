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

//go:build linux
// +build linux

package cgroup

import (
	"flag"
	"log"
	"os"
	"testing"

	"github.com/containerd/cgroups/v3"
)

var (
	cgVersion cgroups.CGMode
	expected  uint64
)

func TestMain(m *testing.M) {
	flag.Uint64Var(&expected, "expected", 0, "Expected cgroup's memory limit")
	flag.Parse()

	cgVersion = cgroups.Mode()
	log.Println("Cgroups version:", cgVersion)

	os.Exit(m.Run())
}

func TestFromCgroup(t *testing.T) {
	_, err := FromCgroup()
	if cgVersion == cgroups.Unavailable && err != ErrNoCgroup {
		t.Fatalf("FromCgroup() error = %v, wantErr %v", err, ErrNoCgroup)
	}

	if err != nil {
		t.Fatalf("FromCgroup() error = %v, wantErr %v", err, nil)
	}
}

func TestFromCgroupV1(t *testing.T) {
	if cgVersion != cgroups.Legacy {
		t.Skip("cgroups v1 is not supported")
	}
	_, err := FromCgroupV1()
	if err != nil {
		t.Fatalf("FromCgroupV1() error = %v, wantErr %v", err, nil)
	}
}

func TestFromCgroupV2(t *testing.T) {
	if cgVersion != cgroups.Hybrid && cgVersion != cgroups.Unified {
		t.Skip("cgroups v2 is not supported")
	}
	_, err := FromCgroupV2()
	if err != nil {
		t.Fatalf("FromCgroupV2() error = %v, wantErr %v", err, nil)
	}
}
