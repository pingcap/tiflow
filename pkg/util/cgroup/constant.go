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

package cgroup

import "errors"

const (
	cgroupMountPoint = "/sys/fs/cgroup"
)

var (
	// ErrNoLimit is returned when the memory limit is not set.
	ErrNoLimit = errors.New("memory is not limited")
	// ErrNoCgroup is returned when the process is not in cgroup.
	ErrNoCgroup = errors.New("process is not in cgroup")
	// ErrCgroupsNotSupported is returned when the system does not support cgroups.
	ErrCgroupsNotSupported = errors.New("cgroups is not supported on this system")
)
