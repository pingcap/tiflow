// Copyright 2020 PingCAP, Inc.
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
// limitations under the License.

package util

// Role is the operator role, mainly used for logging at the moment.
type Role int

const (
	RoleOwner Role = iota
	RoleProcessor
	RoleClient
	RoleRedoLogApplier
	RoleKafkaConsumer
	RoleTester
	RoleUnknown
)

func (r Role) String() string {
	switch r {
	case RoleOwner:
		return "owner"
	case RoleProcessor:
		return "processor"
	case RoleClient:
		return "cdc-client"
	case RoleKafkaConsumer:
		return "kafka-consumer"
	case RoleRedoLogApplier:
		return "redo-applier"
	case RoleTester:
		return "tester"
	case RoleUnknown:
		return "unknown"
	}
	return "unknown"
}
