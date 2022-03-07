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

package framework

// MqListener represents a callback function for listening on the MQ output
type MqListener func(states interface{}, topic string, key []byte, value []byte) error

// Environment is an abstract of the CDC-Upstream-Downstream-MQ complex to be tested
type Environment interface {
	Setup()
	TearDown()
	Reset()
	RunTest(Task)
	SetListener(states interface{}, listener MqListener)
}
