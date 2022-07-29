// Copyright 2019 PingCAP, Inc.
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

package config

import (
	"testing"

	. "github.com/pingcap/check"
)

func TestConfig(t *testing.T) {
	TestingT(t)
}

type testConfig struct{}

var _ = Suite(&testConfig{})

func (t *testConfig) TestCheckingItems(c *C) {
	for item := range AllCheckingItems {
		c.Assert(ValidateCheckingItem(item), IsNil)
	}
	c.Assert(ValidateCheckingItem("xxx"), NotNil)

	// ignore all checking items
	ignoredCheckingItems := []string{AllChecking}
	c.Assert(FilterCheckingItems(ignoredCheckingItems), IsNil)
	ignoredCheckingItems = append(ignoredCheckingItems, ShardTableSchemaChecking)
	c.Assert(FilterCheckingItems(ignoredCheckingItems), IsNil)

	// ignore shard checking items
	checkingItems := make(map[string]string)
	for item, desc := range AllCheckingItems {
		checkingItems[item] = desc
	}
	delete(checkingItems, AllChecking)

	c.Assert(FilterCheckingItems(ignoredCheckingItems[:0]), DeepEquals, checkingItems)

	delete(checkingItems, ShardTableSchemaChecking)
	c.Assert(FilterCheckingItems(ignoredCheckingItems[1:]), DeepEquals, checkingItems)

	ignoredCheckingItems = append(ignoredCheckingItems, ShardAutoIncrementIDChecking)
	delete(checkingItems, ShardAutoIncrementIDChecking)
	c.Assert(FilterCheckingItems(ignoredCheckingItems[1:]), DeepEquals, checkingItems)
}
