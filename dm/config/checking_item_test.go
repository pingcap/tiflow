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

	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"
)

func TestCheckingItems(t *testing.T) {
	lightningCheck, normalCheck := 0, 0
	for item := range AllCheckingItems {
		require.NoError(t, ValidateCheckingItem(item))
		if slices.Contains(LightningPrechecks, item) {
			lightningCheck++
		} else {
			normalCheck++
		}
	}
	// remember to update the number when add new checking items.
	require.Equal(t, 5, lightningCheck)
	require.Equal(t, 16, normalCheck)
	// all LightningPrechecks can be found by iterating AllCheckingItems
	require.Len(t, LightningPrechecks, lightningCheck)
	require.Error(t, ValidateCheckingItem("xxx"))

	// ignore all checking items
	ignoredCheckingItems := []string{AllChecking}
	require.Nil(t, FilterCheckingItems(ignoredCheckingItems))
	ignoredCheckingItems = append(ignoredCheckingItems, ShardTableSchemaChecking)
	require.Nil(t, FilterCheckingItems(ignoredCheckingItems))

	// ignore shard checking items
	checkingItems := make(map[string]string)
	for item, desc := range AllCheckingItems {
		checkingItems[item] = desc
	}
	delete(checkingItems, AllChecking)

	require.Equal(t, checkingItems, FilterCheckingItems(ignoredCheckingItems[:0]))

	delete(checkingItems, ShardTableSchemaChecking)
	require.Equal(t, checkingItems, FilterCheckingItems(ignoredCheckingItems[1:]))

	ignoredCheckingItems = append(ignoredCheckingItems, ShardAutoIncrementIDChecking)
	delete(checkingItems, ShardAutoIncrementIDChecking)
	require.Equal(t, checkingItems, FilterCheckingItems(ignoredCheckingItems[1:]))
}
