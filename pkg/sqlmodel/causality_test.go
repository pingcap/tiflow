// Copyright 2022 PingCAP, Inc.
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

package sqlmodel

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	cdcmodel "github.com/pingcap/tiflow/cdc/model"
)

func TestCausalityKeys(t *testing.T) {
	t.Parallel()

	source := &cdcmodel.TableName{Schema: "db", Table: "tb1"}

	cases := []struct {
		createSQL string
		preValue  []interface{}
		postValue []interface{}

		causalityKeys []string
	}{
		{
			"CREATE TABLE tb1 (c INT PRIMARY KEY, c2 INT, c3 VARCHAR(10) UNIQUE)",
			[]interface{}{1, 2, "abc"},
			[]interface{}{3, 4, "abc"},
			[]string{"1.c.db.tb1", "abc.c3.db.tb1", "3.c.db.tb1", "abc.c3.db.tb1"},
		},
		{
			"CREATE TABLE tb1 (c INT PRIMARY KEY, c2 INT, c3 VARCHAR(10), UNIQUE INDEX(c3(1)))",
			[]interface{}{1, 2, "abc"},
			[]interface{}{3, 4, "adef"},
			[]string{"1.c.db.tb1", "a.c3.db.tb1", "3.c.db.tb1", "a.c3.db.tb1"},
		},
	}

	for _, ca := range cases {
		ti := mockTableInfo(t, ca.createSQL)
		change := NewRowChange(source, nil, ca.preValue, ca.postValue, ti, nil, nil)
		require.Equal(t, ca.causalityKeys, change.CausalityKeys())
	}
}

func TestCausalityKeysNoRace(t *testing.T) {
	t.Parallel()

	source := &cdcmodel.TableName{Schema: "db", Table: "tb1"}
	ti := mockTableInfo(t, "CREATE TABLE tb1 (c INT PRIMARY KEY, c2 INT, c3 VARCHAR(10) UNIQUE)")
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			change := NewRowChange(source, nil, []interface{}{1, 2, "abc"}, []interface{}{3, 4, "abc"}, ti, nil, nil)
			change.CausalityKeys()
			wg.Done()
		}()
	}
	wg.Wait()
}
