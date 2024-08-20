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

package filter

import (
	"testing"

	tifilter "github.com/pingcap/tidb/pkg/util/filter"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestIsSchema(t *testing.T) {
	t.Parallel()
	cases := []struct {
		schema string
		result bool
	}{
		{"", false},
		{"test", false},
		{"SYS", true},
		{"MYSQL", true},
		{tifilter.InformationSchemaName, true},
		{tifilter.InspectionSchemaName, true},
		{tifilter.PerformanceSchemaName, true},
		{tifilter.MetricSchemaName, true},
		{TiCDCSystemSchema, true},
	}
	for _, c := range cases {
		require.Equal(t, c.result, isSysSchema(c.schema))
	}
}

func TestVerifyTableRules(t *testing.T) {
	t.Parallel()
	cases := []struct {
		cfg      *config.FilterConfig
		hasError bool
	}{
		{&config.FilterConfig{}, false},
		{&config.FilterConfig{Rules: []string{""}}, false},
		{&config.FilterConfig{Rules: []string{"*.*"}}, false},
		{&config.FilterConfig{Rules: []string{"test.*ms"}}, false},
		{&config.FilterConfig{Rules: []string{"*.889"}}, false},
		{&config.FilterConfig{Rules: []string{"test-a.*", "*.*.*"}}, true},
		{&config.FilterConfig{Rules: []string{"*.*", "*.*.*", "*.*.*.*"}}, true},
	}
	for _, c := range cases {
		f, err := VerifyTableRules(c.cfg)
		require.Equal(t, c.hasError, err != nil, "case: %s", c.cfg.Rules)
		if !c.hasError {
			require.True(t, f != nil)
		}
	}
}
