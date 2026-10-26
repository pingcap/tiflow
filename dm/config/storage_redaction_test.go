// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package config

import (
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTaskConfigRedacted(t *testing.T) {
	cfg := NewTaskConfig()
	cfg.Name = "task-one"
	cfg.Loaders["local"] = &LoaderConfig{Dir: "./dump"}
	require.Equal(t, cfg.String(), cfg.Redacted().String(),
		"get-config formatting must not change for ordinary configurations")
	raw := "s3://bucket/path?access-key=private-ak&secret-access-key=private-sk"
	cfg.Loaders["remote"] = &LoaderConfig{Dir: raw}
	cfg.MySQLInstances = []*MySQLInstance{{Loader: cfg.Loaders["remote"]}}
	before := cfg.String()
	output := cfg.Redacted().String()
	require.NotContains(t, output, "private-")
	require.Contains(t, output, "bucket/path")
	require.Equal(t, before, cfg.String())
	require.Equal(t, raw, cfg.MySQLInstances[0].Loader.Dir)
}

func TestSubTaskConfigRedacted(t *testing.T) {
	cfg := NewSubTaskConfig()
	cfg.Name = "task-one"
	cfg.LoaderConfig.Dir = "./dump"
	require.Equal(t, cfg.String(), cfg.Redacted().String(),
		"log formatting must not change for ordinary configurations")
	raw := "s3://bucket/path?access-key=private-ak&secret-access-key=private-sk"
	cfg.LoaderConfig.Dir = raw
	before := cfg.String()
	output := cfg.Redacted().String()
	require.NotContains(t, output, "private-")
	require.Contains(t, output, "bucket/path")
	require.Equal(t, before, cfg.String())
	require.Equal(t, raw, cfg.LoaderConfig.Dir)
}

// TestRedactedDirFields fails when a directory-like field is added, so that
// Redacted is reviewed for the new field if it can hold a storage URL.
// Only LoaderConfig.Dir accepts storage URLs; RelayDir and
// SortingDirPhysical are local paths.
func TestRedactedDirFields(t *testing.T) {
	require.Equal(t, []string{"RelayDir", "LoaderConfig.Dir", "LoaderConfig.SortingDirPhysical"},
		dirFieldsForTest(reflect.TypeOf(SubTaskConfig{}), ""))
	// TaskConfig refers to loaders through Loaders and MySQLInstances.
	require.Equal(t, []string{"Dir", "SortingDirPhysical"}, dirFieldsForTest(reflect.TypeOf(LoaderConfig{}), ""))
}

// dirFieldsForTest lists string fields whose name contains "dir", including
// fields of nested and embedded structs.
func dirFieldsForTest(typ reflect.Type, prefix string) []string {
	fields := []string{}
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		switch field.Type.Kind() {
		case reflect.String:
			if strings.Contains(strings.ToLower(field.Name), "dir") {
				fields = append(fields, prefix+field.Name)
			}
		case reflect.Struct:
			fields = append(fields, dirFieldsForTest(field.Type, prefix+field.Name+".")...)
		}
	}
	return fields
}
