// Copyright 2021 PingCAP, Inc.
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

package openapi

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/pingcap/tiflow/dm/pkg/utils"
)

const foreignKeyChecksSessionKey = "foreign_key_checks"

var defaultMetaSchema = "dm_meta"

// Adjust adjusts task and set default value.
func (t *Task) Adjust() error {
	if t.MetaSchema == nil {
		t.MetaSchema = &defaultMetaSchema
	}
	if t.Timezone != nil && *t.Timezone != "" {
		if _, err := utils.ParseTimeZone(*t.Timezone); err != nil {
			return err
		}
	}
	if t.TargetConfig.Session == nil {
		return nil
	}

	session, err := NormalizeTaskTargetSession(t.TargetConfig.Session.AdditionalProperties)
	if err != nil {
		return err
	}
	if len(session) == 0 {
		t.TargetConfig.Session = nil
	} else {
		t.TargetConfig.Session = &TaskTargetDataBase_Session{AdditionalProperties: session}
	}
	return nil
}

// NormalizeTaskTargetSession validates and normalizes public target session parameters.
func NormalizeTaskTargetSession(session map[string]string) (map[string]string, error) {
	if len(session) == 0 {
		return nil, nil
	}

	keys := make([]string, 0, len(session))
	for key := range session {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	normalized := make(map[string]string, len(session))
	for _, key := range keys {
		normalizedKey := strings.ToLower(key)
		if normalizedKey != foreignKeyChecksSessionKey {
			return nil, fmt.Errorf("unsupported target session parameter %q", key)
		}
		if _, ok := normalized[normalizedKey]; ok {
			return nil, fmt.Errorf("target session parameter %q is duplicated after case normalization", normalizedKey)
		}

		value := session[key]
		if value != "0" && value != "1" {
			return nil, fmt.Errorf("target session parameter %q must be the exact string \"0\" or \"1\"", normalizedKey)
		}
		normalized[normalizedKey] = value
	}
	return normalized, nil
}

// FromJSON unmarshal json to task.
func (t *Task) FromJSON(data []byte) error {
	return json.Unmarshal(data, t)
}

// ToJSON marshal json to task.
func (t *Task) ToJSON() ([]byte, error) {
	return json.Marshal(t)
}
