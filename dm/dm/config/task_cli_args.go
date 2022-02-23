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

package config

import (
	"encoding/json"
	"time"

	"github.com/pingcap/tiflow/dm/pkg/terror"
)

const (
	StartTimeFormat  = "2006-01-02 15:04:05"
	StartTimeFormat2 = "2006-01-02T15:04:05"
)

// TaskCliArgs is the task command line arguments, these arguments have higher priority than the config file and
// downstream checkpoint, but may need to be removed after the first time they take effect.
type TaskCliArgs struct {
	StartTime        string `toml:"start-time" json:"start_time"`
	SafeModeDuration string `toml:"safe-mode-duration" json:"safe_mode_duration"`
	WaitTimeOnStop   string `toml:"wait-time-on-stop" json:"wait_time_on_stop"`
}

// ToJSON returns json marshal result.
func (t *TaskCliArgs) ToJSON() (string, error) {
	cfg, err := json.Marshal(t)
	if err != nil {
		return "", err
	}
	return string(cfg), nil
}

// Decode load a json representation of TaskCliArgs.
func (t *TaskCliArgs) Decode(data []byte) error {
	err := json.Unmarshal(data, t)
	return err
}

// Verify checks if all fields are legal.
func (t *TaskCliArgs) Verify() error {
	if t.StartTime != "" {
		if _, err := time.Parse(StartTimeFormat, t.StartTime); err != nil {
			_, err = time.Parse(StartTimeFormat2, t.StartTime)
			if err != nil {
				return terror.Annotate(err, "error while parse start-time, expected in the format like '2006-01-02 15:04:05' or '2006-01-02T15:04:05'")
			}
		}
	}

	if t.SafeModeDuration != "" {
		_, err := time.ParseDuration(t.SafeModeDuration)
		if err != nil {
			return terror.Annotate(err, "error while parse safe-mode-duration, expected in the format like '1s' or '1h'")
		}
	}

	if t.WaitTimeOnStop != "" {
		_, err := time.ParseDuration(t.WaitTimeOnStop)
		if err != nil {
			return terror.Annotate(err, "error while parse stop_wait_timeout_duration, expected in the format like '1s' or '1h'")
		}
	}
	return nil
}
