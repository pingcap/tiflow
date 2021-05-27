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

package main

import "encoding/json"

// singleResult holds the result of a single source.
type singleResult struct {
	Insert int `json:"insert"`
	Update int `json:"update"`
	Delete int `json:"delete"`
	DDL    int `json:"ddl"`
}

func (sr singleResult) String() string {
	data, err := json.Marshal(sr)
	if err != nil {
		return err.Error()
	}
	return string(data)
}

// results holds the results of single or multiple sources task case.
type results []singleResult

func (r results) String() string {
	data, err := json.Marshal(r)
	if err != nil {
		return err.Error()
	}
	return string(data)
}
