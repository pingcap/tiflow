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

// Package pebble is an pebble-based EventSortEngine implementation with such properties:
//  1. all EventSortEngine instances shares several pebble.DB instances;
//  2. keys are encoded with prefix TableID-CRTs-StartTs;
//  3. keys are hashed into different pebble.DB instances based on table prefix.
package pebble
