//go:build !dm_webui
// +build !dm_webui

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

// this file implement all of the APIs of the DataMigration service.
package ui

import "embed"

// WebUIAssets represent the frontend WebUI assets, but this fs is empty when no `make dm-master-with-webui`.
var WebUIAssets embed.FS
