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

//go:build tools
// +build tools

// Tool dependencies are tracked here to make go module happy
// Refer https://github.com/golang/go/wiki/Modules#how-can-i-track-tool-dependencies-for-a-module

package tools

import (

	// make go module happy
	_ "github.com/gogo/protobuf/protoc-gen-gogofaster"
	_ "github.com/golang/mock/mockgen"
	_ "github.com/golangci/golangci-lint/pkg/commands"
	_ "github.com/grpc-ecosystem/grpc-gateway/protoc-gen-grpc-gateway"
	_ "github.com/pingcap/failpoint"
	_ "github.com/pingcap/failpoint/failpoint-ctl"
	_ "mvdan.cc/gofumpt"
	_ "mvdan.cc/sh/v3/cmd/shfmt"
)
