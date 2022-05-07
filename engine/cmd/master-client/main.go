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

package main

import (
	"context"
	"fmt"
	"os"

	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/engine/ctl"
)

func main() {
	ctx := context.Background()
	err := log.InitLogger(&log.Config{
		Level: "info",
	})
	if err != nil {
		fmt.Printf("err: %v", err)
		os.Exit(1)
	}
	ctl.MainStart(ctx, os.Args[1:])
}
