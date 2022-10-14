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
	"path/filepath"

	dmchaos "github.com/pingcap/tiflow/engine/chaos/cases/dm"
	"golang.org/x/sync/errgroup"
)

var filenames = []string{"dmjob"}

func runDMJobCases(ctx context.Context, cfg *config) error {
	eg, ctx2 := errgroup.WithContext(ctx)
	for _, f := range filenames {
		file := f
		eg.Go(func() error {
			testCase, err := dmchaos.NewCase(ctx2, cfg.Addr, file, filepath.Join(cfg.ConfigDir, file+".yaml"))
			if err != nil {
				return err
			}

			return testCase.Run(ctx2)
		})
	}
	return eg.Wait()
}
