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

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	config2 "github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/pb"
)

// createSources does `operate-source create` operation for two sources.
// NOTE: we put source config files in `conf` directory.
func createSources(ctx context.Context, cli pb.MasterClient, cfg *config) error {
	s1Path := filepath.Join(cfg.ConfigDir, "source1.yaml")
	s2Path := filepath.Join(cfg.ConfigDir, "source2.yaml")
	s3Path := filepath.Join(cfg.ConfigDir, "source3.yaml")

	s1Content, err := os.ReadFile(s1Path)
	if err != nil {
		return err
	}
	s2Content, err := os.ReadFile(s2Path)
	if err != nil {
		return err
	}
	s3Content, err := os.ReadFile(s3Path)
	if err != nil {
		return err
	}

	cfg1, err := config2.ParseYaml(string(s1Content))
	if err != nil {
		return err
	}
	cfg2, err := config2.ParseYaml(string(s2Content))
	if err != nil {
		return err
	}
	cfg3, err := config2.ParseYaml(string(s3Content))
	if err != nil {
		return err
	}

	// replace DB config.
	cfg1.From = cfg.Source1
	cfg2.From = cfg.Source2
	cfg3.From = cfg.Source3

	// reduce backoffmax for autoresume
	cfg1.Checker.BackoffMax = config2.Duration{Duration: 5 * time.Second}
	cfg2.Checker.BackoffMax = config2.Duration{Duration: 5 * time.Second}
	cfg3.Checker.BackoffMax = config2.Duration{Duration: 5 * time.Second}

	s1Content2, err := cfg1.Yaml()
	if err != nil {
		return err
	}
	s2Content2, err := cfg2.Yaml()
	if err != nil {
		return err
	}
	s3Content3, err := cfg3.Yaml()
	if err != nil {
		return err
	}

	resp, err := cli.OperateSource(ctx, &pb.OperateSourceRequest{
		Op:     pb.SourceOp_StartSource,
		Config: []string{s1Content2, s2Content2, s3Content3},
	})
	if err != nil {
		return err
	} else if !resp.Result && !strings.Contains(resp.Msg, "already exists") { // imprecise match
		return fmt.Errorf("fail to create source: %s", resp.Msg)
	}
	return nil
}
