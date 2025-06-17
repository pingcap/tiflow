// Copyright 2019 PingCAP, Inc.
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

package master

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/ctl/common"
	"github.com/pingcap/tiflow/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/spf13/cobra"
)

// NewPurgeRelayCmd creates a PurgeRelay command
// three purge methods supported by dmctl
// 1. purge inactive relay log files
// 2. purge before time, like `PURGE BINARY LOGS BEFORE` in MySQL
// 3. purge before filename, like `PURGE BINARY LOGS TO`.
func NewPurgeRelayCmd() *cobra.Command {
	cmd := &cobra.Command{
		// Use:   "purge-relay <-w worker> [--inactive] [--time] [--filename] [--sub-dir]",
		// Short: "purge dm-worker's relay log files, choose 1 of 2 methods",
		Use:   "purge-relay <-s source> <-f filename> [--sub-dir directory]",
		Short: "Purges relay log files of the DM-worker according to the specified filename",
		RunE:  purgeRelayFunc,
	}
	// cmd.Flags().BoolP("inactive", "i", false, "whether try to purge all inactive relay log files")
	// cmd.Flags().StringP("time", "t", "", fmt.Sprintf("whether try to purge relay log files before this time, the format is \"%s\"(_ between date and time)", timeFormat))
	cmd.Flags().StringP("filename", "f", "", "name of the terminal file before which to purge relay log files. Sample format: \"mysql-bin.000006\"")
	cmd.Flags().StringP("sub-dir", "", "", "specify relay sub directory for --filename. If not specified, the latest one will be used. Sample format: \"2ae76434-f79f-11e8-bde2-0242ac130008.000001\"")

	return cmd
}

// purgeRelayFunc does purge relay log files.
func purgeRelayFunc(cmd *cobra.Command, _ []string) error {
	if len(cmd.Flags().Args()) > 0 {
		cmd.SetOut(os.Stdout)
		common.PrintCmdUsage(cmd)
		return errors.New("please check output to see error")
	}

	sources, err := common.GetSourceArgs(cmd)
	if err != nil {
		return err
	}
	if len(sources) == 0 {
		return errors.New("must specify at least one source (`-s` / `--source`)")
	}

	filename, err := cmd.Flags().GetString("filename")
	if err != nil {
		return err
	}

	if len(filename) == 0 {
		return errors.New("must specify the name of the terminal file before which to purge relay log files. (`-f` / `--filename`)")
	}

	subDir, err := cmd.Flags().GetString("sub-dir")
	if err != nil {
		return err
	}

	if len(filename) > 0 {
		// count++
		filename = strings.Trim(filename, "\"")
	}

	if len(filename) > 0 && len(sources) > 1 {
		return errors.New("for --filename, can only specify one source per time")
	}
	if len(subDir) > 0 {
		subDir = utils.TrimQuoteMark(subDir)
	}
	if len(filename) > 0 && len(subDir) == 0 {
		fmt.Println("[warn] no --sub-dir specify for --filename, the latest one will be used")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resp := &pb.PurgeWorkerRelayResponse{}
	err = common.SendRequest(
		ctx,
		"PurgeWorkerRelay",
		&pb.PurgeWorkerRelayRequest{
			Sources: sources,
			// Inactive: inactive,
			// Time:     time2.Unix(),
			Filename: filename,
			SubDir:   subDir,
		},
		&resp,
	)

	if err != nil {
		return err
	}

	common.PrettyPrintResponse(resp)
	return nil
}
