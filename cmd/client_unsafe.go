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

package cmd

import (
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/pkg/txnutil/gc"
	"github.com/spf13/cobra"
)

func newUnsafeCommand() *cobra.Command {
	command := &cobra.Command{
		Use:    "unsafe",
		Args:   cobra.NoArgs,
		Hidden: true,
	}
	command.AddCommand(
		newDeleteServiceGcSafepointCommand(),
		newResetCommand(),
		newShowMetadataCommand(),
	)
	command.PersistentFlags().BoolVar(&noConfirm, "no-confirm", false, "Don't ask user whether to confirm executing meta command")
	return command
}

func newResetCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "reset",
		Args:  cobra.NoArgs,
		Short: "Reset the status of the TiCDC cluster, delete all meta data in etcd, confirm that you know what this command will do and use it at your own risk",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := confirmMetaDelete(cmd); err != nil {
				return err
			}
			ctx := defaultContext

			leases, err := cdcEtcdCli.GetCaptureLeases(ctx)
			if err != nil {
				return errors.Trace(err)
			}

			err = cdcEtcdCli.ClearAllCDCInfo(ctx)
			if err != nil {
				return errors.Trace(err)
			}

			err = cdcEtcdCli.RevokeAllLeases(ctx, leases)
			if err != nil {
				return errors.Trace(err)
			}

			_, err = pdCli.UpdateServiceGCSafePoint(ctx, gc.CDCServiceSafePointID, 0, 0)
			if err != nil {
				return errors.Trace(err)
			}

			cmd.Println("reset and all metadata truncated in PD!")

			return nil
		},
	}
	return command
}

func newShowMetadataCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "show-metadata",
		Args:  cobra.NoArgs,
		Short: "Show metadata stored in PD",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := defaultContext
			kvs, err := cdcEtcdCli.GetAllCDCInfo(ctx)
			if err != nil {
				return errors.Trace(err)
			}
			for _, kv := range kvs {
				cmd.Printf("Key: %s, Value: %s\n", string(kv.Key), string(kv.Value))
			}
			cmd.Printf("Show %d KVs\n", len(kvs))
			return nil
		},
	}
	return command
}

func newDeleteServiceGcSafepointCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "delete-service-gc-safepoint",
		Args:  cobra.NoArgs,
		Short: "Delete CDC service GC safepoint in PD, confirm that you know what this command will do and use it at your own risk",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := confirmMetaDelete(cmd); err != nil {
				return err
			}
			ctx := defaultContext
			_, err := pdCli.UpdateServiceGCSafePoint(ctx, gc.CDCServiceSafePointID, 0, 0)
			if err == nil {
				cmd.Println("CDC service GC safepoint truncated in PD!")
			}
			return errors.Trace(err)
		},
	}
	return command
}

func confirmMetaDelete(cmd *cobra.Command) error {
	if noConfirm {
		return nil
	}
	cmd.Printf("Confirm that you know what this command will do and use it at your own risk [Y/N]\n")
	var yOrN string
	_, err := fmt.Scan(&yOrN)
	if err != nil {
		return err
	}
	if strings.ToLower(strings.TrimSpace(yOrN)) != "y" {
		return errors.NewNoStackError("abort meta command")
	}
	return nil
}
