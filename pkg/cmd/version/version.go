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

package version

import (
	"fmt"

	cmdcontext "github.com/pingcap/tiflow/pkg/cmd/context"
	"github.com/pingcap/tiflow/pkg/cmd/factory"
	"github.com/pingcap/tiflow/pkg/cmd/util"
	"github.com/pingcap/tiflow/pkg/logutil"
	"github.com/pingcap/tiflow/pkg/version"
	"github.com/spf13/cobra"
)

// NewCmdVersion creates the `version` command.
func NewCmdVersion() *cobra.Command {
	cf := factory.NewClientFlags()
	// Construct the client construction factory.
	f := factory.NewFactory(cf)
	cmd := &cobra.Command{
		Use:   "version",
		Short: "Output version information",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			// Here we will initialize the logging configuration
			// and set the current default context.
			util.InitCmd(cmd, &logutil.Config{Level: cf.GetLogLevel()})
			util.LogHTTPProxies()
			return nil
		},
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			cmd.Println("cli version:")
			cmd.Println(version.GetRawInfo())
			apiClient, err := f.APIV1Client()
			if err != nil {
				return err
			}
			ctx := cmdcontext.GetDefaultContext()
			status, err := apiClient.Status().Get(ctx)
			if err != nil {
				return err
			}
			cmd.Println("server version:")
			cmd.Println(fmt.Sprintf(
				"Release Version: %s\n"+
					"Git Commit Hash: %s\n", status.Version, status.GitHash))
			return nil
		},
	}
	cf.AddFlags(cmd)
	return cmd
}
