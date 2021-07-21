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
	"github.com/pingcap/ticdc/pkg/httputil"
	"github.com/spf13/cobra"
	"os"
)

var rootCmd = &cobra.Command{
	Use:   "cdc",
	Short: "CDC",
	Long:  `Change Data Capture`,
}

// Execute runs the root command
func Execute() {
	// Outputs cmd.Print to stdout.
	rootCmd.SetOut(os.Stdout)
	args := os.Args[1:]

	for _, arg := range args {
		if httputil.IsContainNonASCII(arg) {
			rootCmd.Printf("Please input ASCII char only, the arg: %s contain Non-ASCII char.\n", arg)
			os.Exit(1)
		}
	}

	if err := rootCmd.Execute(); err != nil {
		rootCmd.Println(err)
		os.Exit(1)
	}
}
