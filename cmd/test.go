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
	"os"
	"strings"

	pd "github.com/pingcap/pd/v4/client"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/spf13/cobra"
)

var (
	testPdAddr string
)

func init() {
	rootCmd.AddCommand(testKVCmd)

	testKVCmd.Flags().StringVar(&testPdAddr, "pd", "http://127.0.0.1:2379", "address of PD")
}

type testingT struct {
}

// Errorf implements require.TestingT
func (t *testingT) Errorf(format string, args ...interface{}) {
	fmt.Printf(format, args...)
}

// FailNow implements require.TestingT
func (t *testingT) FailNow() {
	os.Exit(-1)
}

var testKVCmd = &cobra.Command{
	Hidden: true,
	Use:    "testkv",
	Short:  "test kv",
	Long:   ``,
	Run: func(cmd *cobra.Command, args []string) {
		addrs := strings.Split(testPdAddr, ",")
		cli, err := pd.NewClient(addrs, getCredential().PDSecurityOption())
		if err != nil {
			fmt.Println(err)
			return
		}

		storage, err := kv.CreateStorage(addrs[0])
		if err != nil {
			fmt.Println(err)
			return
		}

		t := new(testingT)
		kv.TestGetKVSimple(t, cli, storage)
		kv.TestSplit(t, cli, storage)
	},
}
