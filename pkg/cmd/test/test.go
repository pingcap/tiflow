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

package test

import (
	"fmt"
	"os"
	"strings"

	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/spf13/cobra"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
)

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

// options defines flags for the `test` command.
type options struct {
	testPdAddr    string
	caPath        string
	certPath      string
	keyPath       string
	allowedCertCN string
}

// getCredential returns security credentials.
func (o *options) getCredential() *security.Credential {
	var certAllowedCN []string
	if len(o.allowedCertCN) != 0 {
		certAllowedCN = strings.Split(o.allowedCertCN, ",")
	}
	return &security.Credential{
		CAPath:        o.caPath,
		CertPath:      o.certPath,
		KeyPath:       o.keyPath,
		CertAllowedCN: certAllowedCN,
	}
}

// run to run the test and output.
func (o *options) run() {
	addresses := strings.Split(o.testPdAddr, ",")
	cli, err := pd.NewClient(addresses, o.getCredential().PDSecurityOption())
	if err != nil {
		fmt.Println(err)
		return
	}

	storage, err := kv.CreateStorage(addresses[0])
	if err != nil {
		fmt.Println(err)
		return
	}

	tikvStorage := storage.(tikv.Storage) // we know it is tikv.

	t := new(testingT)
	kv.TestGetKVSimple(t, cli, tikvStorage, storage)
	kv.TestSplit(t, cli, tikvStorage, storage)
}

// newOptions creates new options for the `test` command.
func newOptions() *options {
	return &options{}
}

// NewCmdTest creates the `test` command.
func NewCmdTest() *cobra.Command {
	o := newOptions()

	command := &cobra.Command{
		Hidden: true,
		Use:    "testkv",
		Short:  "test kv",
		Long:   ``,
		Run: func(cmd *cobra.Command, args []string) {
			o.run()
		},
	}

	command.Flags().StringVar(&o.testPdAddr, "pd", "http://127.0.0.1:2379", "address of PD")

	return command
}
