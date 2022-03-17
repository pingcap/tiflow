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

package cli

import (
	"github.com/pingcap/tiflow/pkg/cmd/context"
	"github.com/pingcap/tiflow/pkg/cmd/factory"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/cyclic/mark"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/spf13/cobra"
	"github.com/tikv/client-go/v2/oracle"
	pd "github.com/tikv/pd/client"
)

// cyclicCreateMarktablesOptions defines flags for the `cli changefeed cyclic create-marktables` command.
type cyclicCreateMarktablesOptions struct {
	createCommonOptions changefeedCommonOptions

	pdClient pd.Client

	pdAddr     string
	credential *security.Credential

	startTs             uint64
	cyclicUpstreamDSN   string
	upstreamSslCaPath   string
	upstreamSslCertPath string
	upstreamSslKeyPath  string
}

// newCyclicCreateMarktablesOptions creates new options for the `cli changefeed cyclic create-marktables` command.
func newCyclicCreateMarktablesOptions() *cyclicCreateMarktablesOptions {
	return &cyclicCreateMarktablesOptions{}
}

func (o *cyclicCreateMarktablesOptions) getUpstreamCredential() *security.Credential {
	return &security.Credential{
		CAPath:   o.upstreamSslCaPath,
		CertPath: o.upstreamSslCertPath,
		KeyPath:  o.upstreamSslKeyPath,
	}
}

// addFlags receives a *cobra.Command reference and binds
// flags related to template printing to it.
func (o *cyclicCreateMarktablesOptions) addFlags(cmd *cobra.Command) {
	cmd.PersistentFlags().Uint64Var(&o.startTs, "start-ts", 0, "Start ts of changefeed")
	cmd.PersistentFlags().StringVar(&o.cyclicUpstreamDSN, "cyclic-upstream-dsn", "", "(Expremental) Upstream TiDB DSN in the form of [user[:password]@][net[(addr)]]/")
	cmd.PersistentFlags().StringVar(&o.upstreamSslCaPath, "cyclic-upstream-ssl-ca", "", "CA certificate path for TLS connection")
	cmd.PersistentFlags().StringVar(&o.upstreamSslCertPath, "cyclic-upstream-ssl-cert", "", "Certificate path for TLS connection")
	cmd.PersistentFlags().StringVar(&o.upstreamSslKeyPath, "cyclic-upstream-ssl-key", "", "Private key path for TLS connection")
}

// complete adapts from the command line args to the data and client required.
func (o *cyclicCreateMarktablesOptions) complete(f factory.Factory) error {
	pdClient, err := f.PdClient()
	if err != nil {
		return err
	}

	o.pdClient = pdClient

	o.pdAddr = f.GetPdAddr()
	o.credential = f.GetCredential()

	return nil
}

// run the `cli changefeed cyclic create-marktables` command.
func (o *cyclicCreateMarktablesOptions) run(cmd *cobra.Command) error {
	ctx := context.GetDefaultContext()

	cfg := config.GetDefaultReplicaConfig()
	if len(o.createCommonOptions.configFile) > 0 {
		if err := o.createCommonOptions.strictDecodeConfig("TiCDC changefeed", cfg); err != nil {
			return err
		}
	}

	ts, logical, err := o.pdClient.GetTS(ctx)
	if err != nil {
		return err
	}
	o.startTs = oracle.ComposeTS(ts, logical)

	_, eligibleTables, err := getTables(o.pdAddr, o.credential, cfg, o.startTs)
	if err != nil {
		return err
	}

	tables := make([]mark.TableName, len(eligibleTables))
	for i := range eligibleTables {
		tables[i] = &eligibleTables[i]
	}

	err = mark.CreateMarkTables(ctx, o.cyclicUpstreamDSN, o.getUpstreamCredential(), tables...)
	if err != nil {
		return err
	}

	cmd.Printf("Create cyclic replication mark tables successfully! Total tables: %d\n", len(eligibleTables))

	return nil
}

// newCmdCyclicCreateMarktables creates the `cli changefeed cyclic create-marktables` command.
func newCmdCyclicCreateMarktables(f factory.Factory) *cobra.Command {
	o := newCyclicCreateMarktablesOptions()

	command := &cobra.Command{
		Use:   "create-marktables",
		Short: "Create cyclic replication mark tables",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			err := o.complete(f)
			if err != nil {
				return err
			}

			return o.run(cmd)
		},
	}

	o.addFlags(command)

	return command
}
