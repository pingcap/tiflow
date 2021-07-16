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
	"github.com/pingcap/ticdc/pkg/cmd/context"
	"github.com/pingcap/ticdc/pkg/cmd/util"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/cyclic/mark"
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/spf13/cobra"
	"github.com/tikv/client-go/v2/oracle"
)

// cyclicChangefeedOptions defines flags for the `cli changefeed cyclic` command.
type cyclicChangefeedOptions struct {
	createCommonOptions createChangefeedCommonOptions

	cyclicUpstreamDSN   string
	upstreamSslCaPath   string
	upstreamSslCertPath string
	upstreamSslKeyPath  string
}

// newCyclicChangefeedOptions creates new options for the `cli changefeed cyclic` command.
func newCyclicChangefeedOptions() *cyclicChangefeedOptions {
	return &cyclicChangefeedOptions{}
}

func (o *cyclicChangefeedOptions) getUpstreamCredential() *security.Credential {
	return &security.Credential{
		CAPath:   o.upstreamSslCaPath,
		CertPath: o.upstreamSslCertPath,
		KeyPath:  o.upstreamSslKeyPath,
	}
}

func newCmdCyclicChangefeed(f util.Factory) *cobra.Command {
	o := newCyclicChangefeedOptions()

	command := &cobra.Command{
		Use:   "cyclic",
		Short: "(Experimental) Utility about cyclic replication",
	}

	command.AddCommand(
		&cobra.Command{
			Use:   "create-marktables",
			Short: "Create cyclic replication mark tables",
			RunE: func(cmd *cobra.Command, args []string) error {
				ctx := context.GetDefaultContext()

				cfg := config.GetDefaultReplicaConfig()
				if len(o.createCommonOptions.configFile) > 0 {
					if err := o.createCommonOptions.validateReplicaConfig("TiCDC changefeed", cfg); err != nil {
						return err
					}
				}

				pdClient, err := f.PdClient()
				if err != nil {
					return err
				}

				ts, logical, err := pdClient.GetTS(ctx)
				if err != nil {
					return err
				}
				o.createCommonOptions.startTs = oracle.ComposeTS(ts, logical)

				_, eligibleTables, err := o.createCommonOptions.validateTables(f.GetPdAddr(), f.GetCredential(), cfg)
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
			},
		})

	command.PersistentFlags().StringVar(&o.cyclicUpstreamDSN, "cyclic-upstream-dsn", "", "(Expremental) Upsteam TiDB DSN in the form of [user[:password]@][net[(addr)]]/")
	command.PersistentFlags().StringVar(&o.upstreamSslCaPath, "cyclic-upstream-ssl-ca", "", "CA certificate path for TLS connection")
	command.PersistentFlags().StringVar(&o.upstreamSslCertPath, "cyclic-upstream-ssl-cert", "", "Certificate path for TLS connection")
	command.PersistentFlags().StringVar(&o.upstreamSslKeyPath, "cyclic-upstream-ssl-key", "", "Private key path for TLS connection")

	return command
}
