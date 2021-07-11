package changefeed

import (
	"github.com/pingcap/ticdc/pkg/cmd/context"
	"github.com/pingcap/ticdc/pkg/cmd/util"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/cyclic/mark"
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/spf13/cobra"
	"github.com/tikv/client-go/v2/oracle"
)

type CyclicChangefeedOptions struct {
	createCommonOptions CreateCommonOptions

	cyclicUpstreamDSN   string
	upstreamSslCaPath   string
	upstreamSslCertPath string
	upstreamSslKeyPath  string
}

func NewCyclicChangefeedOptions() *CyclicChangefeedOptions {
	return &CyclicChangefeedOptions{}
}

func (o *CyclicChangefeedOptions) getUpstreamCredential() *security.Credential {
	return &security.Credential{
		CAPath:   o.upstreamSslCaPath,
		CertPath: o.upstreamSslCertPath,
		KeyPath:  o.upstreamSslKeyPath,
	}
}

func NewCmdCyclicChangefeed(f util.Factory, commonOptions *CommonOptions) *cobra.Command {
	o := NewCyclicChangefeedOptions()

	command := &cobra.Command{
		Use:   "cyclic",
		Short: "(Experimental) Utility about cyclic replication",
	}

	command.AddCommand(
		&cobra.Command{
			Use:   "create-marktables",
			Short: "Create cyclic replication mark tables",
			Long:  ``,
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

				_, eligibleTables, err := o.createCommonOptions.validateTables(f.GetCredential(), cfg)
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
