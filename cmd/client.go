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
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/chzyer/readline"
	_ "github.com/go-sql-driver/mysql" // mysql driver
	"github.com/mattn/go-shellwords"
	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/cdc"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/logutil"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/spf13/cobra"
	pd "github.com/tikv/pd/client"
	"go.etcd.io/etcd/clientv3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
)

func init() {
	cliCmd := newCliCommand()
	cliCmd.PersistentFlags().StringVar(&cliPdAddr, "pd", "http://127.0.0.1:2379", "PD address, use ',' to separate multiple PDs")
	cliCmd.PersistentFlags().BoolVarP(&interact, "interact", "i", false, "Run cdc cli with readline")
	cliCmd.PersistentFlags().StringVar(&cliLogLevel, "log-level", "warn", "log level (etc: debug|info|warn|error)")
	addSecurityFlags(cliCmd.PersistentFlags(), false /* isServer */)
	rootCmd.AddCommand(cliCmd)
}

var (
	opts       []string
	startTs    uint64
	targetTs   uint64
	sinkURI    string
	configFile string
	cliPdAddr  string
	noConfirm  bool
	sortEngine string
	sortDir    string

	cyclicReplicaID        uint64
	cyclicFilterReplicaIDs []uint
	cyclicSyncDDL          bool
	cyclicUpstreamDSN      string

	cdcEtcdCli kv.CDCEtcdClient
	pdCli      pd.Client

	interact          bool
	simplified        bool
	cliLogLevel       string
	changefeedListAll bool

	changefeedID string
	captureID    string
	interval     uint

	optForceRemove bool

	defaultContext context.Context
)

// changefeedCommonInfo holds some common used information of a changefeed
type changefeedCommonInfo struct {
	ID      string              `json:"id"`
	Summary *cdc.ChangefeedResp `json:"summary"`
}

// capture holds capture information
type capture struct {
	ID            string `json:"id"`
	IsOwner       bool   `json:"is-owner"`
	AdvertiseAddr string `json:"address"`
}

// cfMeta holds changefeed info and changefeed status
type cfMeta struct {
	Info       *model.ChangeFeedInfo   `json:"info"`
	Status     *model.ChangeFeedStatus `json:"status"`
	Count      uint64                  `json:"count"`
	TaskStatus []captureTaskStatus     `json:"task-status"`
}

type captureTaskStatus struct {
	CaptureID  string            `json:"capture-id"`
	TaskStatus *model.TaskStatus `json:"status"`
}

type profileStatus struct {
	OPS            uint64 `json:"ops"`
	Count          uint64 `json:"count"`
	SinkGap        string `json:"sink_gap"`
	ReplicationGap string `json:"replication_gap"`
}

type processorMeta struct {
	Status   *model.TaskStatus   `json:"status"`
	Position *model.TaskPosition `json:"position"`
}

func newCliCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "cli",
		Short: "Manage replication task and TiCDC cluster",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			initCmd(cmd, &logutil.Config{Level: cliLogLevel})

			credential := getCredential()
			tlsConfig, err := credential.ToTLSConfig()
			if err != nil {
				return errors.Annotate(err, "fail to validate TLS settings")
			}
			if tlsConfig != nil {
				if strings.Contains(cliPdAddr, "http://") {
					return errors.New("PD endpoint scheme should be https")
				}
			} else if !strings.Contains(cliPdAddr, "http://") {
				return errors.New("PD endpoint scheme should be http")
			}
			grpcTLSOption, err := credential.ToGRPCDialOption()
			if err != nil {
				return errors.Annotate(err, "fail to validate TLS settings")
			}

			pdEndpoints := strings.Split(cliPdAddr, ",")
			etcdCli, err := clientv3.New(clientv3.Config{
				Context:     defaultContext,
				Endpoints:   pdEndpoints,
				TLS:         tlsConfig,
				DialTimeout: 30 * time.Second,
				DialOptions: []grpc.DialOption{
					grpcTLSOption,
					grpc.WithBlock(),
					grpc.WithConnectParams(grpc.ConnectParams{
						Backoff: backoff.Config{
							BaseDelay:  time.Second,
							Multiplier: 1.1,
							Jitter:     0.1,
							MaxDelay:   3 * time.Second,
						},
						MinConnectTimeout: 3 * time.Second,
					}),
				},
			})
			if err != nil {
				// PD embeds an etcd server.
				return errors.Annotate(err, "fail to open PD etcd client")
			}
			cdcEtcdCli = kv.NewCDCEtcdClient(etcdCli)
			pdCli, err = pd.NewClientWithContext(
				defaultContext, pdEndpoints, credential.PDSecurityOption(),
				pd.WithGRPCDialOptions(
					grpcTLSOption,
					grpc.WithBlock(),
					grpc.WithConnectParams(grpc.ConnectParams{
						Backoff: backoff.Config{
							BaseDelay:  time.Second,
							Multiplier: 1.1,
							Jitter:     0.1,
							MaxDelay:   3 * time.Second,
						},
						MinConnectTimeout: 3 * time.Second,
					}),
				))
			if err != nil {
				return errors.Annotate(err, "fail to open PD client")
			}
			ctx := defaultContext
			errorTiKVIncompatible := true // Error if TiKV is incompatible.
			err = util.CheckClusterVersion(ctx, pdCli, pdEndpoints[0], credential, errorTiKVIncompatible)
			if err != nil {
				return err
			}

			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			if interact {
				loop()
			}
		},
	}
	command.AddCommand(
		newCaptureCommand(),
		newChangefeedCommand(),
		newProcessorCommand(),
		newUnsafeCommand(),
		newTsoCommand(),
	)

	return command
}

func loop() {
	l, err := readline.NewEx(&readline.Config{
		Prompt:            "\033[31m»\033[0m ",
		HistoryFile:       "/tmp/readline.tmp",
		InterruptPrompt:   "^C",
		EOFPrompt:         "^D",
		HistorySearchFold: true,
	})
	if err != nil {
		panic(err)
	}
	defer l.Close()

	for {
		line, err := l.Readline()
		if err != nil {
			if err == readline.ErrInterrupt {
				break
			} else if err == io.EOF {
				break
			}
			continue
		}
		if line == "exit" {
			os.Exit(0)
		}
		args, err := shellwords.Parse(line)
		if err != nil {
			fmt.Printf("parse command err: %v\n", err)
			continue
		}

		command := newCliCommand()
		command.SetArgs(args)
		_ = command.ParseFlags(args)
		command.SetOutput(os.Stdout)
		if err = command.Execute(); err != nil {
			command.Println(err)
		}
	}
}
