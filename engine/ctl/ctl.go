// Copyright 2022 PingCAP, Inc.
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

package ctl

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/pingcap/tiflow/engine/client"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// newRootCmd registers all the sub-commands.
func newRootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:           "tfctl",
		Short:         "TiFlow Command Tools",
		SilenceUsage:  true,
		SilenceErrors: true,
		CompletionOptions: cobra.CompletionOptions{
			DisableDefaultCmd: true,
		},
	}
	cmd.AddCommand(newSubmitJob())
	cmd.AddCommand(newQueryJob())
	cmd.AddCommand(newPauseJob())
	helpCmd := &cobra.Command{
		Use:   "help [command]",
		Short: "Gets help about any commands",
		Long: `Help provides help for any command in the application.
Simply type ` + cmd.Name() + ` help [path to command] for full details.`,

		Run: func(c *cobra.Command, args []string) {
			cmd2, _, e := c.Root().Find(args)
			if cmd2 == nil || e != nil {
				c.Printf("Unknown help topic %#q\n", args)
				e = c.Root().Usage()
			} else {
				cmd2.InitDefaultHelpFlag() // make possible 'help' flag to be shown
				e = cmd2.Help()
			}
			if e != nil {
				log.L().Fatal("error occurs when printing help info", zap.Error(e))
			}
		},
	}
	cmd.SetHelpCommand(helpCmd)
	return cmd
}

type config struct {
	flagSet *pflag.FlagSet

	MasterAddrs string `toml:"master-addr"`

	RPCTimeoutStr string `toml:"rpc-timeout"`
	RPCTimeout    time.Duration

	ConfigFile string
}

var (
	cltManager        = client.NewClientManager()
	defaultRPCTimeout = "30s"
	rpcTimeout        = 30 * time.Second
)

// we gotta analyze the flagset for all flags in all commands.
func defineConfigFlagSet(fs *pflag.FlagSet) {
	fs.BoolP("version", "V", false, "Prints version and exit.")
	fs.String("config", "", "Path to config file.")
	fs.String("master-addr", "", "Master API server address, this parameter is required when interacting with the dm-master")
	fs.String("rpc-timeout", defaultRPCTimeout, fmt.Sprintf("RPC timeout, default is %s.", defaultRPCTimeout))
}

func (c *config) getConfigFromFlagSet() error {
	var err error
	fs := c.flagSet
	c.ConfigFile, err = fs.GetString("config")
	if err != nil {
		return err
	}
	c.MasterAddrs, err = fs.GetString("master-addr")
	if err != nil {
		return err
	}
	c.RPCTimeoutStr, err = fs.GetString("rpc-timeout")
	if err != nil {
		return err
	}
	return nil
}

func (c *config) Adjust() error {
	err := c.getConfigFromFlagSet()
	if err != nil {
		return errors.Trace(err)
	}

	if c.ConfigFile != "" {
		err = c.configFromFile(c.ConfigFile)
		// TODO: Use err pkg.
		if err != nil {
			return err
		}
	}

	if c.MasterAddrs == "" {
		c.MasterAddrs = os.Getenv("TIFLOW_MASTER_ADDR")
	}

	if c.MasterAddrs == "" {
		return errors.New("master addr not found")
	}
	if c.RPCTimeoutStr == "" {
		c.RPCTimeoutStr = defaultRPCTimeout
	}
	timeout, err := time.ParseDuration(c.RPCTimeoutStr)
	if err != nil {
		return errors.Trace(err)
	}
	if timeout <= time.Duration(0) {
		return errors.Errorf("invalid time duration: %s", c.RPCTimeoutStr)
	}
	c.RPCTimeout = timeout
	return nil
}

// configFromFile loads config from file.
func (c *config) configFromFile(path string) error {
	_, err := toml.DecodeFile(path, c)
	return err
}

// Init initializes dm-control.
func Init(ctx context.Context, cfg *config) error {
	// set the log level temporarily
	log.SetLevel(zapcore.InfoLevel)
	rpcTimeout = cfg.RPCTimeout

	endpoints := strings.Split(cfg.MasterAddrs, ",")
	ctx1, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()
	log.L().Info("dialing master", zap.Any("addr", endpoints))
	err := cltManager.AddMasterClient(ctx1, endpoints)
	if err != nil {
		return err
	}
	log.L().Info("dialing master successfully")
	return nil
}

// MainStart runs root command
func MainStart(ctx context.Context, args []string) {
	rootCmd := newRootCmd()
	rootCmd.RunE = func(cmd *cobra.Command, args []string) error {
		return cmd.Help()
	}
	rootCmd.PersistentPreRunE = func(cmd *cobra.Command, args []string) error {
		if printVersion, err := cmd.Flags().GetBool("version"); err != nil {
			return errors.Trace(err)
		} else if printVersion {
			cmd.Println(utils.GetRawInfo())
			os.Exit(0)
		}
		cfg := newConfig(cmd.Flags())
		err := cfg.Adjust()
		if err != nil {
			return err
		}

		return Init(ctx, cfg)
	}

	defineConfigFlagSet(rootCmd.PersistentFlags())
	rootCmd.SetArgs(args)
	if c, err := rootCmd.ExecuteC(); err != nil {
		rootCmd.Println("Error:", err)
		if c.CalledAs() == "" {
			rootCmd.Printf("Run '%v --help' for usage.\n", c.CommandPath())
		}
		os.Exit(1)
	}
}

// newConfig creates a new base config for dmctl.
func newConfig(fs *pflag.FlagSet) *config {
	cfg := &config{}
	cfg.flagSet = fs
	return cfg
}
