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
	"fmt"

	"github.com/pingcap/tiflow/pkg/cmd/factory"
	"github.com/pingcap/tiflow/pkg/cmd/util"
	"github.com/spf13/cobra"
)

type configureCredentialsOptions struct{}

func (o *configureCredentialsOptions) run(cmd *cobra.Command) error {
	cmd.Println("1) TLS Client Certificate")
	cmd.Println("2) TiDB User Credentials")
	cmd.Print("Select Credential Type [default 1]:")

	option, err := readInput()
	if err != nil {
		cmd.Printf("Received invalid input: %s, abort the command.\n", err.Error())
		return fmt.Errorf("invalid input")
	}
	if option == "" {
		option = "1"
	}

	res, err := factory.ReadFromDefaultPath()
	if err != nil {
		mag := "the default config file contains invalid data"
		return fmt.Errorf("%s: %w", mag, err)
	}

	switch option {
	case "1":
		cmd.Printf("CA Certificate path [%s]:", res.CaPath)
		caPath, err := readInput()
		if err != nil {
			cmd.Printf("Received invalid input: %s, abort the command.\n", err.Error())
			return fmt.Errorf("invalid input")
		}
		if caPath != "" {
			res.CaPath = caPath
		}

		cmd.Printf("Client Certificate path [%s]:", res.CertPath)
		certPath, err := readInput()
		if err != nil {
			cmd.Printf("Received invalid input: %s, abort the command.\n", err.Error())
			return fmt.Errorf("invalid input")
		}
		if certPath != "" {
			res.CertPath = certPath
		}

		cmd.Printf("Client Private Key path [%s]:", res.KeyPath)
		keyPath, err := readInput()
		if err != nil {
			cmd.Printf("Received invalid input: %s, abort the command.\n", err.Error())
			return fmt.Errorf("invalid input")
		}
		if keyPath != "" {
			res.KeyPath = keyPath
		}
	case "2":
		cmd.Printf("TiCDC User name [%s]:", res.User)
		user, err := readInput()
		if err != nil {
			cmd.Printf("Received invalid input: %s, abort the command.\n", err.Error())
			return fmt.Errorf("invalid input")
		}
		if user != "" {
			res.User = user
		}

		cmd.Printf("TiCDC Password  [%s]:", res.Password)
		password, err := readInput()
		if err != nil {
			cmd.Printf("Received invalid input: %s, abort the command.\n", err.Error())
			return fmt.Errorf("invalid input")
		}
		if password != "" {
			res.Password = password
		}
	default:
		cmd.Printf("Received invalid input: %s, abort the command.\n", option)
		return fmt.Errorf("invalid input")
	}

	return res.StoreToDefaultPath()
}

// newConfigureCredentials creates the `cli configure-credentials` command
func newConfigureCredentials() *cobra.Command {
	o := &configureCredentialsOptions{}

	command := &cobra.Command{
		Use:   "configure-credentials",
		Short: "Configure tls or authentication credentials",
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			util.CheckErr(o.run(cmd))
		},
	}

	return command
}
