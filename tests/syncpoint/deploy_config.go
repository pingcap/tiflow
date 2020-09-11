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

package main

import (
	"bytes"
	"flag"
	"fmt"
	"os"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/tests/util"
)

func main() {
	if len(os.Args) != 5 {
		log.Info("wrong args,need three args!")
		os.Exit(2)
	}
	primaryTs := os.Args[1]
	secondaryTs := os.Args[2]
	diffConfig := util.NewDiffConfig()
	err := diffConfig.Parse(os.Args[3:])
	switch errors.Cause(err) {
	case nil:
	case flag.ErrHelp:
		os.Exit(0)
	default:
		log.S().Errorf("parse cmd flags err %s\n", err)
		os.Exit(2)
	}
	// if len(os.Args) != 3 {
	// 	log.S().Errorf("wrong args,need two args %s\n", errors.New("wrong args"))
	// 	os.Exit(2)
	// }
	diffConfig.SourceDBCfg[0].Snapshot = primaryTs
	diffConfig.TargetDBCfg.Snapshot = secondaryTs
	buf := new(bytes.Buffer)
	if err := toml.NewEncoder(buf).Encode(diffConfig); err != nil {
		log.Fatal("someting wrong")
	}
	fmt.Println(buf.String())
}
