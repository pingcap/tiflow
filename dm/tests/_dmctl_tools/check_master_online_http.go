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
	"fmt"
	"io"
	"net/http"
	"os"

	"github.com/pingcap/dm/tests/utils"

	toolutils "github.com/pingcap/tidb-tools/pkg/utils"
)

// use show-ddl-locks request to test DM-master is online
func main() {
	addr := os.Args[1]
	sslCA := ""
	sslCert := ""
	sslKey := ""
	transport := http.DefaultTransport.(*http.Transport).Clone()

	if len(os.Args) == 5 {
		sslCA = os.Args[2]
		sslCert = os.Args[3]
		sslKey = os.Args[4]

		tls, err := toolutils.NewTLS(sslCA, sslCert, sslKey, "", nil)
		if err != nil {
			utils.ExitWithError(err)
		}

		tlsCfg := tls.TLSConfig()
		tlsCfg.InsecureSkipVerify = true
		transport.TLSClientConfig = tlsCfg
	}

	client := &http.Client{Transport: transport}

	resp, err := client.Get("https://" + addr + "/status")
	if err != nil {
		utils.ExitWithError(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		utils.ExitWithError(err)
	}
	fmt.Println(string(body))
}
