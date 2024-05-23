// Copyright 2019 PingCAP, Inc.
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
	"context"
	"os"
	"time"

	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tiflow/dm/pb"
	"github.com/pingcap/tiflow/dm/tests/utils"
	"google.golang.org/grpc"
)

// use show-ddl-locks request to test DM-master is online
func main() {
	addr := os.Args[1]

	secureOpt := grpc.WithInsecure()
	if len(os.Args) == 5 {
		sslCA := os.Args[2]
		sslCert := os.Args[3]
		sslKey := os.Args[4]
		tls, err := common.NewTLS(sslCA, sslCert, sslKey, "", nil, nil, nil)
		if err != nil {
			utils.ExitWithError(err)
		}
		secureOpt = tls.ToGRPCDialOption()
	}

	conn, err := grpc.Dial(addr, secureOpt, grpc.WithBackoffMaxDelay(2*time.Second))
	if err != nil {
		utils.ExitWithError(err)
	}
	cli := pb.NewMasterClient(conn)
	req := &pb.ShowDDLLocksRequest{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	_, err = cli.ShowDDLLocks(ctx, req)
	cancel()
	if err != nil {
		utils.ExitWithError(err)
	}
}
