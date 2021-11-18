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

package master

import (
	"net/http"
	"os"
	"time"

	"go.etcd.io/etcd/embed"
	"google.golang.org/grpc"

	"github.com/hanfei1991/microcosom/pkg/terror"
)

const (
	// time waiting for etcd to be started.
	etcdStartTimeout = time.Minute
	// privateDirMode grants owner to make/remove files inside the directory.
	privateDirMode os.FileMode = 0o700
)

// startEtcd starts an embedded etcd server.
func startEtcd(etcdCfg *embed.Config,
	gRPCSvr func(*grpc.Server),
	httpHandles map[string]http.Handler, startTimeout time.Duration) (*embed.Etcd, error) {
	// attach extra gRPC and HTTP server
	if gRPCSvr != nil {
		etcdCfg.ServiceRegister = gRPCSvr
	}
	if httpHandles != nil {
		etcdCfg.UserHandlers = httpHandles
	}

	e, err := embed.StartEtcd(etcdCfg)
	if err != nil {
		return nil, terror.ErrMasterStartEmbedEtcdFail.Delegate(err)
	}

	select {
	case <-e.Server.ReadyNotify():
	}
	return e, nil
}

// isDirExist returns whether the directory is exist.
func isDirExist(d string) bool {
	if stat, err := os.Stat(d); err == nil && stat.IsDir() {
		return true
	}
	return false
}
