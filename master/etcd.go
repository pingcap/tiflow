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
	case <-time.After(startTimeout):
		// if fail to startup, the etcd server may be still blocking in
		// https://github.com/etcd-io/etcd/blob/3cf2f69b5738fb702ba1a935590f36b52b18979b/embed/serve.go#L92
		// then `e.Close` will block in
		// https://github.com/etcd-io/etcd/blob/3cf2f69b5738fb702ba1a935590f36b52b18979b/embed/etcd.go#L377
		// because `close(sctx.serversC)` has not been called in
		// https://github.com/etcd-io/etcd/blob/3cf2f69b5738fb702ba1a935590f36b52b18979b/embed/serve.go#L200.
		// so for `ReadyNotify` timeout, we choose to only call `e.Server.Stop()` now,
		// and we should exit the DM-master process after returned with error from this function.
		e.Server.Stop()
		return nil, terror.ErrMasterStartEmbedEtcdFail.Generatef("start embed etcd timeout %v", startTimeout)
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
