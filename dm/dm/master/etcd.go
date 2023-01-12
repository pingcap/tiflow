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
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	toolutils "github.com/pingcap/tidb-tools/pkg/utils"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/pingcap/tiflow/dm/pkg/etcdutil"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
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
	httpHandles map[string]http.Handler, startTimeout time.Duration,
) (*embed.Etcd, error) {
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

// prepareJoinEtcd prepares config needed to join an existing cluster.
// learn from https://github.com/pingcap/pd/blob/37efcb05f397f26c70cda8dd44acaa3061c92159/server/join/join.go#L44.
//
// when setting `initial-cluster` explicitly to bootstrap a new cluster:
// - if local persistent data exist, just restart the previous cluster (in fact, it's not bootstrapping).
// - if local persistent data not exist, just bootstrap the cluster as a new cluster.
//
// when setting `join` to join an existing cluster (without `initial-cluster` set):
// - if local persistent data exists (in fact, it's not join):
//   - just restart if `member` already exists (already joined before)
//   - read `initial-cluster` back from local persistent data to restart (just like bootstrapping)
//
// - if local persistent data not exist:
//  1. fetch member list from the cluster to check if we can join now.
//  2. call `member add` to add the member info into the cluster.
//  3. generate config for join (`initial-cluster` and `initial-cluster-state`).
//  4. save `initial-cluster` in local persistent data for later restarting.
//
// NOTE: A member can't join to another cluster after it has joined a previous one.
func prepareJoinEtcd(cfg *Config) error {
	// no need to join
	if cfg.Join == "" {
		return nil
	}

	// try to join self, invalid
	if cfg.Join == cfg.AdvertiseAddr {
		return terror.ErrMasterJoinEmbedEtcdFail.Generate(fmt.Sprintf("join self %s is forbidden", cfg.Join))
	}

	// restart with previous data, no `InitialCluster` need to set
	// ref: https://github.com/etcd-io/etcd/blob/ae9734ed278b7a1a7dfc82e800471ebbf9fce56f/etcdserver/server.go#L313
	if isDirExist(filepath.Join(cfg.DataDir, "member", "wal")) {
		cfg.InitialCluster = ""
		cfg.InitialClusterState = embed.ClusterStateFlagExisting
		return nil
	}

	// join with persistent data
	joinFP := filepath.Join(cfg.DataDir, "join")
	if s, err := os.ReadFile(joinFP); err != nil {
		if !os.IsNotExist(err) {
			return terror.ErrMasterJoinEmbedEtcdFail.Delegate(err, "read persistent join data")
		}
	} else {
		cfg.InitialCluster = strings.TrimSpace(string(s))
		cfg.InitialClusterState = embed.ClusterStateFlagExisting
		log.L().Info("using persistent join data", zap.String("file", joinFP), zap.String("data", cfg.InitialCluster))
		return nil
	}

	tlsCfg, err := toolutils.ToTLSConfig(cfg.SSLCA, cfg.SSLCert, cfg.SSLKey)
	if err != nil {
		return terror.ErrMasterJoinEmbedEtcdFail.Delegate(err, "generate tls config")
	}

	// if without previous data, we need a client to contact with the existing cluster.
	client, err := etcdutil.CreateClient(strings.Split(cfg.Join, ","), tlsCfg)
	if err != nil {
		return terror.ErrMasterJoinEmbedEtcdFail.Delegate(err, fmt.Sprintf("create etcd client for %s", cfg.Join))
	}
	defer client.Close()

	// `member list`
	listResp, err := etcdutil.ListMembers(client)
	if err != nil {
		return terror.ErrMasterJoinEmbedEtcdFail.Delegate(err, fmt.Sprintf("list member for %s", cfg.Join))
	}

	// check members
	for _, m := range listResp.Members {
		if m.Name == "" { // the previous existing member without name (not complete the join operation)
			// we can't generate `initial-cluster` correctly with empty member name,
			// and if added a member but not started it to complete the join,
			// the later join operation may encounter `etcdserver: re-configuration failed due to not enough started members`.
			return terror.ErrMasterJoinEmbedEtcdFail.Generate("there is a member that has not joined successfully, continue the join or remove it")
		}
		if m.Name == cfg.Name {
			// a failed DM-master re-joins the previous cluster.
			return terror.ErrMasterJoinEmbedEtcdFail.Generate(fmt.Sprintf("missing data or joining a duplicate member %s", m.Name))
		}
	}

	// `member add`, a new/deleted DM-master joins to an existing cluster.
	addResp, err := etcdutil.AddMember(client, strings.Split(cfg.AdvertisePeerUrls, ","))
	if err != nil {
		return terror.ErrMasterJoinEmbedEtcdFail.Delegate(err, fmt.Sprintf("add member %s", cfg.AdvertisePeerUrls))
	}

	// generate `--initial-cluster`
	ms := make([]string, 0, len(addResp.Members))
	for _, m := range addResp.Members {
		name := m.Name
		if m.ID == addResp.Member.ID {
			// the member only called `member add`,
			// but has not started the process to complete the join should have an empty name.
			// so, we use the `name` in config instead.
			name = cfg.Name
		}
		if name == "" {
			// this should be checked in the previous `member list` operation if having only one member is join.
			// if multi join operations exist, the behavior may be unexpected.
			// check again here only to decrease the unexpectedness.
			return terror.ErrMasterJoinEmbedEtcdFail.Generate("there is a member that has not joined successfully, continue the join or remove it")
		}
		for _, url := range m.PeerURLs {
			ms = append(ms, fmt.Sprintf("%s=%s", name, url))
		}
	}
	cfg.InitialCluster = strings.Join(ms, ",")
	cfg.InitialClusterState = embed.ClusterStateFlagExisting

	// save `--initial-cluster` in persist data
	if err = os.MkdirAll(cfg.DataDir, privateDirMode); err != nil && !os.IsExist(err) {
		return terror.ErrMasterJoinEmbedEtcdFail.Delegate(err, "make directory")
	}
	if err = os.WriteFile(joinFP, []byte(cfg.InitialCluster), privateDirMode); err != nil {
		return terror.ErrMasterJoinEmbedEtcdFail.Delegate(err, "write persistent join data")
	}

	return nil
}

// isDirExist returns whether the directory is exist.
func isDirExist(d string) bool {
	if stat, err := os.Stat(d); err == nil && stat.IsDir() {
		return true
	}
	return false
}
