// Copyright 2024 PingCAP, Inc.
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

package upstream

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb-dashboard/util/distro"
	"github.com/pingcap/tidb-dashboard/util/netutil"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tiflow/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const (
	// topologyTiCDC is /topology/ticdc/{clusterID}/{ip:port}.
	topologyTiCDC = "/topology/ticdc/%s/%s"
	// topologyTiDB is /topology/tidb/{ip:port}.
	// Refer to https://github.com/pingcap/tidb/blob/release-7.5/pkg/domain/infosync/info.go#L78-L79.
	topologyTiDB    = infosync.TopologyInformationPath
	topologyTiDBTTL = infosync.TopologySessionTTL
	// defaultTimeout is the default timeout for etcd and mysql operations.
	defaultTimeout = time.Second * 2
)

type tidbInstance struct {
	IP   string
	Port uint
}

// fetchTiDBTopology parses the TiDB topology from etcd.
func fetchTiDBTopology(ctx context.Context, etcdClient *clientv3.Client) ([]tidbInstance, error) {
	ctx2, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()

	resp, err := etcdClient.Get(ctx2, topologyTiDB, clientv3.WithPrefix())
	if err != nil {
		return nil, errors.ErrPDEtcdAPIError.Wrap(err)
	}

	nodesAlive := make(map[string]struct{}, len(resp.Kvs))
	nodesInfo := make(map[string]*tidbInstance, len(resp.Kvs))

	for _, kv := range resp.Kvs {
		key := string(kv.Key)
		if !strings.HasPrefix(key, topologyTiDB) {
			continue
		}
		// remainingKey looks like `ip:port/info` or `ip:port/ttl`.
		remainingKey := strings.TrimPrefix(key[len(topologyTiDB):], "/")
		keyParts := strings.Split(remainingKey, "/")
		if len(keyParts) != 2 {
			log.Warn("Ignored invalid topology key", zap.String("component", distro.R().TiDB), zap.String("key", key))
			continue
		}

		switch keyParts[1] {
		case "info":
			address := keyParts[0]
			hostname, port, err := netutil.ParseHostAndPortFromAddress(address)
			if err != nil {
				log.Warn("Ignored invalid tidb topology info entry",
					zap.String("key", key),
					zap.String("value", string(kv.Value)),
					zap.Error(err))
				continue
			}
			nodesInfo[keyParts[0]] = &tidbInstance{
				IP:   hostname,
				Port: port,
			}
		case "ttl":
			alive, err := parseTiDBAliveness(kv.Value)
			if !alive || err != nil {
				log.Warn("Ignored invalid tidb topology TTL entry",
					zap.String("key", key),
					zap.String("value", string(kv.Value)),
					zap.Error(err))
				continue
			}
			nodesAlive[keyParts[0]] = struct{}{}
		}
	}

	nodes := make([]tidbInstance, 0)
	for addr, info := range nodesInfo {
		if _, ok := nodesAlive[addr]; ok {
			nodes = append(nodes, *info)
		}
	}
	return nodes, nil
}

func parseTiDBAliveness(value []byte) (bool, error) {
	unixTimestampNano, err := strconv.ParseUint(string(value), 10, 64)
	if err != nil {
		return false, errors.ErrUnmarshalFailed.Wrap(err)
	}
	t := time.Unix(0, int64(unixTimestampNano))
	if time.Since(t) > topologyTiDBTTL*time.Second {
		return false, nil
	}
	return true, nil
}
