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

package etcd

import (
	"net/url"
	"time"

	"github.com/pingcap/errors"
	"github.com/tikv/pd/pkg/tempurl"
	"go.etcd.io/etcd/embed"
)

// getFreeListenURLs get free ports and localhost as url.
func getFreeListenURLs(n int) (urls []*url.URL, retErr error) {
	for i := 0; i < n; i++ {
		u, err := url.Parse(tempurl.Alloc())
		if err != nil {
			retErr = errors.Trace(err)
			return
		}
		urls = append(urls, u)
	}

	return
}

// SetupEmbedEtcd starts an embed etcd server
func SetupEmbedEtcd(dir string) (clientURL *url.URL, e *embed.Etcd, err error) {
	cfg := embed.NewConfig()
	cfg.Dir = dir

	urls, err := getFreeListenURLs(2)
	if err != nil {
		return
	}
	cfg.LPUrls = []url.URL{*urls[0]}
	cfg.LCUrls = []url.URL{*urls[1]}
	cfg.Logger = "zap"
	clientURL = urls[1]

	e, err = embed.StartEtcd(cfg)
	if err != nil {
		return
	}

	select {
	case <-e.Server.ReadyNotify():
	case <-time.After(60 * time.Second):
		e.Server.Stop() // trigger a shutdown
		err = errors.New("server took too long to start")
	}

	return
}
