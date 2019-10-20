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

package etcd

import (
	"net/url"
	"strconv"
	"time"

	"github.com/coreos/etcd/embed"
	"github.com/phayes/freeport"
	"github.com/pingcap/errors"
)

// getFreeListenURLs get free ports and localhost as url.
func getFreeListenURLs(n int) (urls []*url.URL, err error) {
	ports, err2 := freeport.GetFreePorts(n)
	if err != nil {
		err = errors.Trace(err2)
		return
	}

	for _, port := range ports {
		u, err2 := url.Parse("http://localhost:" + strconv.Itoa(port))
		if err2 != nil {
			err = errors.Trace(err2)
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
	clientURL = urls[1]

	e, err = embed.StartEtcd(cfg)

	select {
	case <-e.Server.ReadyNotify():
	case <-time.After(60 * time.Second):
		e.Server.Stop() // trigger a shutdown
		err = errors.New("Server took too long to start!")
	}

	return
}
