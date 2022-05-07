// Copyright 2022 PingCAP, Inc.
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

package etcdkv

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/url"
	"time"

	"github.com/phayes/freeport"
	"go.etcd.io/etcd/server/v3/embed"
)

func allocTempURL() (string, error) {
	port, err := freeport.GetFreePort()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("http://127.0.0.1:%d", port), nil
}

func MockBackendEtcd() (*embed.Etcd, string, error) {
	cfg := embed.NewConfig()
	tmpDir := "embedded-etcd"
	dir, err := ioutil.TempDir("", tmpDir)
	if err != nil {
		return nil, "", err
	}
	cfg.Dir = dir
	peers, err := allocTempURL()
	if err != nil {
		return nil, "", err
	}
	log.Printf("Allocate server peer port is %s", peers)
	u, err := url.Parse(peers)
	if err != nil {
		return nil, "", err
	}
	cfg.LPUrls = []url.URL{*u}
	advertises, err := allocTempURL()
	if err != nil {
		return nil, "", err
	}
	log.Printf("Allocate server advertises port is %s", advertises)
	u, err = url.Parse(advertises)
	if err != nil {
		return nil, "", err
	}
	cfg.LCUrls = []url.URL{*u}
	svr, err := embed.StartEtcd(cfg)
	if err != nil {
		return nil, "", err
	}
	select {
	case <-svr.Server.ReadyNotify():
		log.Printf("Server is ready!")
	case <-time.After(60 * time.Second):
		svr.Server.Stop() // trigger a shutdown
		svr.Close()
		return nil, "", errors.New("embedded etcd start fail")
	}

	return svr, advertises, nil
}

func CloseEmbededEtcd(svr *embed.Etcd) {
	if svr != nil {
		svr.Server.Stop()
		svr.Close()
	}
}
