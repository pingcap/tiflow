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
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/phayes/freeport"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/retry"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"
)

func allocTempURL() (string, error) {
	port, err := freeport.GetFreePort()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("http://127.0.0.1:%d", port), nil
}

// RetryMockBackendEtcd retry to create backend DB if meet 'address already in use' error
// for at most 3 times.
func RetryMockBackendEtcd() (s *embed.Etcd, addr string, err error) {
	err = retry.Do(context.TODO(), func() error {
		s, addr, err = mockBackendEtcd()
		if err != nil {
			return err
		}
		return nil
	},
		retry.WithBackoffBaseDelay(1000 /* 1000 ms */),
		retry.WithBackoffMaxDelay(3000 /* 3 seconds */),
		retry.WithMaxTries(3 /* fail after 10 seconds*/),
		retry.WithIsRetryableErr(func(err error) bool {
			if strings.Contains(err.Error(), "address already in use") {
				log.Info("address already in use, retry again")
				return true
			}
			return false
		}),
	)

	return
}

// mockBackendEtcd mock the etcd using embedded etcd as backend storage
func mockBackendEtcd() (*embed.Etcd, string, error) {
	failpoint.Inject("MockEtcdAddressAlreadyUse", func() {
		failpoint.Return(nil, "", errors.New("address already in use"))
	})

	cfg := embed.NewConfig()
	tmpDir := "embedded-etcd"
	dir, err := os.MkdirTemp("", tmpDir)
	if err != nil {
		return nil, "", err
	}
	cfg.Dir = dir
	peers, err := allocTempURL()
	if err != nil {
		return nil, "", err
	}
	log.Info("Allocate server peer port", zap.String("peers", peers))
	u, err := url.Parse(peers)
	if err != nil {
		return nil, "", err
	}
	cfg.LPUrls = []url.URL{*u}
	advertises, err := allocTempURL()
	if err != nil {
		return nil, "", err
	}
	log.Info("Allocate server advertises port", zap.String("advertises", advertises))
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
		log.Info("Server is ready!")
	case <-time.After(60 * time.Second):
		svr.Server.Stop() // trigger a shutdown
		svr.Close()
		return nil, "", errors.New("embedded etcd start fail")
	}

	return svr, advertises, nil
}

// CloseEmbededEtcd close the input embedded etcd server
func CloseEmbededEtcd(svr *embed.Etcd) {
	if svr != nil {
		svr.Server.Stop()
		svr.Close()
	}
}
