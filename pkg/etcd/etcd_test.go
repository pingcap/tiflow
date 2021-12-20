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
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/tiflow/pkg/util/testleak"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/embed"
)

func Test(t *testing.T) { check.TestingT(t) }

type etcdSuite struct {
	etcd      *embed.Etcd
	clientURL *url.URL
}

var _ = check.Suite(&etcdSuite{})

// Set up a embeded etcd using free ports.
func (s *etcdSuite) SetUpTest(c *check.C) {
	dir := c.MkDir()
	curl, e, err := SetupEmbedEtcd(dir)
	c.Assert(err, check.IsNil)
	s.clientURL = curl
	s.etcd = e
}

func (s *etcdSuite) TearDownTest(c *check.C) {
	s.etcd.Close()
logEtcdError:
	for {
		select {
		case err := <-s.etcd.Err():
			c.Logf("etcd server error: %v", err)
		default:
			break logEtcdError
		}
	}
}

func (s *etcdSuite) TestEmbedEtcd(c *check.C) {
	defer testleak.AfterTest(c)()
	curl := s.clientURL.String()
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{curl},
		DialTimeout: 3 * time.Second,
	})
	c.Assert(err, check.IsNil)
	defer cli.Close()

	var (
		key = "test-key"
		val = "test-val"
	)
	_, err = cli.Put(context.Background(), key, val)
	c.Assert(err, check.IsNil)
	resp, err2 := cli.Get(context.Background(), key)
	c.Assert(err2, check.IsNil)
	c.Assert(resp.Kvs, check.HasLen, 1)
	c.Assert(resp.Kvs[0].Value, check.DeepEquals, []byte(val))
	s.TearDownTest(c)
}
