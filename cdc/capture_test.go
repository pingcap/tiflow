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

package cdc

import (
	"context"
	"net/url"
	"sync"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/cdc/kv"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/etcd"
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/ticdc/pkg/util/testleak"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/embed"
	"golang.org/x/sync/errgroup"
)

type captureSuite struct {
	e         *embed.Etcd
	clientURL *url.URL
	client    kv.CDCEtcdClient
	ctx       context.Context
	cancel    context.CancelFunc
	errg      *errgroup.Group
}

var _ = check.Suite(&captureSuite{})

func (s *captureSuite) SetUpTest(c *check.C) {
	dir := c.MkDir()
	var err error
	s.clientURL, s.e, err = etcd.SetupEmbedEtcd(dir)
	c.Assert(err, check.IsNil)
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{s.clientURL.String()},
		DialTimeout: 3 * time.Second,
	})
	c.Assert(err, check.IsNil)
	s.client = kv.NewCDCEtcdClient(context.Background(), client)
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.errg = util.HandleErrWithErrGroup(s.ctx, s.e.Err(), func(e error) { c.Log(e) })
}

func (s *captureSuite) TearDownTest(c *check.C) {
	s.e.Close()
	s.cancel()
	err := s.errg.Wait()
	if err != nil {
		c.Errorf("Error group error: %s", err)
	}
	s.client.Close() //nolint:errcheck
}

func (s *captureSuite) TestCaptureSuicide(c *check.C) {
	defer testleak.AfterTest(c)()
	defer s.TearDownTest(c)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	capture, err := NewCapture(ctx, []string{s.clientURL.String()}, nil,
		&security.Credential{}, "127.0.0.1:12034",
		&processorOpts{flushCheckpointInterval: time.Millisecond * 200})
	c.Assert(err, check.IsNil)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := capture.Run(ctx)
		c.Assert(cerror.ErrCaptureSuicide.Equal(err), check.IsTrue)
	}()
	// ttl is 5s, wait 1s to ensure `capture.Run` starts
	time.Sleep(time.Second)
	_, err = s.client.Client.Revoke(ctx, capture.session.Lease())
	c.Assert(err, check.IsNil)
	wg.Wait()

	err = capture.etcdClient.Close()
	if err != nil {
		c.Assert(errors.Cause(err), check.Equals, context.Canceled)
	}
}
