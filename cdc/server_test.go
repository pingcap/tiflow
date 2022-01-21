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
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tidb/br/pkg/httputil"
	"github.com/pingcap/tiflow/cdc/capture"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cerrors "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/pingcap/tiflow/pkg/retry"
	security2 "github.com/pingcap/tiflow/pkg/security"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/tempurl"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/embed"
	"golang.org/x/sync/errgroup"
)

type testServer struct {
	server    *Server
	e         *embed.Etcd
	clientURL *url.URL
	ctx       context.Context
	cancel    context.CancelFunc
	errg      *errgroup.Group
}

func newServer(t *testing.T) *testServer {
	var err error
	dir := t.TempDir()
	s := &testServer{}
	s.clientURL, s.e, err = etcd.SetupEmbedEtcd(dir)
	require.Nil(t, err)

	pdEndpoints := []string{
		"http://" + s.clientURL.Host,
		"http://invalid-pd-host:2379",
	}
	server, err := NewServer(pdEndpoints)
	require.Nil(t, err)
	require.NotNil(t, server)
	s.server = server

	s.ctx, s.cancel = context.WithCancel(context.Background())
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   s.server.pdEndpoints,
		Context:     s.ctx,
		DialTimeout: 5 * time.Second,
	})
	require.Nil(t, err)
	etcdClient := etcd.NewCDCEtcdClient(s.ctx, client)
	s.server.etcdClient = &etcdClient

	s.errg = util.HandleErrWithErrGroup(s.ctx, s.e.Err(), func(e error) { t.Log(e) })
	return s
}

func (s *testServer) close(t *testing.T) {
	s.server.Close()
	s.e.Close()
	s.cancel()
	err := s.errg.Wait()
	if err != nil {
		t.Errorf("Error group error: %s", err)
	}
}

func TestServerBasic(t *testing.T) {
	t.Parallel()
	s := newServer(t)
	defer s.close(t)
	testEtcdHealthChecker(t, s)
	testSetUpDataDir(t, s)
}

func testEtcdHealthChecker(t *testing.T, s *testServer) {
	s.errg.Go(func() error {
		err := s.server.etcdHealthChecker(s.ctx)
		require.Equal(t, context.Canceled, err)
		return nil
	})
	// longer than one check tick 3s
	time.Sleep(time.Second * 4)
}

func testSetUpDataDir(t *testing.T, s *testServer) {
	conf := config.GetGlobalServerConfig()
	// DataDir is not set, and no changefeed exist, use the default
	conf.DataDir = ""
	err := s.server.setUpDir(s.ctx)
	require.Nil(t, err)
	require.Equal(t, defaultDataDir, conf.DataDir)
	require.Equal(t, filepath.Join(defaultDataDir, config.DefaultSortDir), conf.Sorter.SortDir)

	// DataDir is not set, but has existed changefeed, use the one with the largest available space
	conf.DataDir = ""
	dir := t.TempDir()
	err = s.server.etcdClient.SaveChangeFeedInfo(s.ctx, &model.ChangeFeedInfo{SortDir: dir}, "a")
	require.Nil(t, err)

	err = s.server.etcdClient.SaveChangeFeedInfo(s.ctx, &model.ChangeFeedInfo{}, "b")
	require.Nil(t, err)

	err = s.server.setUpDir(s.ctx)
	require.Nil(t, err)

	require.Equal(t, dir, conf.DataDir)
	require.Equal(t, filepath.Join(dir, config.DefaultSortDir), conf.Sorter.SortDir)

	conf.DataDir = t.TempDir()
	// DataDir has been set, just use it
	err = s.server.setUpDir(s.ctx)
	require.Nil(t, err)
	require.NotEqual(t, "", conf.DataDir)
	require.Equal(t, filepath.Join(conf.DataDir, config.DefaultSortDir), conf.Sorter.SortDir)
}

func TestCheckDir(t *testing.T) {
	me, err := user.Current()
	require.Nil(t, err)
	if me.Name == "root" || runtime.GOOS == "windows" {
		t.Skip("test case is running as a superuser or in windows")
	}

	dir := t.TempDir()
	_, err = checkDir(dir)
	require.Nil(t, err)

	readOnly := filepath.Join(dir, "readOnly")
	err = os.MkdirAll(readOnly, 0o400)
	require.Nil(t, err)
	_, err = checkDir(readOnly)
	require.Error(t, err)

	writeOnly := filepath.Join(dir, "writeOnly")
	err = os.MkdirAll(writeOnly, 0o200)
	require.Nil(t, err)
	_, err = checkDir(writeOnly)
	require.Error(t, err)

	file := filepath.Join(dir, "file")
	f, err := os.Create(file)
	require.Nil(t, err)
	require.Nil(t, f.Close())
	_, err = checkDir(file)
	require.Error(t, err)
}

const retryTime = 20

func TestServerTLSWithoutCommonName(t *testing.T) {
	addr := tempurl.Alloc()[len("http://"):]
	// Do not specify common name
	security, err := security2.NewCredential4Test("")
	require.Nil(t, err)
	conf := config.GetDefaultServerConfig()
	conf.Addr = addr
	conf.AdvertiseAddr = addr
	conf.Security = &security
	config.StoreGlobalServerConfig(conf)

	server, err := NewServer([]string{"https://127.0.0.1:2379"})
	server.capture = capture.NewCapture4Test(false)
	require.Nil(t, err)
	err = server.startStatusHTTP(server.tcpServer.HTTP1Listener())
	require.Nil(t, err)
	defer func() {
		require.Nil(t, server.statusServer.Close())
	}()

	statusURL := fmt.Sprintf("https://%s/api/v1/status", addr)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := server.tcpServer.Run(ctx)
		require.Contains(t, err.Error(), "ErrTCPServerClosed")
	}()

	// test cli sends request without a cert will success
	err = retry.Do(ctx, func() error {
		tr := &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		}
		cli := &http.Client{Transport: tr}
		resp, err := cli.Get(statusURL)
		if err != nil {
			return err
		}
		decoder := json.NewDecoder(resp.Body)
		captureInfo := &model.CaptureInfo{}
		err = decoder.Decode(captureInfo)
		require.Nil(t, err)
		require.Equal(t, server.capture.Info().ID, captureInfo.ID)
		resp.Body.Close()
		return nil
	}, retry.WithMaxTries(retryTime), retry.WithBackoffBaseDelay(50), retry.WithIsRetryableErr(cerrors.IsRetryableError))
	require.Nil(t, err)

	// test cli sends request with a cert will success
	err = retry.Do(ctx, func() error {
		tlsConfig, err := security.ToTLSConfigWithVerify()
		require.Nil(t, err)

		cli := httputil.NewClient(tlsConfig)
		resp, err := cli.Get(statusURL)
		if err != nil {
			return err
		}
		decoder := json.NewDecoder(resp.Body)
		captureInfo := &model.CaptureInfo{}
		err = decoder.Decode(captureInfo)
		require.Nil(t, err)
		require.Equal(t, server.capture.Info().ID, captureInfo.ID)
		resp.Body.Close()
		return nil
	}, retry.WithMaxTries(retryTime), retry.WithBackoffBaseDelay(50), retry.WithIsRetryableErr(cerrors.IsRetryableError))
	require.Nil(t, err)

	cancel()
	wg.Wait()
}

func TestServerTLSWithCommonName(t *testing.T) {
	addr := tempurl.Alloc()[len("http://"):]
	// specify a common name
	security, err := security2.NewCredential4Test("test")
	require.Nil(t, err)
	conf := config.GetDefaultServerConfig()
	conf.Addr = addr
	conf.AdvertiseAddr = addr
	conf.Security = &security
	config.StoreGlobalServerConfig(conf)

	server, err := NewServer([]string{"https://127.0.0.1:2379"})
	server.capture = capture.NewCapture4Test(false)
	require.Nil(t, err)
	err = server.startStatusHTTP(server.tcpServer.HTTP1Listener())
	require.Nil(t, err)
	defer func() {
		require.Nil(t, server.statusServer.Close())
	}()

	statusURL := fmt.Sprintf("https://%s/api/v1/status", addr)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := server.tcpServer.Run(ctx)
		require.Contains(t, err.Error(), "ErrTCPServerClosed")
	}()

	// test cli sends request without a cert will fail
	err = retry.Do(ctx, func() error {
		tr := &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		}
		cli := &http.Client{Transport: tr}
		resp, err := cli.Get(statusURL)
		if err != nil {
			return err
		}
		decoder := json.NewDecoder(resp.Body)
		captureInfo := &model.CaptureInfo{}
		err = decoder.Decode(captureInfo)
		require.Nil(t, err)
		require.Equal(t, server.capture.Info().ID, captureInfo.ID)
		resp.Body.Close()
		return nil
	}, retry.WithMaxTries(retryTime), retry.WithBackoffBaseDelay(50), retry.WithIsRetryableErr(cerrors.IsRetryableError))
	require.Contains(t, err.Error(), "remote error: tls: bad certificate")

	// test cli sends request with a cert will success
	err = retry.Do(ctx, func() error {
		tlsConfig, err := security.ToTLSConfigWithVerify()
		require.Nil(t, err)

		cli := httputil.NewClient(tlsConfig)
		resp, err := cli.Get(statusURL)
		if err != nil {
			return err
		}
		decoder := json.NewDecoder(resp.Body)
		captureInfo := &model.CaptureInfo{}
		err = decoder.Decode(captureInfo)
		require.Nil(t, err)
		require.Equal(t, server.capture.Info().ID, captureInfo.ID)
		resp.Body.Close()
		return nil
	}, retry.WithMaxTries(retryTime), retry.WithBackoffBaseDelay(50), retry.WithIsRetryableErr(cerrors.IsRetryableError))
	require.Nil(t, err)
}
