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

package server

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

	"github.com/golang/mock/gomock"
	"github.com/pingcap/tiflow/cdc/capture"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cerrors "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/etcd"
	mock_etcd "github.com/pingcap/tiflow/pkg/etcd/mock"
	"github.com/pingcap/tiflow/pkg/httputil"
	"github.com/pingcap/tiflow/pkg/retry"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/utils/tempurl"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"golang.org/x/sync/errgroup"
)

type testServer struct {
	server    *server
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
	server, err := New(pdEndpoints)
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

	etcdClient, err := etcd.NewCDCEtcdClient(s.ctx, client, etcd.DefaultCDCClusterID)
	require.Nil(t, err)
	s.server.etcdClient = etcdClient

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
	testSetUpDataDir(t, s)
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
	err = s.server.etcdClient.SaveChangeFeedInfo(s.ctx,
		&model.ChangeFeedInfo{SortDir: dir}, model.DefaultChangeFeedID("a"))
	require.Nil(t, err)

	err = s.server.etcdClient.SaveChangeFeedInfo(s.ctx,
		&model.ChangeFeedInfo{}, model.DefaultChangeFeedID("b"))
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
	_, securityCfg, err := security.NewServerCredential4Test("")
	require.Nil(t, err)
	conf := config.GetDefaultServerConfig()
	conf.Addr = addr
	conf.AdvertiseAddr = addr
	conf.Security = securityCfg
	config.StoreGlobalServerConfig(conf)

	server, err := New([]string{"https://127.0.0.1:2379"})
	cp := capture.NewCapture4Test(nil)
	ctrl := gomock.NewController(t)
	etcdClient := mock_etcd.NewMockCDCEtcdClient(ctrl)
	etcdClient.EXPECT().GetClusterID().Return("abcd").AnyTimes()
	cp.EtcdClient = etcdClient
	server.capture = cp
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
		require.ErrorContains(t, err, "ErrTCPServerClosed")
	}()

	// test cli sends request without a cert will success
	err = retry.Do(ctx, func() error {
		tr := &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		}
		cli := httputil.NewTestClient(tr)
		resp, err := cli.Get(ctx, statusURL)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		decoder := json.NewDecoder(resp.Body)
		captureInfo := &model.CaptureInfo{}
		err = decoder.Decode(captureInfo)
		require.Nil(t, err)
		info, err := server.capture.Info()
		require.Nil(t, err)
		require.Equal(t, info.ID, captureInfo.ID)
		return nil
	}, retry.WithMaxTries(retryTime), retry.WithBackoffBaseDelay(50),
		retry.WithIsRetryableErr(cerrors.IsRetryableError))
	require.Nil(t, err)

	// test cli sends request with a cert will success
	err = retry.Do(ctx, func() error {
		cli, err := httputil.NewClient(securityCfg)
		require.Nil(t, err)
		resp, err := cli.Get(ctx, statusURL)
		if err != nil {
			return err
		}
		decoder := json.NewDecoder(resp.Body)
		captureInfo := &model.CaptureInfo{}
		err = decoder.Decode(captureInfo)
		require.Nil(t, err)
		info, err := server.capture.Info()
		require.Nil(t, err)
		require.Equal(t, info.ID, captureInfo.ID)
		resp.Body.Close()
		return nil
	}, retry.WithMaxTries(retryTime), retry.WithBackoffBaseDelay(50),
		retry.WithIsRetryableErr(cerrors.IsRetryableError))
	require.Nil(t, err)

	cancel()
	wg.Wait()
}

func TestServerTLSWithCommonNameAndRotate(t *testing.T) {
	addr := tempurl.Alloc()[len("http://"):]
	// specify a common name
	ca, securityCfg, err := security.NewServerCredential4Test("server")
	securityCfg.CertAllowedCN = append(securityCfg.CertAllowedCN, "client1")
	require.Nil(t, err)

	conf := config.GetDefaultServerConfig()
	conf.Addr = addr
	conf.AdvertiseAddr = addr
	conf.Security = securityCfg
	config.StoreGlobalServerConfig(conf)

	server, err := New([]string{"https://127.0.0.1:2379"})
	cp := capture.NewCapture4Test(nil)
	ctrl := gomock.NewController(t)
	etcdClient := mock_etcd.NewMockCDCEtcdClient(ctrl)
	etcdClient.EXPECT().GetClusterID().Return("abcd").AnyTimes()
	cp.EtcdClient = etcdClient
	server.capture = cp
	require.Nil(t, err)
	err = server.startStatusHTTP(server.tcpServer.HTTP1Listener())
	require.Nil(t, err)
	defer func() {
		require.Nil(t, server.statusServer.Close())
	}()

	statusURL := fmt.Sprintf("https://%s/api/v1/status", addr)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := server.tcpServer.Run(ctx)
		require.ErrorContains(t, err, "ErrTCPServerClosed")
	}()

	// test cli sends request without a cert will fail
	err = retry.Do(ctx, func() error {
		tr := &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		}
		cli := httputil.NewTestClient(tr)
		require.Nil(t, err)
		resp, err := cli.Get(ctx, statusURL)
		if err != nil {
			return err
		}
		decoder := json.NewDecoder(resp.Body)
		captureInfo := &model.CaptureInfo{}
		err = decoder.Decode(captureInfo)
		require.Nil(t, err)
		info, err := server.capture.Info()
		require.Nil(t, err)
		require.Equal(t, info.ID, captureInfo.ID)
		resp.Body.Close()
		return nil
	}, retry.WithMaxTries(retryTime), retry.WithBackoffBaseDelay(50),
		retry.WithIsRetryableErr(cerrors.IsRetryableError))
	require.ErrorContains(t, err, "remote error: tls: bad certificate")

	testTlSClient := func(securityCfg *security.Credential) error {
		return retry.Do(ctx, func() error {
			cli, err := httputil.NewClient(securityCfg)
			require.Nil(t, err)
			resp, err := cli.Get(ctx, statusURL)
			if err != nil {
				return err
			}
			decoder := json.NewDecoder(resp.Body)
			captureInfo := &model.CaptureInfo{}
			err = decoder.Decode(captureInfo)
			require.Nil(t, err)
			info, err := server.capture.Info()
			require.Nil(t, err)
			require.Equal(t, info.ID, captureInfo.ID)
			resp.Body.Close()
			return nil
		}, retry.WithMaxTries(retryTime), retry.WithBackoffBaseDelay(50),
			retry.WithIsRetryableErr(cerrors.IsRetryableError))
	}

	// test cli sends request with a cert will success

	// test peer success
	require.NoError(t, testTlSClient(securityCfg))

	// test rotate
	serverCert, serverkey, err := ca.GenerateCerts("rotate")
	require.NoError(t, err)
	err = os.WriteFile(securityCfg.CertPath, serverCert, 0o600)
	require.NoError(t, err)
	err = os.WriteFile(securityCfg.KeyPath, serverkey, 0o600)
	require.NoError(t, err)
	// peer fail due to invalid common name `rotate`
	require.ErrorContains(t, testTlSClient(securityCfg), "client certificate authentication failed")

	cert, key, err := ca.GenerateCerts("client1")
	require.NoError(t, err)
	certPath, err := security.WriteFile("ticdc-test-client-cert", cert)
	require.NoError(t, err)
	keyPath, err := security.WriteFile("ticdc-test-client-key", key)
	require.NoError(t, err)
	require.NoError(t, testTlSClient(&security.Credential{
		CAPath:        securityCfg.CAPath,
		CertPath:      certPath,
		KeyPath:       keyPath,
		CertAllowedCN: []string{"rotate"},
	}))

	cancel()
	wg.Wait()
}
