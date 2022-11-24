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

package server

import (
	"context"
	"encoding/json"

	"github.com/pingcap/log"
	pb "github.com/pingcap/tiflow/engine/enginepb"
	"github.com/pingcap/tiflow/engine/pkg/client"
	"github.com/pingcap/tiflow/engine/pkg/meta"
	metaModel "github.com/pingcap/tiflow/engine/pkg/meta/model"
	"github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// MetastoreManager maintains all metastore clients we need.
// Except for FrameworkStore and BusinessClientConn,
// a MetastoreManager is not thread-safe.
//
// TODO refactor some code repetition together with servermaster.MetaStoreManager,
// and add integration tests between MetastoreManager in this file and servermaster.MetaStoreManager.
type MetastoreManager interface {
	// Init fetches metastore configurations from Servermaster and
	// creates the necessary client.
	// Init is made part of the interface because the interface is intended
	// to reflect the dependency between the objects during server initialization.
	// NOTE: Init must be called before other methods can be.
	Init(ctx context.Context, discoveryClient client.DiscoveryClient) error
	IsInitialized() bool
	Close()

	FrameworkClientConn() metaModel.ClientConn
	BusinessClientConn() metaModel.ClientConn
}

// NewMetastoreManager returns a new MetastoreManager.
// Note that Init() should be called first before using it.
func NewMetastoreManager() MetastoreManager {
	return &metastoreManagerImpl{
		creator: metastoreCreatorImpl{},
	}
}

// metastoreManagerImpl implements MetastoreManager.
// We make the implementation private because it
// is the only one implementation used in production code.
type metastoreManagerImpl struct {
	initialized atomic.Bool

	frameworkClientConn metaModel.ClientConn
	businessClientConn  metaModel.ClientConn

	creator MetastoreCreator
}

// MetastoreCreator abstracts creation behavior of the various
// metastore clients.
type MetastoreCreator interface {
	CreateClientConnForBusiness(
		ctx context.Context, params metaModel.StoreConfig,
	) (metaModel.ClientConn, error)

	CreateClientConnForFramework(
		ctx context.Context, params metaModel.StoreConfig,
	) (metaModel.ClientConn, error)
}

type metastoreCreatorImpl struct{}

func (c metastoreCreatorImpl) CreateClientConnForBusiness(
	_ context.Context, params metaModel.StoreConfig,
) (metaModel.ClientConn, error) {
	cc, err := meta.NewClientConn(&params)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return cc, nil
}

func (c metastoreCreatorImpl) CreateClientConnForFramework(
	_ context.Context, params metaModel.StoreConfig,
) (metaModel.ClientConn, error) {
	cc, err := meta.NewClientConn(&params)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return cc, err
}

func (m *metastoreManagerImpl) Init(ctx context.Context, discoveryClient client.DiscoveryClient) (retErr error) {
	if m.initialized.Load() {
		log.Panic("MetastoreManager: double Init")
	}

	defer func() {
		// Close all store clients in case the final return value
		// is not nil.
		if retErr != nil {
			m.Close()
		}
	}()

	if err := m.initFrameworkStore(ctx, discoveryClient); err != nil {
		return err
	}

	if err := m.initBusinessStore(ctx, discoveryClient); err != nil {
		return err
	}

	m.initialized.Store(true)
	return nil
}

func (m *metastoreManagerImpl) IsInitialized() bool {
	return m.initialized.Load()
}

func (m *metastoreManagerImpl) initFrameworkStore(ctx context.Context, discoveryClient client.DiscoveryClient) error {
	// Query framework metastore endpoints.
	resp, err := discoveryClient.QueryMetaStore(
		ctx,
		&pb.QueryMetaStoreRequest{Tp: pb.StoreType_SystemMetaStore})
	if err != nil {
		return errors.Trace(err)
	}

	conf := parseStoreConfig(resp.Config)
	cc, err := m.creator.CreateClientConnForFramework(ctx, conf)
	if err != nil {
		return err
	}

	log.Info("Obtained framework metastore endpoint", zap.Any("endpoints", conf.Endpoints),
		zap.Any("schema", conf.Schema), zap.Any("username", conf.User))
	m.frameworkClientConn = cc
	return nil
}

func (m *metastoreManagerImpl) initBusinessStore(ctx context.Context, discoveryClient client.DiscoveryClient) error {
	// fetch business metastore connection endpoint
	resp, err := discoveryClient.QueryMetaStore(
		ctx,
		&pb.QueryMetaStoreRequest{Tp: pb.StoreType_AppMetaStore})
	if err != nil {
		return err
	}

	conf := parseStoreConfig(resp.Config)
	cc, err := m.creator.CreateClientConnForBusiness(ctx, conf)
	if err != nil {
		return err
	}

	log.Info("Obtained framework metastore endpoint", zap.Any("endpoints", conf.Endpoints),
		zap.Any("schema", conf.Schema), zap.Any("username", conf.User))
	m.businessClientConn = cc
	return nil
}

func (m *metastoreManagerImpl) FrameworkClientConn() metaModel.ClientConn {
	if !m.initialized.Load() {
		log.Panic("FrameworkStore is called before Init is successful")
	}
	return m.frameworkClientConn
}

func (m *metastoreManagerImpl) BusinessClientConn() metaModel.ClientConn {
	if !m.initialized.Load() {
		log.Panic("BusinessClientConn is called before Init is successful")
	}
	return m.businessClientConn
}

func (m *metastoreManagerImpl) Close() {
	if m.frameworkClientConn != nil {
		_ = m.frameworkClientConn.Close()
		m.frameworkClientConn = nil
	}

	if m.businessClientConn != nil {
		_ = m.businessClientConn.Close()
		m.businessClientConn = nil
	}

	log.Info("MetastoreManager: Closed all metastores")
}

func parseStoreConfig(rawBytes []byte) metaModel.StoreConfig {
	var conf metaModel.StoreConfig

	// Try unmarshal as json first.
	err := json.Unmarshal(rawBytes, &conf)
	if err == nil {
		return conf
	}

	log.Info("Could not unmarshal metastore config, fallback to treating it as an endpoint list")

	conf.SetEndpoints(string(rawBytes))
	return conf
}
