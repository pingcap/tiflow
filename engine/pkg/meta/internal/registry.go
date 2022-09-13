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

package internal

import (
	"sync"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/engine/pkg/meta/internal/etcdkv"
	"github.com/pingcap/tiflow/engine/pkg/meta/internal/mockkv"
	"github.com/pingcap/tiflow/engine/pkg/meta/internal/sqlkv"
	metaModel "github.com/pingcap/tiflow/engine/pkg/meta/model"
	"github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

func init() {
	MustRegisterClientBuilder(&mockkv.ClientBuilderImpl{})
	MustRegisterClientBuilder(&etcdkv.ClientBuilderImpl{})
	MustRegisterClientBuilder(&sqlkv.ClientBuilderImpl{})
}

// clientBuilderRegistra is the registra for client builder
var clientBuilderRegistra = NewRegistry()

// GlobalClientBuilderRegistra return the global clientBuilderRegistry
func GlobalClientBuilderRegistra() *clientBuilderRegistry {
	return clientBuilderRegistra
}

type clientBuilderRegistry struct {
	mu  sync.Mutex
	reg map[metaModel.ClientType]clientBuilder
}

// NewRegistry return a new clientBuilderRegistry
func NewRegistry() *clientBuilderRegistry {
	return &clientBuilderRegistry{
		reg: make(map[metaModel.ClientType]clientBuilder),
	}
}

// MustRegisterClientBuilder must register a clientBuilder to registra.
// Panic if it fail
func MustRegisterClientBuilder(builder clientBuilder) {
	r := GlobalClientBuilderRegistra()
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.reg[builder.ClientType()]; exists {
		log.Panic("client type is already existed", zap.Any("client-type", builder.ClientType()))
	}

	r.reg[builder.ClientType()] = builder
}

// GetClientBuilder return the clientBuilder of the client type
func GetClientBuilder(tp metaModel.ClientType) (clientBuilder, error) {
	r := GlobalClientBuilderRegistra()
	r.mu.Lock()
	defer r.mu.Unlock()

	builder, exists := r.reg[tp]
	if exists {
		return builder, nil
	}

	return nil, errors.ErrMetaClientTypeNotSupport.GenWithStackByArgs(tp.String())
}
