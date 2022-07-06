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

	metaModel "github.com/pingcap/tiflow/engine/pkg/meta/model"
	"github.com/pingcap/tiflow/pkg/errors"
)

type clientBuilderRegistry struct {
	mu  sync.Mutex
	reg map[metaModel.ClientType]clientBuilder
}

// clientBuilderRegistra is the registra for client builder
var clientBuilderRegistra clientBuilderRegistry

// MustRegister must register a clientBuilder to registra.
// Panic if it fail
func MustRegister(builder clientBuilder) {
	clientBuilderRegistra.mu.Lock()
	defer clientBuilderRegistra.mu.Unlock()

	if _, exists := clientBuilderRegistra.reg[builder.ClientType()]; exists {
		panic("already exists build type")
	}

	clientBuilderRegistra.reg[builder.ClientType()] = builder
}

// GetClientBuilder return the clientBuilder of the client type
func GetClientBuilder(tp metaModel.ClientType) (clientBuilder, error) {
	clientBuilderRegistra.mu.Lock()
	defer clientBuilderRegistra.mu.Unlock()

	builder, exists := clientBuilderRegistra.reg[tp]
	if exists {
		return builder, nil
	}

	return nil, errors.ErrMetaClientTypeUnfounded.GenWithStackByArgs(int(tp))
}
