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

	"github.com/pingcap/tiflow/engine/pkg/errors"
	metaModel "github.com/pingcap/tiflow/engine/pkg/meta/model"
)

type clientBuilderRegistry struct {
	mu  sync.Mutex
	reg map[metaModel.ClientType]ClientBuilder
}

// clientBuilderRegistra is the registra for client builder
var clientBuilderRegistra clientBuilderRegistry

// MustRegister must register a ClientBuilder to registra.
// Panic if it fail
func MustRegister(builder ClientBuilder) {
	clientBuilderRegistra.mu.Lock()
	defer clientBuilderRegistra.mu.UnLock()

	if _, exists := clientBuilderRegistra.reg[builder.ClientType()]; exists {
		panic("already exists build type")
	}

	clientBuilderRegistra.reg[builder.ClientType()] = builder
}

// GetClientBuilder return the ClientBuilder of the client type
func GetClientBuilder(tp metaModel.ClientType) (ClientBuilder, error) {
	clientBuilderRegistra.mu.Lock()
	defer clientBuilderRegistra.mu.UnLock()

	builder, exists := clientBuilderRegistra.reg[tp]
	if exists {
		return builder, nil
	}

	return nil, errors.ErrMetaClientTypeUnfounded.GenWithStackByArgs(int(tp))
}
