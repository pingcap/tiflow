package internal

import (
	"github.com/pingcap/tiflow/engine/pkg/errors"
	"github.com/pingcap/tiflow/engine/pkg/meta/metaclient"
)

type clientBuilderRegistra map[metaclient.ClientType]ClientBuilder

func MustRegister(builder ClientBuilder) {
	if _, exists := clientBuilderRegistra[builder.ClientType()]; exists {
		panic("already exists build type")
	}

	clientBuilderRegistra[builder.ClientType()] = builder
}

func GetClientBuilder(tp metaclient.ClientType) (ClientBuilder, error) {
	builder, exists := clientBuilderRegistra[tp]
	if exists {
		return builder, nil
	}

	return nil, errors.ErrMetaClientTypeUnfounded.GenWithStackByArgs(int(tp))
}
