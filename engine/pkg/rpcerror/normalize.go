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

package rpcerror

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/bytedance/sonic"
	"github.com/pingcap/errors"
	"google.golang.org/grpc/codes"

	pb "github.com/pingcap/tiflow/engine/enginepb"
)

type normalizeOpts struct {
	name string
	msg  string
}

type NormalizeOpt func(opts *normalizeOpts)

func WithMessage(msg string) NormalizeOpt {
	return func(opts *normalizeOpts) {
		opts.msg = msg
	}
}

func WithName(name string) NormalizeOpt {
	return func(opts *normalizeOpts) {
		opts.name = name
	}
}

type Prototype[E errorInfo] struct {
	msg  string
	name string
}

func Normalize[E errorInfo](ops ...NormalizeOpt) *Prototype[E] {
	var opts normalizeOpts
	for _, op := range ops {
		op(&opts)
	}

	if opts.name == "" {
		var e E
		opts.name = reflect.TypeOf(e).Name()
	}

	return &Prototype[E]{
		msg:  opts.msg,
		name: opts.name,
	}
}

func (p *Prototype[E]) Gen(e *E) error {
	return &normalizedError[E]{
		prototype: p,
		inner:     *e,
	}
}

func (p *Prototype[E]) GenWithStack(e *E) error {
	return errors.WithStack(p.Gen(e))
}

type normalizedError[E errorInfo] struct {
	prototype *Prototype[E]
	inner     E
}

func (e *normalizedError[E]) name() string {
	return e.prototype.name
}

func (e *normalizedError[E]) message() string {
	return e.prototype.msg
}

func (e *normalizedError[E]) Error() string {
	jsonBytes := e.mustMarshalJSON()

	var builder strings.Builder
	builder.WriteString(e.name())
	builder.WriteByte(':')
	builder.WriteByte(' ')

	if e.message() != "" {
		builder.WriteString(e.message())
		builder.WriteByte(' ')
	}
	builder.Write(jsonBytes)
	return builder.String()
}

func (e *normalizedError[E]) mustMarshalJSON() []byte {
	// Sonic is used for better performance,
	// in case Error() is called on some critical path.
	jsonBytes, err := sonic.Marshal(e.inner)
	if err != nil {
		panic(fmt.Sprintf("marshaling json for %s failed: %s", e.name(), err.Error()))
	}

	return jsonBytes
}

func (e *normalizedError[E]) toPB() *pb.ErrorV2 {
	return &pb.ErrorV2{
		Name:    e.name(),
		Details: e.mustMarshalJSON(),
	}
}

func (e *normalizedError[E]) statusCode() codes.Code {
	if statusCoder, ok := any(e.inner).(grpcStatusCoder); ok {
		return statusCoder.grpcStatusCode()
	}

	return codes.Unknown
}
