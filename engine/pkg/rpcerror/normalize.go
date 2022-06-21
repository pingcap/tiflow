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
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync"

	"github.com/pingcap/errors"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"

	"github.com/pingcap/tiflow/dm/pkg/log"
	pb "github.com/pingcap/tiflow/engine/enginepb"
)

type normalizeOpts struct {
	name string
	msg  string
}

// NormalizeOpt is an option to Normalize.
type NormalizeOpt func(opts *normalizeOpts)

// WithMessage provides a custom error message.
func WithMessage(msg string) NormalizeOpt {
	return func(opts *normalizeOpts) {
		opts.msg = msg
	}
}

// WithName overrides the default error name, which
// is the type name of the type argument to Normalize.
func WithName(name string) NormalizeOpt {
	return func(opts *normalizeOpts) {
		opts.name = name
	}
}

var prototypeRegistry = &sync.Map{}

// Prototype is a generator for all instances
// of a given kind of errors.
type Prototype[E errorInfo] struct {
	msg  string
	name string
}

// Normalize registers E as the information struct for an error,
// and returns a Prototype for this kind of error.
func Normalize[E errorInfo](ops ...NormalizeOpt) *Prototype[E] {
	var opts normalizeOpts
	for _, op := range ops {
		op(&opts)
	}

	if opts.name == "" {
		var e E
		opts.name = reflect.TypeOf(e).Name()
	}

	ret := &Prototype[E]{
		msg:  opts.msg,
		name: opts.name,
	}

	_, exists := prototypeRegistry.LoadOrStore(opts.name, ret)
	if exists {
		log.L().Warn("duplicate error type", zap.String("name", opts.name))
	}

	return ret
}

// Gen generates a stackless instance of the given error.
func (p *Prototype[E]) Gen(e *E) error {
	return &normalizedError[E]{
		prototype: p,
		inner:     *e,
	}
}

// GenWithStack is similar to Gen but attaches the call stack to it.
func (p *Prototype[E]) GenWithStack(e *E) error {
	return errors.WithStack(p.Gen(e))
}

// Is returns whether errIn is an instance of the prototype.
func (p *Prototype[E]) Is(errIn error) bool {
	normalized, ok := tryUnwrapNormalizedError(errIn)
	if !ok {
		return false
	}

	_, ok = normalized.(*normalizedError[E])
	return ok
}

// Convert converts errIn to the type argument provided to Normalize.
func (p *Prototype[E]) Convert(errIn error) (*E, bool) {
	normalized, ok := tryUnwrapNormalizedError(errIn)
	if !ok {
		return nil, false
	}

	typedNormalized, ok := normalized.(*normalizedError[E])
	if !ok {
		return nil, false
	}

	return &typedNormalized.inner, true
}

func (p *Prototype[E]) fromJSONBytes(jsonBytes []byte) (typeErasedNormalizedError, error) {
	var e E
	if err := json.Unmarshal(jsonBytes, &e); err != nil {
		return nil, errors.Annotate(err, "failed to unmarshal error info")
	}

	return &normalizedError[E]{
		prototype: p,
		inner:     e,
	}, nil
}

type normalizedError[E errorInfo] struct {
	prototype *Prototype[E]
	inner     E
}

func (e *normalizedError[E]) isRetryable() bool {
	if r, ok := any(e.inner).(retryablity); ok {
		return r.isRetryable()
	}

	// Not retryable by default.
	return false
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
	jsonBytes, err := json.Marshal(e.inner)
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

type typeErasedNormalizedError interface {
	toPB() *pb.ErrorV2
	statusCode() codes.Code
	message() string
	isRetryable() bool
	Error() string
}

type jsonDeserializer interface {
	fromJSONBytes(jsonBytes []byte) (typeErasedNormalizedError, error)
}
