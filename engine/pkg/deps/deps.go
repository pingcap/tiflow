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

package deps

import (
	"reflect"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/dig"
	"go.uber.org/zap"
)

type Deps struct {
	container *dig.Container
}

func NewDeps() *Deps {
	return &Deps{
		container: dig.New(),
	}
}

func (d *Deps) Provide(constructor interface{}) error {
	return d.container.Provide(constructor)
}

// Construct takes a function in the form of
// `func(arg1 Type1, arg2 Type2,...) (ret, error)`.
// The arguments to the function is automatically filled with
// the dependency injection functionality.
func (d *Deps) Construct(fn interface{}) (interface{}, error) {
	fnTp := reflect.TypeOf(fn)
	if fnTp.NumOut() != 2 {
		log.L().Panic("Unexpected input type", zap.Any("type", reflect.TypeOf(fn)))
	}

	var in, out []reflect.Type
	for i := 0; i < fnTp.NumIn(); i++ {
		in = append(in, fnTp.In(i))
	}
	out = append(out, fnTp.Out(1))

	invokeFnTp := reflect.FuncOf(in, out, false)

	var obj reflect.Value
	invokeFn := reflect.MakeFunc(invokeFnTp, func(args []reflect.Value) (results []reflect.Value) {
		retVals := reflect.ValueOf(fn).Call(args)
		obj = retVals[0]
		return retVals[1:]
	})

	if err := d.container.Invoke(invokeFn.Interface()); err != nil {
		return nil, errors.Trace(err)
	}

	return obj.Interface(), nil
}

func (d *Deps) Fill(params interface{}) error {
	invokeFnTp := reflect.FuncOf(
		[]reflect.Type{reflect.TypeOf(params).Elem()},
		[]reflect.Type{reflect.TypeOf(new(error))},
		false)
	invokeFn := reflect.MakeFunc(invokeFnTp, func(args []reflect.Value) (results []reflect.Value) {
		defer func() {
			if v := recover(); v != nil {
				results = []reflect.Value{reflect.ValueOf(errors.Errorf("internal error: %v", v))}
			}
		}()
		reflect.ValueOf(params).Elem().Set(args[0])
		return []reflect.Value{reflect.ValueOf(new(error))}
	})
	if err := d.container.Invoke(invokeFn.Interface()); err != nil {
		return errors.Trace(err)
	}
	return nil
}
