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

package dataset

import (
	"context"
	"encoding/json"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/engine/pkg/adapter"
	"github.com/pingcap/tiflow/engine/pkg/meta/metaclient"
	derror "github.com/pingcap/tiflow/pkg/errors"
)

// DataSet is a generic layer for using CRUD patterns with KV-backed storage.
// nolint:structcheck
type DataSet[E any, T DataEntry[E]] struct {
	metaclient metaclient.KV
	keyPrefix  adapter.KeyAdapter
}

// DataEntry is a type constraint for individual records.
type DataEntry[E any] interface {
	GetID() string
	*E
}

// NewDataSet returns a new DataSet.
func NewDataSet[E any, T DataEntry[E]](metaclient metaclient.KV, keyPrefix adapter.KeyAdapter) *DataSet[E, T] {
	return &DataSet[E, T]{
		metaclient: metaclient,
		keyPrefix:  keyPrefix,
	}
}

// Get point-gets a record by ID.
func (d *DataSet[E, T]) Get(ctx context.Context, id string) (T, error) {
	getResp, kvErr := d.metaclient.Get(ctx, d.getKey(id))
	if kvErr != nil {
		return nil, errors.Trace(kvErr)
	}

	if len(getResp.Kvs) == 0 {
		return nil, derror.ErrDatasetEntryNotFound.GenWithStackByArgs(d.getKey(id))
	}
	rawBytes := getResp.Kvs[0].Value

	var retVal E
	if err := json.Unmarshal(rawBytes, &retVal); err != nil {
		return nil, errors.Trace(err)
	}
	return &retVal, nil
}

// Upsert updates or inserts a record.
func (d *DataSet[E, T]) Upsert(ctx context.Context, entry T) error {
	rawBytes, err := json.Marshal(entry)
	if err != nil {
		return errors.Trace(err)
	}

	if _, err := d.metaclient.Put(ctx, d.getKey(entry.GetID()), string(rawBytes)); err != nil {
		return err
	}
	return nil
}

// Delete removes a record.
func (d *DataSet[E, T]) Delete(ctx context.Context, id string) error {
	if _, err := d.metaclient.Delete(ctx, d.getKey(id)); err != nil {
		return err
	}
	return nil
}

// LoadAll loads all records.
func (d *DataSet[E, T]) LoadAll(ctx context.Context) ([]T, error) {
	getResp, kvErr := d.metaclient.Get(ctx, d.keyPrefix.Path(), metaclient.WithPrefix())
	if kvErr != nil {
		return nil, errors.Trace(kvErr)
	}

	var ret []T
	for _, kv := range getResp.Kvs {
		rawBytes := kv.Value
		var val E
		if err := json.Unmarshal(rawBytes, &val); err != nil {
			return nil, errors.Trace(err)
		}
		ret = append(ret, &val)
	}
	return ret, nil
}

func (d *DataSet[E, T]) getKey(id string) string {
	return d.keyPrefix.Encode(id)
}
