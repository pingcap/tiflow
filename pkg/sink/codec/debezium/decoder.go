// Copyright 2024 PingCAP, Inc.
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

package debezium

import (
	"context"
	"database/sql"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/errors"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"github.com/pingcap/tiflow/pkg/util"
	"go.uber.org/zap"
)

// Decoder implement the RowEventDecoder interface
type Decoder struct {
	config *common.Config

	upstreamTiDB *sql.DB
	storage      storage.ExternalStorage

	msg   *message
	value []byte
}

// NewDecoder returns a new Decoder
func NewDecoder(ctx context.Context, config *common.Config, db *sql.DB) (*Decoder, error) {
	var (
		externalStorage storage.ExternalStorage
		err             error
	)
	if config.LargeMessageHandle.EnableClaimCheck() {
		storageURI := config.LargeMessageHandle.ClaimCheckStorageURI
		externalStorage, err = util.GetExternalStorage(ctx, storageURI, nil, util.NewS3Retryer(10, 10*time.Second, 10*time.Second))
		if err != nil {
			return nil, cerror.WrapError(cerror.ErrKafkaInvalidConfig, err)
		}
	}

	if config.LargeMessageHandle.HandleKeyOnly() && db == nil {
		return nil, cerror.ErrCodecDecode.
			GenWithStack("handle-key-only is enabled, but upstream TiDB is not provided")
	}

	return &Decoder{
		config:       config,
		storage:      externalStorage,
		upstreamTiDB: db,
	}, errors.Trace(err)
}

// AddKeyValue add the received key and values to the Decoder,
func (d *Decoder) AddKeyValue(_, value []byte) (err error) {
	if d.value != nil {
		return cerror.ErrCodecDecode.GenWithStack(
			"Decoder value already exists, not consumed yet")
	}
	d.value, err = common.Decompress(d.config.LargeMessageHandle.LargeMessageHandleCompression, value)
	return err
}

// HasNext returns whether there is any event need to be consumed
func (d *Decoder) HasNext() (model.MessageType, bool, error) {
	if d.value == nil {
		return model.MessageTypeUnknown, false, nil
	}
	m := new(message)

	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	err := json.Unmarshal(d.value, &m)
	if err != nil {
		return model.MessageTypeUnknown, false, cerror.WrapError(cerror.ErrDecodeFailed, err)
	}
	d.msg = m
	d.value = nil

	if d.msg.Payload.Op != "" {
		return model.MessageTypeRow, true, nil
	}

	if d.msg.Payload.DDL == "" {
		return model.MessageTypeResolved, true, nil
	}

	return model.MessageTypeDDL, true, nil
}

// NextResolvedEvent returns the next resolved event if exists
func (d *Decoder) NextResolvedEvent() (uint64, error) {
	ts := d.msg.Payload.Source.CommitTs
	d.msg = nil

	return ts, nil
}

// NextRowChangedEvent returns the next row changed event if exists
func (d *Decoder) NextRowChangedEvent() (*model.RowChangedEvent, error) {
	if d.msg == nil || (d.msg.Payload.Before == nil && d.msg.Payload.After == nil) {
		return nil, cerror.ErrCodecDecode.GenWithStack(
			"invalid row changed event message")
	}

	event := msgToRowChangeEvent(d.msg)
	d.msg = nil
	log.Debug("row changed event assembled", zap.Any("event", event))
	return event, nil
}

// // NextDDLEvent returns the next DDL event if exists
func (d *Decoder) NextDDLEvent() (*model.DDLEvent, error) {
	if d.msg.Payload.DDL == "" {
		return nil, cerror.ErrCodecDecode.GenWithStack(
			"no message found when decode DDL event")
	}
	ddlEvent := msgToDDLEvent(d.msg)
	d.msg = nil

	return ddlEvent, nil
}
