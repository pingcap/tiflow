// Copyright 2023 PingCAP, Inc.
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

package simple

import (
	"context"
	"testing"

	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"github.com/stretchr/testify/require"
)

func TestEncodeCheckpoint(t *testing.T) {
	t.Parallel()

	codecConfig := common.NewConfig(config.ProtocolSimple)
	enc := NewBuilder(codecConfig).Build()

	checkpoint := 23
	m, err := enc.EncodeCheckpointEvent(uint64(checkpoint))
	require.NoError(t, err)

	dec := NewDecoder()
	err = dec.AddKeyValue(m.Key, m.Value)
	require.NoError(t, err)

	messageType, hasNext, err := dec.HasNext()
	require.NoError(t, err)
	require.True(t, hasNext)
	require.Equal(t, model.MessageTypeResolved, messageType)
	require.NotEqual(t, 0, dec.msg.BuildTs)

	ts, err := dec.NextResolvedEvent()
	require.NoError(t, err)
	require.Equal(t, uint64(checkpoint), ts)
}

func TestEncodeDDLEvent(t *testing.T) {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()

	sql := `create table test.t(
    	id int primary key,
    	name varchar(255) not null,
    	age int,
    	email varchar(255) not null,
    	key idx_name(name),
    	key idx_name_email(name, email))`
	ddlEvent := helper.DDL2Event(sql)

	codecConfig := common.NewConfig(config.ProtocolSimple)
	enc := NewBuilder(codecConfig).Build()

	m, err := enc.EncodeDDLEvent(ddlEvent)
	require.NoError(t, err)

	dec := NewDecoder()
	err = dec.AddKeyValue(m.Key, m.Value)
	require.NoError(t, err)

	messageType, hasNext, err := dec.HasNext()
	require.NoError(t, err)
	require.True(t, hasNext)
	require.Equal(t, model.MessageTypeDDL, messageType)
	require.NotEqual(t, 0, dec.msg.BuildTs)

	event, err := dec.NextDDLEvent()
	require.NoError(t, err)
	require.Equal(t, ddlEvent.CommitTs, event.CommitTs)
	// because we don't we don't set startTs in the encoded message,
	// so the startTs is equal to commitTs
	require.Equal(t, ddlEvent.CommitTs, event.StartTs)
	require.Equal(t, ddlEvent.Query, event.Query)
	require.Equal(t, len(ddlEvent.TableInfo.Columns), len(event.TableInfo.Columns))
	require.Equal(t, len(ddlEvent.TableInfo.Indices), len(event.TableInfo.Indices))
	require.Equal(t, ddlEvent.TableInfo.Charset, event.TableInfo.Charset)
	require.Equal(t, ddlEvent.TableInfo.Collate, event.TableInfo.Collate)

	item := dec.memo.Read(ddlEvent.TableInfo.TableName.Schema,
		ddlEvent.TableInfo.TableName.Table, ddlEvent.TableInfo.UpdateTS)
	require.NotNil(t, item)

	sql = `insert into test.t values (1, "jack", 23, "jack@abc.com")`
	row := helper.DML2Event(sql, "test", "t")

	err = enc.AppendRowChangedEvent(context.Background(), "", row, func() {})
	require.NoError(t, err)

	messages := enc.Build()
	require.Len(t, messages, 1)

	err = dec.AddKeyValue(messages[0].Key, messages[0].Value)
	require.NoError(t, err)

	messageType, hasNext, err = dec.HasNext()
	require.NoError(t, err)
	require.True(t, hasNext)
	require.Equal(t, model.MessageTypeRow, messageType)
	require.NotEqual(t, 0, dec.msg.BuildTs)

	decodedRow, err := dec.NextRowChangedEvent()
	require.NoError(t, err)
	require.Equal(t, decodedRow.CommitTs, row.CommitTs)
	require.Equal(t, decodedRow.Table.Schema, row.Table.Schema)
	require.Equal(t, decodedRow.Table.Table, row.Table.Table)
	require.Nil(t, decodedRow.PreColumns)
	require.Equal(t, decodedRow.TableInfo.Charset, row.TableInfo.Charset)
	require.Equal(t, decodedRow.TableInfo.Collate, row.TableInfo.Collate)
}

func TestEncodeBootstrapEvent(t *testing.T) {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()

	sql := `create table test.t(
    	id int primary key,
    	name varchar(255) not null,
    	age int,
    	email varchar(255) not null,
    	key idx_name(name),
    	key idx_name_email(name, email))`
	ddlEvent := helper.DDL2Event(sql)
	ddlEvent.IsBootstrap = true

	codecConfig := common.NewConfig(config.ProtocolSimple)
	enc := NewBuilder(codecConfig).Build()

	m, err := enc.EncodeDDLEvent(ddlEvent)
	require.NoError(t, err)

	dec := NewDecoder()
	err = dec.AddKeyValue(m.Key, m.Value)
	require.NoError(t, err)

	messageType, hasNext, err := dec.HasNext()
	require.NoError(t, err)
	require.True(t, hasNext)
	require.Equal(t, model.MessageTypeDDL, messageType)
	require.NotEqual(t, 0, dec.msg.BuildTs)

	event, err := dec.NextDDLEvent()
	require.NoError(t, err)
	require.Equal(t, ddlEvent.CommitTs, event.CommitTs)
	// because we don't we don't set startTs in the encoded message,
	// so the startTs is equal to commitTs
	require.Equal(t, ddlEvent.CommitTs, event.StartTs)
	// Bootstrap event doesn't have query
	require.Equal(t, "", event.Query)
	require.Equal(t, len(ddlEvent.TableInfo.Columns), len(event.TableInfo.Columns))
	require.Equal(t, len(ddlEvent.TableInfo.Indices), len(event.TableInfo.Indices))
	require.Equal(t, ddlEvent.TableInfo.Charset, event.TableInfo.Charset)
	require.Equal(t, ddlEvent.TableInfo.Collate, event.TableInfo.Collate)

	item := dec.memo.Read(ddlEvent.TableInfo.TableName.Schema,
		ddlEvent.TableInfo.TableName.Table, ddlEvent.TableInfo.UpdateTS)
	require.NotNil(t, item)

	sql = `insert into test.t values (1, "jack", 23, "jack@abc.com")`
	row := helper.DML2Event(sql, "test", "t")

	err = enc.AppendRowChangedEvent(context.Background(), "", row, func() {})
	require.NoError(t, err)

	messages := enc.Build()
	require.Len(t, messages, 1)

	err = dec.AddKeyValue(messages[0].Key, messages[0].Value)
	require.NoError(t, err)

	messageType, hasNext, err = dec.HasNext()
	require.NoError(t, err)
	require.True(t, hasNext)
	require.Equal(t, model.MessageTypeRow, messageType)
	require.NotEqual(t, 0, dec.msg.BuildTs)

	decodedRow, err := dec.NextRowChangedEvent()
	require.NoError(t, err)
	require.Equal(t, decodedRow.CommitTs, row.CommitTs)
	require.Equal(t, decodedRow.Table.Schema, row.Table.Schema)
	require.Equal(t, decodedRow.Table.Table, row.Table.Table)
	require.Nil(t, decodedRow.PreColumns)
	require.Equal(t, decodedRow.TableInfo.Charset, row.TableInfo.Charset)
	require.Equal(t, decodedRow.TableInfo.Collate, row.TableInfo.Collate)
}
