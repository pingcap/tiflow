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

	ts, err := dec.NextResolvedEvent()
	require.NoError(t, err)
	require.Equal(t, uint64(checkpoint), ts)
}

func TestEncodeDDLEvent(t *testing.T) {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()

	sql := `create table test.test(id int primary key, name varchar(255) not null,
	 age int, email varchar(255) not null, key idx_name(name), key idx_name_email(name, email))`
	job := helper.DDL2Job(sql)
	tableInfo := model.WrapTableInfo(1, "test", 1, job.BinlogInfo.TableInfo)

	codecConfig := common.NewConfig(config.ProtocolSimple)
	enc := NewBuilder(codecConfig).Build()
	ddlEvent := &model.DDLEvent{
		StartTs:   1,
		CommitTs:  2,
		TableInfo: tableInfo,
		Query:     sql,
		Type:      job.Type,
	}

	m, err := enc.EncodeDDLEvent(ddlEvent)
	require.NoError(t, err)

	dec := NewDecoder()
	err = dec.AddKeyValue(m.Key, m.Value)
	require.NoError(t, err)

	messageType, hasNext, err := dec.HasNext()
	require.NoError(t, err)
	require.True(t, hasNext)
	require.Equal(t, model.MessageTypeDDL, messageType)

	event, err := dec.NextDDLEvent()
	require.NoError(t, err)
	require.Equal(t, ddlEvent.CommitTs, event.CommitTs)
	// because we don't we don't set startTs in the encoded message,
	// so the startTs is equal to commitTs
	require.Equal(t, ddlEvent.CommitTs, event.StartTs)
	require.Equal(t, ddlEvent.Query, event.Query)
	require.Equal(t, len(ddlEvent.TableInfo.Columns), len(event.TableInfo.Columns))
	require.Equal(t, len(ddlEvent.TableInfo.Indices), len(event.TableInfo.Indices))
}

func TestEncodeBootstrapEvent(t *testing.T) {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()

	sql := `create table test.test(id int primary key, name varchar(255) not null,
	 age int, email varchar(255) not null, key idx_name(name), key idx_name_email(name, email))`
	job := helper.DDL2Job(sql)
	tableInfo := model.WrapTableInfo(1, "test", 1, job.BinlogInfo.TableInfo)

	codecConfig := common.NewConfig(config.ProtocolSimple)
	enc := NewBuilder(codecConfig).Build()
	ddlEvent := &model.DDLEvent{
		StartTs:   1,
		CommitTs:  2,
		TableInfo: tableInfo,
		Query:     sql,
		Type:      job.Type,
	}
	ddlEvent.IsBootstrap = true

	m, err := enc.EncodeDDLEvent(ddlEvent)
	require.NoError(t, err)

	dec := NewDecoder()
	err = dec.AddKeyValue(m.Key, m.Value)
	require.NoError(t, err)

	messageType, hasNext, err := dec.HasNext()
	require.NoError(t, err)
	require.True(t, hasNext)
	require.Equal(t, model.MessageTypeDDL, messageType)

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
}
