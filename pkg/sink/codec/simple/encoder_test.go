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
	"database/sql"
	"math/rand"
	"sort"
	"strconv"
	"testing"
	"time"

	timodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/compression"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/integrity"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"github.com/pingcap/tiflow/pkg/sink/codec/utils"
	"github.com/stretchr/testify/require"
)

func TestEncodeCheckpoint(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	codecConfig := common.NewConfig(config.ProtocolSimple)
	for _, format := range []common.EncodingFormatType{
		common.EncodingFormatAvro,
		common.EncodingFormatJSON,
	} {
		codecConfig.EncodingFormat = format

		for _, compressionType := range []string{
			compression.None,
			compression.Snappy,
			compression.LZ4,
		} {
			codecConfig.LargeMessageHandle.LargeMessageHandleCompression = compressionType
			b, err := NewBuilder(ctx, codecConfig)
			require.NoError(t, err)
			enc := b.Build()

			checkpoint := 446266400629063682
			m, err := enc.EncodeCheckpointEvent(uint64(checkpoint))
			require.NoError(t, err)

			dec, err := NewDecoder(ctx, codecConfig, nil)
			require.NoError(t, err)

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
	}
}

func TestEncodeDMLEnableChecksum(t *testing.T) {
	replicaConfig := config.GetDefaultReplicaConfig()
	replicaConfig.Integrity.IntegrityCheckLevel = integrity.CheckLevelCorrectness
	createTableDDL, insertEvent, _, _ := utils.NewLargeEvent4Test(t, replicaConfig)
	rand.New(rand.NewSource(time.Now().Unix())).Shuffle(len(createTableDDL.TableInfo.Columns), func(i, j int) {
		createTableDDL.TableInfo.Columns[i], createTableDDL.TableInfo.Columns[j] = createTableDDL.TableInfo.Columns[j], createTableDDL.TableInfo.Columns[i]
	})

	ctx := context.Background()
	codecConfig := common.NewConfig(config.ProtocolSimple)
	codecConfig.EnableRowChecksum = true
	for _, format := range []common.EncodingFormatType{
		common.EncodingFormatAvro,
		common.EncodingFormatJSON,
	} {
		codecConfig.EncodingFormat = format
		for _, compressionType := range []string{
			compression.None,
			compression.Snappy,
			compression.LZ4,
		} {
			codecConfig.LargeMessageHandle.LargeMessageHandleCompression = compressionType

			b, err := NewBuilder(ctx, codecConfig)
			require.NoError(t, err)
			enc := b.Build()

			dec, err := NewDecoder(ctx, codecConfig, nil)
			require.NoError(t, err)

			m, err := enc.EncodeDDLEvent(createTableDDL)
			require.NoError(t, err)

			err = dec.AddKeyValue(m.Key, m.Value)
			require.NoError(t, err)

			messageType, hasNext, err := dec.HasNext()
			require.NoError(t, err)
			require.True(t, hasNext)
			require.Equal(t, model.MessageTypeDDL, messageType)

			_, err = dec.NextDDLEvent()
			require.NoError(t, err)

			err = enc.AppendRowChangedEvent(ctx, "", insertEvent, func() {})
			require.NoError(t, err)

			messages := enc.Build()
			require.Len(t, messages, 1)

			err = dec.AddKeyValue(messages[0].Key, messages[0].Value)
			require.NoError(t, err)

			messageType, hasNext, err = dec.HasNext()
			require.NoError(t, err)
			require.True(t, hasNext)
			require.Equal(t, model.MessageTypeRow, messageType)

			decodedRow, err := dec.NextRowChangedEvent()
			require.NoError(t, err)
			require.Equal(t, insertEvent.Checksum.Current, decodedRow.Checksum.Current)
			require.Equal(t, insertEvent.Checksum.Previous, decodedRow.Checksum.Previous)
			require.False(t, decodedRow.Checksum.Corrupted)
		}
	}
}

func TestEncodeDDLEvent(t *testing.T) {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()

	sql := `create table test.t(id int primary key, name varchar(255) not null, gender enum('male', 'female'), email varchar(255) not null, key idx_name_email(name, email))`
	createTableDDLEvent := helper.DDL2Event(sql)
	rand.New(rand.NewSource(time.Now().Unix())).Shuffle(len(createTableDDLEvent.TableInfo.Columns), func(i, j int) {
		createTableDDLEvent.TableInfo.Columns[i], createTableDDLEvent.TableInfo.Columns[j] = createTableDDLEvent.TableInfo.Columns[j], createTableDDLEvent.TableInfo.Columns[i]
	})

	sql = `insert into test.t values (1, "jack", "male", "jack@abc.com")`
	insertEvent := helper.DML2Event(sql, "test", "t")

	sql = `rename table test.t to test.abc`
	renameTableDDLEvent := helper.DDL2Event(sql)

	sql = `insert into test.abc values (2, "anna", "female", "anna@abc.com")`
	insertEvent2 := helper.DML2Event(sql, "test", "abc")

	helper.Tk().MustExec("drop table test.abc")

	ctx := context.Background()
	codecConfig := common.NewConfig(config.ProtocolSimple)
	for _, format := range []common.EncodingFormatType{
		common.EncodingFormatAvro,
		common.EncodingFormatJSON,
	} {
		codecConfig.EncodingFormat = format
		for _, compressionType := range []string{
			compression.None,
			compression.Snappy,
			compression.LZ4,
		} {
			codecConfig.LargeMessageHandle.LargeMessageHandleCompression = compressionType
			b, err := NewBuilder(ctx, codecConfig)
			require.NoError(t, err)
			enc := b.Build()

			dec, err := NewDecoder(ctx, codecConfig, nil)
			require.NoError(t, err)

			m, err := enc.EncodeDDLEvent(createTableDDLEvent)
			require.NoError(t, err)

			err = dec.AddKeyValue(m.Key, m.Value)
			require.NoError(t, err)

			messageType, hasNext, err := dec.HasNext()
			require.NoError(t, err)
			require.True(t, hasNext)
			require.Equal(t, model.MessageTypeDDL, messageType)
			require.NotEqual(t, 0, dec.msg.BuildTs)

			columnSchemas := dec.msg.TableSchema.Columns
			sortedColumns := make([]*timodel.ColumnInfo, len(createTableDDLEvent.TableInfo.Columns))
			copy(sortedColumns, createTableDDLEvent.TableInfo.Columns)
			sort.Slice(sortedColumns, func(i, j int) bool {
				return sortedColumns[i].ID < sortedColumns[j].ID
			})
			for idx, column := range sortedColumns {
				require.Equal(t, column.Name.O, columnSchemas[idx].Name)
			}

			event, err := dec.NextDDLEvent()
			require.NoError(t, err)
			require.Equal(t, createTableDDLEvent.CommitTs, event.CommitTs)
			// because we don't we don't set startTs in the encoded message,
			// so the startTs is equal to commitTs
			require.Equal(t, createTableDDLEvent.CommitTs, event.StartTs)
			require.Equal(t, createTableDDLEvent.Query, event.Query)
			require.Equal(t, len(createTableDDLEvent.TableInfo.Columns), len(event.TableInfo.Columns))
			require.Equal(t, len(createTableDDLEvent.TableInfo.Indices)+1, len(event.TableInfo.Indices))
			require.Nil(t, event.PreTableInfo)

			item := dec.memo.Read(createTableDDLEvent.TableInfo.TableName.Schema,
				createTableDDLEvent.TableInfo.TableName.Table, createTableDDLEvent.TableInfo.UpdateTS)
			require.NotNil(t, item)

			err = enc.AppendRowChangedEvent(ctx, "", insertEvent, func() {})
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
			require.Equal(t, decodedRow.CommitTs, insertEvent.CommitTs)
			require.Equal(t, decodedRow.Table.Schema, insertEvent.Table.Schema)
			require.Equal(t, decodedRow.Table.Table, insertEvent.Table.Table)
			require.Nil(t, decodedRow.PreColumns)

			m, err = enc.EncodeDDLEvent(renameTableDDLEvent)
			require.NoError(t, err)

			err = dec.AddKeyValue(m.Key, m.Value)
			require.NoError(t, err)

			messageType, hasNext, err = dec.HasNext()
			require.NoError(t, err)
			require.True(t, hasNext)
			require.Equal(t, model.MessageTypeDDL, messageType)
			require.NotEqual(t, 0, dec.msg.BuildTs)

			event, err = dec.NextDDLEvent()
			require.NoError(t, err)
			require.Equal(t, renameTableDDLEvent.CommitTs, event.CommitTs)
			// because we don't we don't set startTs in the encoded message,
			// so the startTs is equal to commitTs
			require.Equal(t, renameTableDDLEvent.CommitTs, event.StartTs)
			require.Equal(t, renameTableDDLEvent.Query, event.Query)
			require.Equal(t, len(renameTableDDLEvent.TableInfo.Columns), len(event.TableInfo.Columns))
			require.Equal(t, len(renameTableDDLEvent.TableInfo.Indices)+1, len(event.TableInfo.Indices))
			require.NotNil(t, event.PreTableInfo)

			item = dec.memo.Read(renameTableDDLEvent.TableInfo.TableName.Schema,
				renameTableDDLEvent.TableInfo.TableName.Table, renameTableDDLEvent.TableInfo.UpdateTS)
			require.NotNil(t, item)

			err = enc.AppendRowChangedEvent(context.Background(), "", insertEvent2, func() {})
			require.NoError(t, err)

			messages = enc.Build()
			require.Len(t, messages, 1)

			err = dec.AddKeyValue(messages[0].Key, messages[0].Value)
			require.NoError(t, err)

			messageType, hasNext, err = dec.HasNext()
			require.NoError(t, err)
			require.True(t, hasNext)
			require.Equal(t, model.MessageTypeRow, messageType)
			require.NotEqual(t, 0, dec.msg.BuildTs)

			decodedRow, err = dec.NextRowChangedEvent()
			require.NoError(t, err)
			require.Equal(t, decodedRow.CommitTs, insertEvent2.CommitTs)
			require.Equal(t, decodedRow.Table.Schema, insertEvent2.Table.Schema)
			require.Equal(t, decodedRow.Table.Table, insertEvent2.Table.Table)
			require.Nil(t, decodedRow.PreColumns)
		}
	}
}

func TestEncodeIntegerTypes(t *testing.T) {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()

	sql := `create table test.tp_unsigned_int (
		id          int auto_increment,
		c_unsigned_tinyint   tinyint   unsigned null,
		c_unsigned_smallint  smallint  unsigned null,
		c_unsigned_mediumint mediumint unsigned null,
		c_unsigned_int       int       unsigned null,
		c_unsigned_bigint    bigint    unsigned null,
		constraint pk primary key (id))`
	ddlEvent := helper.DDL2Event(sql)

	sql = `insert into test.tp_unsigned_int(c_unsigned_tinyint, c_unsigned_smallint, c_unsigned_mediumint,
		c_unsigned_int, c_unsigned_bigint) values (255, 65535, 16777215, 4294967295, 18446744073709551615)`
	dmlEvent := helper.DML2Event(sql, "test", "tp_unsigned_int")

	ctx := context.Background()
	codecConfig := common.NewConfig(config.ProtocolSimple)
	for _, format := range []common.EncodingFormatType{
		common.EncodingFormatAvro,
		common.EncodingFormatJSON,
	} {
		codecConfig.EncodingFormat = format
		b, err := NewBuilder(ctx, codecConfig)
		require.NoError(t, err)
		enc := b.Build()

		m, err := enc.EncodeDDLEvent(ddlEvent)
		require.NoError(t, err)

		dec, err := NewDecoder(ctx, codecConfig, nil)
		require.NoError(t, err)

		err = dec.AddKeyValue(m.Key, m.Value)
		require.NoError(t, err)

		messageType, hasNext, err := dec.HasNext()
		require.NoError(t, err)
		require.True(t, hasNext)
		require.Equal(t, model.MessageTypeDDL, messageType)

		_, err = dec.NextDDLEvent()
		require.NoError(t, err)

		err = enc.AppendRowChangedEvent(ctx, "", dmlEvent, func() {})
		require.NoError(t, err)

		messages := enc.Build()
		err = dec.AddKeyValue(messages[0].Key, messages[0].Value)
		require.NoError(t, err)

		messageType, hasNext, err = dec.HasNext()
		require.NoError(t, err)
		require.True(t, hasNext)
		require.Equal(t, model.MessageTypeRow, messageType)

		decodedRow, err := dec.NextRowChangedEvent()
		require.NoError(t, err)
		require.Equal(t, decodedRow.CommitTs, dmlEvent.CommitTs)

		decodedColumns := make(map[string]*model.Column, len(decodedRow.Columns))
		for _, column := range decodedRow.Columns {
			decodedColumns[column.Name] = column
		}

		for _, expected := range dmlEvent.Columns {
			decoded, ok := decodedColumns[expected.Name]
			require.True(t, ok)
			require.EqualValues(t, expected.Value, decoded.Value)
			require.Equal(t, expected.Charset, decoded.Charset)
			require.Equal(t, expected.Collation, decoded.Collation)
		}
	}
}

func TestEncoderOtherTypes(t *testing.T) {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()

	sql := `create table test.t(
			a int primary key auto_increment,
			b enum('a', 'b', 'c'),
			c set('a', 'b', 'c'),
			d bit(64),
			e json)`
	ddlEvent := helper.DDL2Event(sql)

	sql = `insert into test.t() values (1, 'a', 'a,b', b'1000001', '{
		  "key1": "value1",
		  "key2": "value2"
		}');`
	row := helper.DML2Event(sql, "test", "t")

	ctx := context.Background()
	codecConfig := common.NewConfig(config.ProtocolSimple)
	for _, format := range []common.EncodingFormatType{
		common.EncodingFormatAvro,
		common.EncodingFormatJSON,
	} {
		codecConfig.EncodingFormat = format
		b, err := NewBuilder(ctx, codecConfig)
		require.NoError(t, err)
		enc := b.Build()

		m, err := enc.EncodeDDLEvent(ddlEvent)
		require.NoError(t, err)

		dec, err := NewDecoder(ctx, codecConfig, nil)
		require.NoError(t, err)

		err = dec.AddKeyValue(m.Key, m.Value)
		require.NoError(t, err)

		messageType, hasNext, err := dec.HasNext()
		require.NoError(t, err)
		require.True(t, hasNext)
		require.Equal(t, model.MessageTypeDDL, messageType)

		_, err = dec.NextDDLEvent()
		require.NoError(t, err)

		err = enc.AppendRowChangedEvent(ctx, "", row, func() {})
		require.NoError(t, err)

		messages := enc.Build()
		require.Len(t, messages, 1)

		err = dec.AddKeyValue(messages[0].Key, messages[0].Value)
		require.NoError(t, err)

		messageType, hasNext, err = dec.HasNext()
		require.NoError(t, err)
		require.True(t, hasNext)
		require.Equal(t, model.MessageTypeRow, messageType)

		decodedRow, err := dec.NextRowChangedEvent()
		require.NoError(t, err)

		decodedColumns := make(map[string]*model.Column, len(decodedRow.Columns))
		for _, column := range decodedRow.Columns {
			decodedColumns[column.Name] = column
		}

		for _, expected := range row.Columns {
			decoded, ok := decodedColumns[expected.Name]
			require.True(t, ok)
			require.Equal(t, expected.Value, decoded.Value)
			require.Equal(t, expected.Charset, decoded.Charset)
			require.Equal(t, expected.Collation, decoded.Collation)
		}
	}
}

func TestEncodeBootstrapEvent(t *testing.T) {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()

	sql := `create table test.t(
    	id int primary key,
    	name varchar(255) not null,
    	age int,
    	email varchar(255) not null,
    	key idx_name_email(name, email))`
	ddlEvent := helper.DDL2Event(sql)
	ddlEvent.IsBootstrap = true

	sql = `insert into test.t values (1, "jack", 23, "jack@abc.com")`
	row := helper.DML2Event(sql, "test", "t")

	helper.Tk().MustExec("drop table test.t")

	ctx := context.Background()
	codecConfig := common.NewConfig(config.ProtocolSimple)
	for _, format := range []common.EncodingFormatType{
		common.EncodingFormatAvro,
		common.EncodingFormatJSON,
	} {
		codecConfig.EncodingFormat = format
		for _, compressionType := range []string{
			compression.None,
			compression.Snappy,
			compression.LZ4,
		} {
			codecConfig.LargeMessageHandle.LargeMessageHandleCompression = compressionType
			b, err := NewBuilder(ctx, codecConfig)
			require.NoError(t, err)
			enc := b.Build()

			m, err := enc.EncodeDDLEvent(ddlEvent)
			require.NoError(t, err)

			dec, err := NewDecoder(ctx, codecConfig, nil)
			require.NoError(t, err)

			err = dec.AddKeyValue(m.Key, m.Value)
			require.NoError(t, err)

			messageType, hasNext, err := dec.HasNext()
			require.NoError(t, err)
			require.True(t, hasNext)
			require.Equal(t, model.MessageTypeDDL, messageType)
			require.NotEqual(t, 0, dec.msg.BuildTs)

			event, err := dec.NextDDLEvent()
			require.NoError(t, err)
			// Bootstrap event doesn't have query
			require.Equal(t, "", event.Query)
			require.Equal(t, len(ddlEvent.TableInfo.Columns), len(event.TableInfo.Columns))
			require.Equal(t, len(ddlEvent.TableInfo.Indices)+1, len(event.TableInfo.Indices))

			item := dec.memo.Read(ddlEvent.TableInfo.TableName.Schema,
				ddlEvent.TableInfo.TableName.Table, ddlEvent.TableInfo.UpdateTS)
			require.NotNil(t, item)

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
		}
	}
}

func TestEncodeLargeEventsNormal(t *testing.T) {
	ddlEvent, insertEvent, updateEvent, deleteEvent := utils.NewLargeEvent4Test(t, config.GetDefaultReplicaConfig())

	ctx := context.Background()
	codecConfig := common.NewConfig(config.ProtocolSimple)
	for _, format := range []common.EncodingFormatType{
		common.EncodingFormatAvro,
		common.EncodingFormatJSON,
	} {
		codecConfig.EncodingFormat = format
		for _, compressionType := range []string{
			compression.None,
			compression.Snappy,
			compression.LZ4,
		} {
			codecConfig.LargeMessageHandle.LargeMessageHandleCompression = compressionType

			b, err := NewBuilder(ctx, codecConfig)
			require.NoError(t, err)
			enc := b.Build()

			dec, err := NewDecoder(ctx, codecConfig, nil)
			require.NoError(t, err)

			m, err := enc.EncodeDDLEvent(ddlEvent)
			require.NoError(t, err)

			err = dec.AddKeyValue(m.Key, m.Value)
			require.NoError(t, err)

			messageType, hasNext, err := dec.HasNext()
			require.NoError(t, err)
			require.True(t, hasNext)
			require.Equal(t, model.MessageTypeDDL, messageType)

			obtainedDDL, err := dec.NextDDLEvent()
			require.NoError(t, err)
			require.NotNil(t, obtainedDDL)

			obtainedDefaultValues := make(map[string]interface{}, len(obtainedDDL.TableInfo.Columns))
			for _, col := range obtainedDDL.TableInfo.Columns {
				obtainedDefaultValues[col.Name.O] = entry.GetColumnDefaultValue(col)
				switch col.GetType() {
				case mysql.TypeFloat, mysql.TypeDouble:
					require.Equal(t, 0, col.GetDecimal())
				default:
				}
			}
			for _, col := range ddlEvent.TableInfo.Columns {
				expected := entry.GetColumnDefaultValue(col)
				obtained := obtainedDefaultValues[col.Name.O]
				require.Equal(t, expected, obtained)
			}

			for _, event := range []*model.RowChangedEvent{insertEvent, updateEvent, deleteEvent} {
				err = enc.AppendRowChangedEvent(ctx, "", event, func() {})
				require.NoError(t, err)

				messages := enc.Build()
				require.Len(t, messages, 1)

				err = dec.AddKeyValue(messages[0].Key, messages[0].Value)
				require.NoError(t, err)

				messageType, hasNext, err = dec.HasNext()
				require.NoError(t, err)
				require.True(t, hasNext)
				require.Equal(t, model.MessageTypeRow, messageType)

				decodedRow, err := dec.NextRowChangedEvent()
				require.NoError(t, err)

				require.Equal(t, decodedRow.CommitTs, event.CommitTs)
				require.Equal(t, decodedRow.Table.Schema, event.Table.Schema)
				require.Equal(t, decodedRow.Table.Table, event.Table.Table)

				decodedColumns := make(map[string]*model.Column, len(decodedRow.Columns))
				for _, column := range decodedRow.Columns {
					decodedColumns[column.Name] = column
				}
				for _, col := range event.Columns {
					decoded, ok := decodedColumns[col.Name]
					require.True(t, ok)
					require.Equal(t, col.Type, decoded.Type)
					require.Equal(t, col.Charset, decoded.Charset)
					require.Equal(t, col.Collation, decoded.Collation)
					require.EqualValues(t, col.Value, decoded.Value)
				}

				decodedPreviousColumns := make(map[string]*model.Column, len(decodedRow.PreColumns))
				for _, column := range decodedRow.PreColumns {
					decodedPreviousColumns[column.Name] = column
				}
				for _, col := range event.PreColumns {
					decoded, ok := decodedPreviousColumns[col.Name]
					require.True(t, ok)
					require.Equal(t, col.Type, decoded.Type)
					require.Equal(t, col.Charset, decoded.Charset)
					require.Equal(t, col.Collation, decoded.Collation)
					require.EqualValues(t, col.Value, decoded.Value)
				}
			}
		}
	}
}

func TestDDLMessageTooLarge(t *testing.T) {
	ddlEvent, _, _, _ := utils.NewLargeEvent4Test(t, config.GetDefaultReplicaConfig())

	codecConfig := common.NewConfig(config.ProtocolSimple)
	codecConfig.MaxMessageBytes = 100
	for _, format := range []common.EncodingFormatType{
		common.EncodingFormatAvro,
		common.EncodingFormatJSON,
	} {
		codecConfig.EncodingFormat = format
		b, err := NewBuilder(context.Background(), codecConfig)
		require.NoError(t, err)
		enc := b.Build()

		_, err = enc.EncodeDDLEvent(ddlEvent)
		require.ErrorIs(t, err, errors.ErrMessageTooLarge)
	}
}

func TestDMLMessageTooLarge(t *testing.T) {
	_, insertEvent, _, _ := utils.NewLargeEvent4Test(t, config.GetDefaultReplicaConfig())

	codecConfig := common.NewConfig(config.ProtocolSimple)
	codecConfig.MaxMessageBytes = 100
	for _, format := range []common.EncodingFormatType{
		common.EncodingFormatAvro,
		common.EncodingFormatJSON,
	} {
		codecConfig.EncodingFormat = format
		b, err := NewBuilder(context.Background(), codecConfig)
		require.NoError(t, err)
		enc := b.Build()

		err = enc.AppendRowChangedEvent(context.Background(), "", insertEvent, func() {})
		require.ErrorIs(t, err, errors.ErrMessageTooLarge)
	}
}

func TestLargerMessageHandleClaimCheck(t *testing.T) {
	ddlEvent, _, updateEvent, _ := utils.NewLargeEvent4Test(t, config.GetDefaultReplicaConfig())

	ctx := context.Background()
	codecConfig := common.NewConfig(config.ProtocolSimple)
	codecConfig.LargeMessageHandle.LargeMessageHandleOption = config.LargeMessageHandleOptionClaimCheck
	codecConfig.LargeMessageHandle.ClaimCheckStorageURI = "file:///tmp/simple-claim-check"
	for _, format := range []common.EncodingFormatType{
		common.EncodingFormatAvro,
		common.EncodingFormatJSON,
	} {
		codecConfig.EncodingFormat = format
		for _, compressionType := range []string{
			compression.None,
			compression.Snappy,
			compression.LZ4,
		} {
			codecConfig.MaxMessageBytes = config.DefaultMaxMessageBytes
			codecConfig.LargeMessageHandle.LargeMessageHandleCompression = compressionType

			b, err := NewBuilder(ctx, codecConfig)
			require.NoError(t, err)
			enc := b.Build()

			m, err := enc.EncodeDDLEvent(ddlEvent)
			require.NoError(t, err)

			dec, err := NewDecoder(ctx, codecConfig, nil)
			require.NoError(t, err)

			err = dec.AddKeyValue(m.Key, m.Value)
			require.NoError(t, err)

			messageType, hasNext, err := dec.HasNext()
			require.NoError(t, err)
			require.True(t, hasNext)
			require.Equal(t, model.MessageTypeDDL, messageType)

			_, err = dec.NextDDLEvent()
			require.NoError(t, err)

			enc.(*encoder).config.MaxMessageBytes = 500
			err = enc.AppendRowChangedEvent(ctx, "", updateEvent, func() {})
			require.NoError(t, err)

			claimCheckLocationM := enc.Build()[0]

			dec.config.MaxMessageBytes = 500
			err = dec.AddKeyValue(claimCheckLocationM.Key, claimCheckLocationM.Value)
			require.NoError(t, err)

			messageType, hasNext, err = dec.HasNext()
			require.NoError(t, err)
			require.True(t, hasNext)
			require.Equal(t, model.MessageTypeRow, messageType)
			require.NotEqual(t, "", dec.msg.ClaimCheckLocation)

			decodedRow, err := dec.NextRowChangedEvent()
			require.NoError(t, err)

			require.Equal(t, decodedRow.CommitTs, updateEvent.CommitTs)
			require.Equal(t, decodedRow.Table.Schema, updateEvent.Table.Schema)
			require.Equal(t, decodedRow.Table.Table, updateEvent.Table.Table)

			decodedColumns := make(map[string]*model.Column, len(decodedRow.Columns))
			for _, column := range decodedRow.Columns {
				decodedColumns[column.Name] = column
			}
			for _, col := range updateEvent.Columns {
				decoded, ok := decodedColumns[col.Name]
				require.True(t, ok)
				require.Equal(t, col.Type, decoded.Type)
				require.Equal(t, col.Charset, decoded.Charset)
				require.Equal(t, col.Collation, decoded.Collation)
				require.EqualValues(t, col.Value, decoded.Value)
			}

			for _, column := range decodedRow.PreColumns {
				decodedColumns[column.Name] = column
			}
			for _, col := range updateEvent.PreColumns {
				decoded, ok := decodedColumns[col.Name]
				require.True(t, ok)
				require.Equal(t, col.Type, decoded.Type)
				require.Equal(t, col.Charset, decoded.Charset)
				require.Equal(t, col.Collation, decoded.Collation)
				require.EqualValues(t, col.Value, decoded.Value)
			}
		}
	}
}

func TestLargeMessageHandleKeyOnly(t *testing.T) {
	_, _, updateEvent, _ := utils.NewLargeEvent4Test(t, config.GetDefaultReplicaConfig())

	ctx := context.Background()
	codecConfig := common.NewConfig(config.ProtocolSimple)
	codecConfig.MaxMessageBytes = 500
	codecConfig.LargeMessageHandle.LargeMessageHandleOption = config.LargeMessageHandleOptionHandleKeyOnly
	for _, format := range []common.EncodingFormatType{
		common.EncodingFormatJSON,
		common.EncodingFormatAvro,
	} {
		codecConfig.EncodingFormat = format
		for _, compressionType := range []string{
			compression.None,
			compression.Snappy,
			compression.LZ4,
		} {
			codecConfig.LargeMessageHandle.LargeMessageHandleCompression = compressionType

			b, err := NewBuilder(ctx, codecConfig)
			require.NoError(t, err)
			enc := b.Build()

			err = enc.AppendRowChangedEvent(ctx, "", updateEvent, func() {})
			require.NoError(t, err)

			messages := enc.Build()
			require.Len(t, messages, 1)

			dec, err := NewDecoder(ctx, codecConfig, &sql.DB{})
			require.NoError(t, err)

			err = dec.AddKeyValue(messages[0].Key, messages[0].Value)
			require.NoError(t, err)

			messageType, hasNext, err := dec.HasNext()
			require.NoError(t, err)
			require.True(t, hasNext)
			require.Equal(t, model.MessageTypeRow, messageType)
			require.True(t, dec.msg.HandleKeyOnly)

			obtainedValues := make(map[string]interface{}, len(dec.msg.Data))
			for name, value := range dec.msg.Data {
				obtainedValues[name] = value
			}
			for _, col := range updateEvent.Columns {
				if col.Flag.IsHandleKey() {
					require.Contains(t, dec.msg.Data, col.Name)
					obtained := obtainedValues[col.Name]
					switch v := obtained.(type) {
					case string:
						var err error
						obtained, err = strconv.ParseInt(v, 10, 64)
						require.NoError(t, err)
					}
					require.EqualValues(t, col.Value, obtained)
				} else {
					require.NotContains(t, dec.msg.Data, col.Name)
				}
			}

			obtainedValues = make(map[string]interface{}, len(dec.msg.Old))
			for name, value := range dec.msg.Old {
				obtainedValues[name] = value
			}
			for _, col := range updateEvent.PreColumns {
				if col.Flag.IsHandleKey() {
					require.Contains(t, dec.msg.Old, col.Name)
					obtained := obtainedValues[col.Name]
					switch v := obtained.(type) {
					case string:
						var err error
						obtained, err = strconv.ParseInt(v, 10, 64)
						require.NoError(t, err)
					}
					require.Equal(t, col.Value, obtained)
				} else {
					require.NotContains(t, dec.msg.Old, col.Name)
				}
			}
		}
	}
}
