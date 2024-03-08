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

func TestEncodeChecksum(t *testing.T) {
	replicaConfig := config.GetDefaultReplicaConfig()
	replicaConfig.Integrity.IntegrityCheckLevel = integrity.CheckLevelCorrectness
	helper := entry.NewSchemaTestHelperWithReplicaConfig(t, replicaConfig)
	defer helper.Close()

	helper.Tk().MustExec("use test")

	ctx := context.Background()
	codecConfig := common.NewConfig(config.ProtocolSimple)
	codecConfig.EnableRowChecksum = true
	codecConfig.EncodingFormat = common.EncodingFormatAvro

	createTableDDL := helper.DDL2Event("CREATE TABLE `t1` (`pk` varchar(32) NOT NULL, `sk` varchar(32) NOT NULL, `ts` timestamp(6) NOT NULL, `v` varchar(10240) DEFAULT NULL, PRIMARY KEY (`pk`,`sk`,`ts`) /*T![clustered_index] CLUSTERED */) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin")

	b, err := NewBuilder(ctx, codecConfig)
	require.NoError(t, err)
	enc := b.Build()

	m, err := enc.EncodeDDLEvent(createTableDDL)
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

	insertEvent := helper.DML2Event("INSERT INTO `t1` VALUES ('168716582', 'z2_10', '2024-03-08 07:20:55.953494', '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')", "test", "t1")
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

func TestEncodeDDLSequence(t *testing.T) {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")

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

			createTableDDLEvent := helper.DDL2Event("CREATE TABLE `TBL1` (`id` INT PRIMARY KEY AUTO_INCREMENT,`value` VARCHAR(255),`payload` VARCHAR(2000),`a` INT)")
			m, err := enc.EncodeDDLEvent(createTableDDLEvent)
			require.NoError(t, err)

			err = dec.AddKeyValue(m.Key, m.Value)
			require.NoError(t, err)

			messageType, hasNext, err := dec.HasNext()
			require.NoError(t, err)
			require.True(t, hasNext)
			require.Equal(t, model.MessageTypeDDL, messageType)
			require.Equal(t, DDLTypeCreate, dec.msg.Type)

			event, err := dec.NextDDLEvent()
			require.NoError(t, err)
			require.Len(t, event.TableInfo.Indices, 1)
			require.Len(t, event.TableInfo.Columns, 4)

			addColumnDDLEvent := helper.DDL2Event("ALTER TABLE `TBL1` ADD COLUMN `nn` INT")
			m, err = enc.EncodeDDLEvent(addColumnDDLEvent)
			require.NoError(t, err)

			err = dec.AddKeyValue(m.Key, m.Value)
			require.NoError(t, err)

			_, _, err = dec.HasNext()
			require.NoError(t, err)
			require.Equal(t, DDLTypeAlter, dec.msg.Type)

			event, err = dec.NextDDLEvent()
			require.NoError(t, err)
			require.Len(t, event.TableInfo.Indices, 1)
			require.Len(t, event.TableInfo.Columns, 5)

			dropColumnDDLEvent := helper.DDL2Event("ALTER TABLE `TBL1` DROP COLUMN `nn`")
			m, err = enc.EncodeDDLEvent(dropColumnDDLEvent)
			require.NoError(t, err)

			err = dec.AddKeyValue(m.Key, m.Value)
			require.NoError(t, err)

			_, _, err = dec.HasNext()
			require.NoError(t, err)
			require.Equal(t, DDLTypeAlter, dec.msg.Type)

			event, err = dec.NextDDLEvent()
			require.NoError(t, err)
			require.Len(t, event.TableInfo.Indices, 1)
			require.Len(t, event.TableInfo.Columns, 4)

			changeColumnDDLEvent := helper.DDL2Event("ALTER TABLE `TBL1` CHANGE COLUMN `value` `value2` VARCHAR(512)")
			m, err = enc.EncodeDDLEvent(changeColumnDDLEvent)
			require.NoError(t, err)

			err = dec.AddKeyValue(m.Key, m.Value)
			require.NoError(t, err)

			_, _, err = dec.HasNext()
			require.NoError(t, err)
			require.Equal(t, DDLTypeAlter, dec.msg.Type)

			event, err = dec.NextDDLEvent()
			require.NoError(t, err)
			require.Len(t, event.TableInfo.Indices, 1)
			require.Len(t, event.TableInfo.Columns, 4)

			modifyColumnDDLEvent := helper.DDL2Event("ALTER TABLE `TBL1` MODIFY COLUMN `value2` VARCHAR(512) FIRST")
			m, err = enc.EncodeDDLEvent(modifyColumnDDLEvent)
			require.NoError(t, err)

			err = dec.AddKeyValue(m.Key, m.Value)
			require.NoError(t, err)

			_, _, err = dec.HasNext()
			require.NoError(t, err)
			require.Equal(t, DDLTypeAlter, dec.msg.Type)

			event, err = dec.NextDDLEvent()
			require.NoError(t, err)
			require.Equal(t, 1, len(event.TableInfo.Indices))
			require.Equal(t, 4, len(event.TableInfo.Columns))

			setDefaultDDLEvent := helper.DDL2Event("ALTER TABLE `TBL1` ALTER COLUMN `payload` SET DEFAULT _UTF8MB4'a'")

			m, err = enc.EncodeDDLEvent(setDefaultDDLEvent)
			require.NoError(t, err)

			err = dec.AddKeyValue(m.Key, m.Value)
			require.NoError(t, err)

			_, _, err = dec.HasNext()
			require.NoError(t, err)
			require.Equal(t, DDLTypeAlter, dec.msg.Type)

			event, err = dec.NextDDLEvent()
			require.NoError(t, err)
			require.Equal(t, 1, len(event.TableInfo.Indices))
			require.Equal(t, 4, len(event.TableInfo.Columns))
			for _, col := range event.TableInfo.Columns {
				if col.Name.O == "payload" {
					require.Equal(t, "a", col.DefaultValue)
				}
			}

			dropDefaultDDLEvent := helper.DDL2Event("ALTER TABLE `TBL1` ALTER COLUMN `payload` DROP DEFAULT")
			m, err = enc.EncodeDDLEvent(dropDefaultDDLEvent)
			require.NoError(t, err)

			err = dec.AddKeyValue(m.Key, m.Value)
			require.NoError(t, err)

			_, _, err = dec.HasNext()
			require.NoError(t, err)
			require.Equal(t, DDLTypeAlter, dec.msg.Type)

			event, err = dec.NextDDLEvent()
			require.NoError(t, err)
			require.Equal(t, 1, len(event.TableInfo.Indices))
			require.Equal(t, 4, len(event.TableInfo.Columns))
			for _, col := range event.TableInfo.Columns {
				if col.Name.O == "payload" {
					require.Nil(t, col.DefaultValue)
				}
			}

			autoIncrementDDLEvent := helper.DDL2Event("ALTER TABLE `TBL1` AUTO_INCREMENT = 5")
			m, err = enc.EncodeDDLEvent(autoIncrementDDLEvent)
			require.NoError(t, err)

			err = dec.AddKeyValue(m.Key, m.Value)
			require.NoError(t, err)

			_, _, err = dec.HasNext()
			require.NoError(t, err)
			require.Equal(t, DDLTypeAlter, dec.msg.Type)

			event, err = dec.NextDDLEvent()
			require.NoError(t, err)
			require.Equal(t, 1, len(event.TableInfo.Indices))
			require.Equal(t, 4, len(event.TableInfo.Columns))

			modifyColumnNullDDLEvent := helper.DDL2Event("ALTER TABLE `TBL1` MODIFY COLUMN `a` INT NULL")
			m, err = enc.EncodeDDLEvent(modifyColumnNullDDLEvent)
			require.NoError(t, err)

			err = dec.AddKeyValue(m.Key, m.Value)
			require.NoError(t, err)

			_, _, err = dec.HasNext()
			require.NoError(t, err)
			require.Equal(t, DDLTypeAlter, dec.msg.Type)

			event, err = dec.NextDDLEvent()
			require.NoError(t, err)
			require.Equal(t, 1, len(event.TableInfo.Indices))
			require.Equal(t, 4, len(event.TableInfo.Columns))
			for _, col := range event.TableInfo.Columns {
				if col.Name.O == "a" {
					require.True(t, !mysql.HasNotNullFlag(col.GetFlag()))
				}
			}

			modifyColumnNotNullDDLEvent := helper.DDL2Event("ALTER TABLE `TBL1` MODIFY COLUMN `a` INT NOT NULL")
			m, err = enc.EncodeDDLEvent(modifyColumnNotNullDDLEvent)
			require.NoError(t, err)

			err = dec.AddKeyValue(m.Key, m.Value)
			require.NoError(t, err)

			_, _, err = dec.HasNext()
			require.NoError(t, err)
			require.Equal(t, DDLTypeAlter, dec.msg.Type)

			event, err = dec.NextDDLEvent()
			require.NoError(t, err)
			require.Equal(t, 1, len(event.TableInfo.Indices))
			require.Equal(t, 4, len(event.TableInfo.Columns))
			for _, col := range event.TableInfo.Columns {
				if col.Name.O == "a" {
					require.True(t, mysql.HasNotNullFlag(col.GetFlag()))
				}
			}

			addIndexDDLEvent := helper.DDL2Event("CREATE INDEX `idx_a` ON `TBL1` (`a`)")
			m, err = enc.EncodeDDLEvent(addIndexDDLEvent)
			require.NoError(t, err)

			err = dec.AddKeyValue(m.Key, m.Value)
			require.NoError(t, err)

			_, _, err = dec.HasNext()
			require.NoError(t, err)
			require.Equal(t, DDLTypeCIndex, dec.msg.Type)

			event, err = dec.NextDDLEvent()
			require.NoError(t, err)
			require.Equal(t, 2, len(event.TableInfo.Indices))
			require.Equal(t, 4, len(event.TableInfo.Columns))

			renameIndexDDLEvent := helper.DDL2Event("ALTER TABLE `TBL1` RENAME INDEX `idx_a` TO `new_idx_a`")
			m, err = enc.EncodeDDLEvent(renameIndexDDLEvent)
			require.NoError(t, err)

			err = dec.AddKeyValue(m.Key, m.Value)
			require.NoError(t, err)

			_, _, err = dec.HasNext()
			require.NoError(t, err)
			require.Equal(t, DDLTypeAlter, dec.msg.Type)

			event, err = dec.NextDDLEvent()
			require.NoError(t, err)
			require.Equal(t, 2, len(event.TableInfo.Indices))
			require.Equal(t, 4, len(event.TableInfo.Columns))
			hasNewIndex := false
			noOldIndex := true
			for _, index := range event.TableInfo.Indices {
				if index.Name.O == "new_idx_a" {
					hasNewIndex = true
				}
				if index.Name.O == "idx_a" {
					noOldIndex = false
				}
			}
			require.True(t, hasNewIndex)
			require.True(t, noOldIndex)

			indexVisibilityDDLEvent := helper.DDL2Event("ALTER TABLE TBL1 ALTER INDEX `new_idx_a` INVISIBLE")
			m, err = enc.EncodeDDLEvent(indexVisibilityDDLEvent)
			require.NoError(t, err)

			err = dec.AddKeyValue(m.Key, m.Value)
			require.NoError(t, err)

			_, _, err = dec.HasNext()
			require.NoError(t, err)
			require.Equal(t, DDLTypeAlter, dec.msg.Type)

			event, err = dec.NextDDLEvent()
			require.NoError(t, err)
			require.Equal(t, 2, len(event.TableInfo.Indices))
			require.Equal(t, 4, len(event.TableInfo.Columns))

			dropIndexDDLEvent := helper.DDL2Event("DROP INDEX `new_idx_a` ON `TBL1`")
			m, err = enc.EncodeDDLEvent(dropIndexDDLEvent)
			require.NoError(t, err)

			err = dec.AddKeyValue(m.Key, m.Value)
			require.NoError(t, err)

			_, _, err = dec.HasNext()
			require.NoError(t, err)
			require.Equal(t, DDLTypeDIndex, dec.msg.Type)

			event, err = dec.NextDDLEvent()
			require.NoError(t, err)
			require.Equal(t, 1, len(event.TableInfo.Indices))
			require.Equal(t, 4, len(event.TableInfo.Columns))

			truncateTableDDLEvent := helper.DDL2Event("TRUNCATE TABLE TBL1")
			m, err = enc.EncodeDDLEvent(truncateTableDDLEvent)
			require.NoError(t, err)

			err = dec.AddKeyValue(m.Key, m.Value)
			require.NoError(t, err)

			_, _, err = dec.HasNext()
			require.NoError(t, err)
			require.Equal(t, DDLTypeTruncate, dec.msg.Type)

			event, err = dec.NextDDLEvent()
			require.NoError(t, err)
			require.Equal(t, 1, len(event.TableInfo.Indices))
			require.Equal(t, 4, len(event.TableInfo.Columns))

			multiSchemaChangeDDLEvent := helper.DDL2Event("ALTER TABLE TBL1 ADD COLUMN `new_col` INT, ADD INDEX `idx_new_col` (`a`)")
			m, err = enc.EncodeDDLEvent(multiSchemaChangeDDLEvent)
			require.NoError(t, err)

			err = dec.AddKeyValue(m.Key, m.Value)
			require.NoError(t, err)

			_, _, err = dec.HasNext()
			require.NoError(t, err)
			require.Equal(t, DDLTypeAlter, dec.msg.Type)

			event, err = dec.NextDDLEvent()
			require.NoError(t, err)
			require.Equal(t, 2, len(event.TableInfo.Indices))
			require.Equal(t, 5, len(event.TableInfo.Columns))

			multiSchemaChangeDropDDLEvent := helper.DDL2Event("ALTER TABLE TBL1 DROP COLUMN `new_col`, DROP INDEX `idx_new_col`")
			m, err = enc.EncodeDDLEvent(multiSchemaChangeDropDDLEvent)
			require.NoError(t, err)

			err = dec.AddKeyValue(m.Key, m.Value)
			require.NoError(t, err)

			_, _, err = dec.HasNext()
			require.NoError(t, err)
			require.Equal(t, DDLTypeAlter, dec.msg.Type)

			event, err = dec.NextDDLEvent()
			require.NoError(t, err)
			require.Equal(t, 1, len(event.TableInfo.Indices))
			require.Equal(t, 4, len(event.TableInfo.Columns))

			renameTableDDLEvent := helper.DDL2Event("RENAME TABLE TBL1 TO TBL2")
			m, err = enc.EncodeDDLEvent(renameTableDDLEvent)
			require.NoError(t, err)

			err = dec.AddKeyValue(m.Key, m.Value)
			require.NoError(t, err)

			_, _, err = dec.HasNext()
			require.NoError(t, err)
			require.Equal(t, DDLTypeRename, dec.msg.Type)

			event, err = dec.NextDDLEvent()
			require.NoError(t, err)
			require.Equal(t, 1, len(event.TableInfo.Indices))
			require.Equal(t, 4, len(event.TableInfo.Columns))

			helper.Tk().MustExec("set @@tidb_allow_remove_auto_inc = 1")
			renameColumnDDLEvent := helper.DDL2Event("ALTER TABLE TBL2 CHANGE COLUMN `id` `id2` INT")
			m, err = enc.EncodeDDLEvent(renameColumnDDLEvent)
			require.NoError(t, err)

			err = dec.AddKeyValue(m.Key, m.Value)
			require.NoError(t, err)

			_, _, err = dec.HasNext()
			require.NoError(t, err)
			require.Equal(t, DDLTypeAlter, dec.msg.Type)

			event, err = dec.NextDDLEvent()
			require.NoError(t, err)
			require.Equal(t, 1, len(event.TableInfo.Indices))
			require.Equal(t, 4, len(event.TableInfo.Columns))

			partitionTableDDLEvent := helper.DDL2Event("ALTER TABLE TBL2 PARTITION BY RANGE (a) (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (20))")
			m, err = enc.EncodeDDLEvent(partitionTableDDLEvent)
			require.NoError(t, err)

			err = dec.AddKeyValue(m.Key, m.Value)
			require.NoError(t, err)

			_, _, err = dec.HasNext()
			require.NoError(t, err)
			require.Equal(t, DDLTypeAlter, dec.msg.Type)

			event, err = dec.NextDDLEvent()
			require.NoError(t, err)
			require.Equal(t, 1, len(event.TableInfo.Indices))
			require.Equal(t, 4, len(event.TableInfo.Columns))

			addPartitionDDLEvent := helper.DDL2Event("ALTER TABLE TBL2 ADD PARTITION (PARTITION p2 VALUES LESS THAN (30))")
			m, err = enc.EncodeDDLEvent(addPartitionDDLEvent)
			require.NoError(t, err)

			err = dec.AddKeyValue(m.Key, m.Value)
			require.NoError(t, err)

			_, _, err = dec.HasNext()
			require.NoError(t, err)
			require.Equal(t, DDLTypeAlter, dec.msg.Type)

			event, err = dec.NextDDLEvent()
			require.NoError(t, err)
			require.Equal(t, 1, len(event.TableInfo.Indices))
			require.Equal(t, 4, len(event.TableInfo.Columns))

			dropPartitionDDLEvent := helper.DDL2Event("ALTER TABLE TBL2 DROP PARTITION p2")
			m, err = enc.EncodeDDLEvent(dropPartitionDDLEvent)
			require.NoError(t, err)

			err = dec.AddKeyValue(m.Key, m.Value)
			require.NoError(t, err)

			_, _, err = dec.HasNext()
			require.NoError(t, err)
			require.Equal(t, DDLTypeAlter, dec.msg.Type)

			event, err = dec.NextDDLEvent()
			require.NoError(t, err)
			require.Equal(t, 1, len(event.TableInfo.Indices))
			require.Equal(t, 4, len(event.TableInfo.Columns))

			truncatePartitionDDLevent := helper.DDL2Event("ALTER TABLE TBL2 TRUNCATE PARTITION p1")
			m, err = enc.EncodeDDLEvent(truncatePartitionDDLevent)
			require.NoError(t, err)

			err = dec.AddKeyValue(m.Key, m.Value)
			require.NoError(t, err)

			_, _, err = dec.HasNext()
			require.NoError(t, err)
			require.Equal(t, DDLTypeAlter, dec.msg.Type)

			event, err = dec.NextDDLEvent()
			require.NoError(t, err)
			require.Equal(t, 1, len(event.TableInfo.Indices))
			require.Equal(t, 4, len(event.TableInfo.Columns))

			reorganizePartitionDDLEvent := helper.DDL2Event("ALTER TABLE TBL2 REORGANIZE PARTITION p1 INTO (PARTITION p3 VALUES LESS THAN (40))")
			m, err = enc.EncodeDDLEvent(reorganizePartitionDDLEvent)
			require.NoError(t, err)

			err = dec.AddKeyValue(m.Key, m.Value)
			require.NoError(t, err)

			_, _, err = dec.HasNext()
			require.NoError(t, err)
			require.Equal(t, DDLTypeAlter, dec.msg.Type)

			event, err = dec.NextDDLEvent()
			require.NoError(t, err)
			require.Equal(t, 1, len(event.TableInfo.Indices))
			require.Equal(t, 4, len(event.TableInfo.Columns))

			removePartitionDDLEvent := helper.DDL2Event("ALTER TABLE TBL2 REMOVE PARTITIONING")
			m, err = enc.EncodeDDLEvent(removePartitionDDLEvent)
			require.NoError(t, err)

			err = dec.AddKeyValue(m.Key, m.Value)
			require.NoError(t, err)

			_, _, err = dec.HasNext()
			require.NoError(t, err)
			require.Equal(t, DDLTypeAlter, dec.msg.Type)

			event, err = dec.NextDDLEvent()
			require.NoError(t, err)
			require.Equal(t, 1, len(event.TableInfo.Indices))
			require.Equal(t, 4, len(event.TableInfo.Columns))

			alterCharsetCollateDDLEvent := helper.DDL2Event("ALTER TABLE TBL2 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_bin")
			m, err = enc.EncodeDDLEvent(alterCharsetCollateDDLEvent)
			require.NoError(t, err)

			err = dec.AddKeyValue(m.Key, m.Value)
			require.NoError(t, err)

			_, _, err = dec.HasNext()
			require.NoError(t, err)
			require.Equal(t, DDLTypeAlter, dec.msg.Type)

			event, err = dec.NextDDLEvent()
			require.NoError(t, err)
			require.Equal(t, 1, len(event.TableInfo.Indices))
			require.Equal(t, 4, len(event.TableInfo.Columns))

			dropTableDDLEvent := helper.DDL2Event("DROP TABLE TBL2")
			m, err = enc.EncodeDDLEvent(dropTableDDLEvent)
			require.NoError(t, err)

			err = dec.AddKeyValue(m.Key, m.Value)
			require.NoError(t, err)

			_, _, err = dec.HasNext()
			require.NoError(t, err)
			require.Equal(t, DDLTypeErase, dec.msg.Type)

			event, err = dec.NextDDLEvent()
			require.NoError(t, err)
			require.Equal(t, 1, len(event.TableInfo.Indices))
			require.Equal(t, 4, len(event.TableInfo.Columns))
		}
	}
}

func TestEncodeDDLEvent(t *testing.T) {
	replicaConfig := config.GetDefaultReplicaConfig()
	replicaConfig.Integrity.IntegrityCheckLevel = integrity.CheckLevelCorrectness
	helper := entry.NewSchemaTestHelperWithReplicaConfig(t, replicaConfig)
	defer helper.Close()

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

			createTableSQL := `create table test.t(id int primary key, name varchar(255) not null, gender enum('male', 'female'), email varchar(255) not null, key idx_name_email(name, email))`
			createTableDDLEvent := helper.DDL2Event(createTableSQL)
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
			require.Equal(t, createTableDDLEvent.TableInfo.TableName.TableID, event.TableInfo.TableName.TableID)
			require.Equal(t, createTableDDLEvent.CommitTs, event.CommitTs)

			// because we don't we don't set startTs in the encoded message,
			// so the startTs is equal to commitTs

			require.Equal(t, createTableDDLEvent.CommitTs, event.StartTs)
			require.Equal(t, createTableDDLEvent.Query, event.Query)
			require.Equal(t, len(createTableDDLEvent.TableInfo.Columns), len(event.TableInfo.Columns))
			require.Equal(t, 2, len(event.TableInfo.Indices))
			require.Nil(t, event.PreTableInfo)

			item := dec.memo.Read(createTableDDLEvent.TableInfo.TableName.Schema,
				createTableDDLEvent.TableInfo.TableName.Table, createTableDDLEvent.TableInfo.UpdateTS)
			require.NotNil(t, item)

			insertEvent := helper.DML2Event(`insert into test.t values (1, "jack", "male", "jack@abc.com")`, "test", "t")
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
			require.Equal(t, decodedRow.TableInfo.GetSchemaName(), insertEvent.TableInfo.GetSchemaName())
			require.Equal(t, decodedRow.TableInfo.GetTableName(), insertEvent.TableInfo.GetTableName())
			require.Nil(t, decodedRow.PreColumns)

			renameTableDDLEvent := helper.DDL2Event(`rename table test.t to test.abc`)
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
			require.Equal(t, renameTableDDLEvent.TableInfo.TableName.TableID, event.TableInfo.TableName.TableID)
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

			insertEvent2 := helper.DML2Event(`insert into test.abc values (2, "anna", "female", "anna@abc.com")`, "test", "abc")
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
			require.Equal(t, decodedRow.TableInfo.GetSchemaName(), insertEvent2.TableInfo.GetSchemaName())
			require.Equal(t, decodedRow.TableInfo.GetTableName(), insertEvent2.TableInfo.GetTableName())
			require.Nil(t, decodedRow.PreColumns)

			helper.Tk().MustExec("drop table test.abc")
		}
	}
}

func TestEncodeIntegerTypes(t *testing.T) {
	replicaConfig := config.GetDefaultReplicaConfig()
	replicaConfig.Integrity.IntegrityCheckLevel = integrity.CheckLevelCorrectness
	helper := entry.NewSchemaTestHelperWithReplicaConfig(t, replicaConfig)
	defer helper.Close()

	createTableDDL := `create table test.t(
		id int primary key auto_increment,
 		a tinyint, b tinyint unsigned,
 		c smallint, d smallint unsigned,
 		e mediumint, f mediumint unsigned,
 		g int, h int unsigned,
 		i bigint, j bigint unsigned)`
	ddlEvent := helper.DDL2Event(createTableDDL)

	sql := `insert into test.t values(
		1,
		-128, 0,
		-32768, 0,
		-8388608, 0,
		-2147483648, 0,
		-9223372036854775808, 0)`
	minValues := helper.DML2Event(sql, "test", "t")

	sql = `insert into test.t values (
		2,
 		127, 255,
 		32767, 65535,
 		8388607, 16777215,
 		2147483647, 4294967295,
 		9223372036854775807, 18446744073709551615)`
	maxValues := helper.DML2Event(sql, "test", "t")

	ctx := context.Background()
	codecConfig := common.NewConfig(config.ProtocolSimple)
	codecConfig.EnableRowChecksum = true
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

		for _, event := range []*model.RowChangedEvent{
			minValues,
			maxValues,
		} {
			err = enc.AppendRowChangedEvent(ctx, "", event, func() {})
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
			require.Equal(t, decodedRow.CommitTs, event.CommitTs)

			decodedColumns := make(map[string]*model.ColumnData, len(decodedRow.Columns))
			for _, column := range decodedRow.Columns {
				colName := decodedRow.TableInfo.ForceGetColumnName(column.ColumnID)
				decodedColumns[colName] = column
			}

			for _, expected := range event.Columns {
				colName := event.TableInfo.ForceGetColumnName(expected.ColumnID)
				decoded, ok := decodedColumns[colName]
				require.True(t, ok)
				require.EqualValues(t, expected.Value, decoded.Value)
			}
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

		decodedColumns := make(map[string]*model.ColumnData, len(decodedRow.Columns))
		for _, column := range decodedRow.Columns {
			colName := decodedRow.TableInfo.ForceGetColumnName(column.ColumnID)
			decodedColumns[colName] = column
		}
		for _, expected := range row.Columns {
			colName := row.TableInfo.ForceGetColumnName(expected.ColumnID)
			decoded, ok := decodedColumns[colName]
			require.True(t, ok)
			require.EqualValues(t, expected.Value, decoded.Value)
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
			require.Equal(t, ddlEvent.TableInfo.TableName.TableID, event.TableInfo.TableName.TableID)
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
			require.Equal(t, decodedRow.TableInfo.GetSchemaName(), row.TableInfo.GetSchemaName())
			require.Equal(t, decodedRow.TableInfo.GetTableName(), row.TableInfo.GetTableName())
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
				obtainedDefaultValues[col.Name.O] = model.GetColumnDefaultValue(col)
				switch col.GetType() {
				case mysql.TypeFloat, mysql.TypeDouble:
					require.Equal(t, 0, col.GetDecimal())
				default:
				}
			}
			for _, col := range ddlEvent.TableInfo.Columns {
				expected := model.GetColumnDefaultValue(col)
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

				if event.IsDelete() {
					require.Equal(t, dec.msg.Type, DMLTypeDelete)
				} else if event.IsUpdate() {
					require.Equal(t, dec.msg.Type, DMLTypeUpdate)
				} else {
					require.Equal(t, dec.msg.Type, DMLTypeInsert)
				}

				decodedRow, err := dec.NextRowChangedEvent()
				require.NoError(t, err)

				require.Equal(t, decodedRow.CommitTs, event.CommitTs)
				require.Equal(t, decodedRow.TableInfo.GetSchemaName(), event.TableInfo.GetSchemaName())
				require.Equal(t, decodedRow.TableInfo.GetTableName(), event.TableInfo.GetTableName())
				require.Equal(t, decodedRow.PhysicalTableID, event.PhysicalTableID)

				decodedColumns := make(map[string]*model.ColumnData, len(decodedRow.Columns))
				for _, column := range decodedRow.Columns {
					colName := decodedRow.TableInfo.ForceGetColumnName(column.ColumnID)
					decodedColumns[colName] = column
				}
				for _, col := range event.Columns {
					colName := event.TableInfo.ForceGetColumnName(col.ColumnID)
					decoded, ok := decodedColumns[colName]
					require.True(t, ok)
					require.EqualValues(t, col.Value, decoded.Value)
				}

				decodedPreviousColumns := make(map[string]*model.ColumnData, len(decodedRow.PreColumns))
				for _, column := range decodedRow.PreColumns {
					colName := decodedRow.TableInfo.ForceGetColumnName(column.ColumnID)
					decodedPreviousColumns[colName] = column
				}
				for _, col := range event.PreColumns {
					colName := event.TableInfo.ForceGetColumnName(col.ColumnID)
					decoded, ok := decodedPreviousColumns[colName]
					require.True(t, ok)
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
			require.Equal(t, decodedRow.TableInfo.GetSchemaName(), updateEvent.TableInfo.GetSchemaName())
			require.Equal(t, decodedRow.TableInfo.GetTableName(), updateEvent.TableInfo.GetTableName())

			decodedColumns := make(map[string]*model.ColumnData, len(decodedRow.Columns))
			for _, column := range decodedRow.Columns {
				colName := decodedRow.TableInfo.ForceGetColumnName(column.ColumnID)
				decodedColumns[colName] = column
			}
			for _, col := range updateEvent.Columns {
				colName := updateEvent.TableInfo.ForceGetColumnName(col.ColumnID)
				decoded, ok := decodedColumns[colName]
				require.True(t, ok)
				require.EqualValues(t, col.Value, decoded.Value)
			}

			for _, column := range decodedRow.PreColumns {
				colName := decodedRow.TableInfo.ForceGetColumnName(column.ColumnID)
				decodedColumns[colName] = column
			}
			for _, col := range updateEvent.PreColumns {
				colName := updateEvent.TableInfo.ForceGetColumnName(col.ColumnID)
				decoded, ok := decodedColumns[colName]
				require.True(t, ok)
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
				colName := updateEvent.TableInfo.ForceGetColumnName(col.ColumnID)
				colFlag := updateEvent.TableInfo.ForceGetColumnFlagType(col.ColumnID)
				if colFlag.IsHandleKey() {
					require.Contains(t, dec.msg.Data, colName)
					obtained := obtainedValues[colName]
					switch v := obtained.(type) {
					case string:
						var err error
						obtained, err = strconv.ParseInt(v, 10, 64)
						require.NoError(t, err)
					}
					require.EqualValues(t, col.Value, obtained)
				} else {
					require.NotContains(t, dec.msg.Data, colName)
				}
			}

			obtainedValues = make(map[string]interface{}, len(dec.msg.Old))
			for name, value := range dec.msg.Old {
				obtainedValues[name] = value
			}
			for _, col := range updateEvent.PreColumns {
				colName := updateEvent.TableInfo.ForceGetColumnName(col.ColumnID)
				colFlag := updateEvent.TableInfo.ForceGetColumnFlagType(col.ColumnID)
				if colFlag.IsHandleKey() {
					require.Contains(t, dec.msg.Old, colName)
					obtained := obtainedValues[colName]
					switch v := obtained.(type) {
					case string:
						var err error
						obtained, err = strconv.ParseInt(v, 10, 64)
						require.NoError(t, err)
					}
					require.Equal(t, col.Value, obtained)
				} else {
					require.NotContains(t, dec.msg.Old, colName)
				}
			}
		}
	}
}
