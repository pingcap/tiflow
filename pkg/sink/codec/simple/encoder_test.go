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
			require.Equal(t, insertEvent2.CommitTs, decodedRow.CommitTs)
			require.Equal(t, insertEvent2.TableInfo.GetSchemaName(), decodedRow.TableInfo.GetSchemaName())
			require.Equal(t, insertEvent2.TableInfo.GetTableName(), decodedRow.TableInfo.GetTableName())
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
