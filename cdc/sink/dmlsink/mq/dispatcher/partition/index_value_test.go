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

package partition

import (
	"context"
	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"github.com/pingcap/tiflow/pkg/sink/codec/simple"
	"testing"

	timodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestIndexValueDispatcher(t *testing.T) {
	t.Parallel()

	tableInfoWithSinglePK := model.BuildTableInfo("test", "t1", []*model.Column{
		{
			Name: "a",
			Flag: model.HandleKeyFlag | model.PrimaryKeyFlag,
		}, {
			Name: "b",
		},
	}, [][]int{{0}})

	tableInfoWithCompositePK := model.BuildTableInfo("test", "t2", []*model.Column{
		{
			Name: "a",
			Flag: model.HandleKeyFlag | model.PrimaryKeyFlag,
		}, {
			Name: "b",
			Flag: model.HandleKeyFlag | model.PrimaryKeyFlag,
		},
	}, [][]int{{0, 1}})
	testCases := []struct {
		row             *model.RowChangedEvent
		expectPartition int32
	}{
		{row: &model.RowChangedEvent{
			TableInfo: tableInfoWithSinglePK,
			Columns: model.Columns2ColumnDatas([]*model.Column{
				{
					Name:  "a",
					Value: 11,
				}, {
					Name:  "b",
					Value: 22,
				},
			}, tableInfoWithSinglePK),
		}, expectPartition: 2},
		{row: &model.RowChangedEvent{
			TableInfo: tableInfoWithSinglePK,
			Columns: model.Columns2ColumnDatas([]*model.Column{
				{
					Name:  "a",
					Value: 22,
				}, {
					Name:  "b",
					Value: 22,
				},
			}, tableInfoWithSinglePK),
		}, expectPartition: 11},
		{row: &model.RowChangedEvent{
			TableInfo: tableInfoWithSinglePK,
			Columns: model.Columns2ColumnDatas([]*model.Column{
				{
					Name:  "a",
					Value: 11,
				}, {
					Name:  "b",
					Value: 33,
				},
			}, tableInfoWithSinglePK),
		}, expectPartition: 2},
		{row: &model.RowChangedEvent{
			TableInfo: tableInfoWithCompositePK,
			Columns: model.Columns2ColumnDatas([]*model.Column{
				{
					Name:  "a",
					Value: 11,
				}, {
					Name:  "b",
					Value: 22,
				},
			}, tableInfoWithCompositePK),
		}, expectPartition: 5},
		{row: &model.RowChangedEvent{
			TableInfo: tableInfoWithCompositePK,
			Columns: model.Columns2ColumnDatas([]*model.Column{
				{
					Name:  "b",
					Value: 22,
				}, {
					Name:  "a",
					Value: 11,
				},
			}, tableInfoWithCompositePK),
		}, expectPartition: 5},
		{row: &model.RowChangedEvent{
			TableInfo: tableInfoWithCompositePK,
			Columns: model.Columns2ColumnDatas([]*model.Column{
				{
					Name:  "a",
					Value: 11,
				}, {
					Name:  "b",
					Value: 0,
				},
			}, tableInfoWithCompositePK),
		}, expectPartition: 14},
		{row: &model.RowChangedEvent{
			TableInfo: tableInfoWithCompositePK,
			Columns: model.Columns2ColumnDatas([]*model.Column{
				{
					Name:  "a",
					Value: 11,
				}, {
					Name:  "b",
					Value: 33,
				},
			}, tableInfoWithCompositePK),
		}, expectPartition: 2},
	}
	p := NewIndexValueDispatcher("")
	for _, tc := range testCases {
		index, _, err := p.DispatchRowChangedEvent(tc.row, 16)
		require.Equal(t, tc.expectPartition, index)
		require.NoError(t, err)
	}
}

func TestIndexValueDispatcherWithIndexName(t *testing.T) {
	t.Parallel()

	tidbTableInfo := &timodel.TableInfo{
		ID:   100,
		Name: timodel.NewCIStr("t1"),
		Columns: []*timodel.ColumnInfo{
			{ID: 1, Name: timodel.NewCIStr("a"), FieldType: *types.NewFieldType(mysql.TypeLong)},
		},
		Indices: []*timodel.IndexInfo{
			{
				Name: timodel.CIStr{
					O: "index1",
				},
				Columns: []*timodel.IndexColumn{
					{
						Name: timodel.CIStr{
							O: "a",
						},
					},
				},
			},
		},
	}
	tableInfo := model.WrapTableInfo(100, "test", 33, tidbTableInfo)

	event := &model.RowChangedEvent{
		TableInfo: tableInfo,
		Columns: []*model.ColumnData{
			{ColumnID: 1, Value: 11},
		},
	}

	p := NewIndexValueDispatcher("index2")
	_, _, err := p.DispatchRowChangedEvent(event, 16)
	require.ErrorIs(t, err, errors.ErrDispatcherFailed)

	p = NewIndexValueDispatcher("index1")
	index, _, err := p.DispatchRowChangedEvent(event, 16)
	require.NoError(t, err)
	require.Equal(t, int32(2), index)
}

func TestIndexValueDispatcherXXX(t *testing.T) {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()

	_ = helper.DDL2Event("create database uds2")
	createTableDDL := `CREATE TABLE uds2.Data20 (
		model_id bigint(20) unsigned NOT NULL,
		object_id bigint(20) unsigned NOT NULL AUTO_INCREMENT,
		object_value longblob DEFAULT NULL,
		version int(11) unsigned NOT NULL,
		PRIMARY KEY (object_id))`
	createTableDDLEvent := helper.DDL2Event(createTableDDL)

	insertDML := `insert into uds2.data20 values (439623264, 49842, "a285a0hmQXdwVVMyOUFSTFQ1ZDRtZnEydlhuNjU3U0IyakJ3aGZvTGJLdUVDMXJpajNKYzV3SVpIbkM0OUxFZnhTTTBYSUVKc2MzZW9mbDF4UmEybXdCUjdjTURZcExzWUJCc2tZNUNzNUs0VWhVdGkxN3lYdlZsMWNURUl6UlRKN2IzbWVuRndiVkVwcDRyVmszQ0I2SGlyRGkwMmhXeE5RZngzaHBzemlKaFNNV2FaUmtuakxlQXlsNzd0ZlVZSkpuOGxTOGJlNkJwNk9JS3FoVG5XQ3V6SVZRQ2tPMzgxRkx3MGJjRWtxNTdKVGdNa3F5WjJXSXdkZk11RVdPakxmeXJzSE83OUxEQnZiUXZ0ams2aVV0bThtejRmVmNPM3AwNm9UREt2VkJyYnNESVI4WUw3QlFvZjBCb0lGRjBoN0xJdExsR1dJT245Yk9TWE9sbHVyemtJbjlkMjJTSm90RGYwSElhOG9leno2bVVYa3N1QW5nRTQ1OW42Tm1XaWtVT3ppNkQ1Vk5HRkVZdEowV2g5dVAwQXdkeVl4Y2VTaDM3cFczS1RadVo3T01FM2piS25Xbld4NWZ4b2NVNUhaSVc2TE53eEZvZHA2eDRoTVJocFNaUG1lVHBJODNBa05aNmE5bXN3UFBmMHoyR1FoaE8zRUdCY0FudWpGSHVqYU82SnREbzNnSW1kaktYR0hieGhHTVVXbjRLVTlweFZabjlxbmttMkk3YTVqbnl3aVB6U3dPeDFacjZqcFdaOG9kZnplcklBSTBJQm8xVEJhdE1lY282ZnB0OFMzZ0lJMURNcnRHVWZPU094c2JEMUtnR3pyMXJtTFY3dE5yWDVITXBmUE9JaWZWTnhJMHIxdUdKWTR6NTFKZXBhYXdIaU5ITDA2c0RkUWF1MmJOc1FaQVVJTnEzZWRFdnBTMFV6R01kcWFXSEZCeVN0ZlJqUzFTR2RKZ3hCb0ZMQzN3c2V1MkFORmc5eUZqT2JIa0hoNFU3eHQ2cFVqR1c3OWFkODRrbzZ0SmxENG5Lb1FSVFF4N0w1WEp1ODlkNjF1d1JTQzc1Nk14WkJUend3UUR6aGdCNkdkZW1QMlR0dE9YYkVXUHVFeDNRc2w2VXpjRTNhNUVWVXdJdEdNTTd4T25rWDRqVllQZzdoOW02a3laMDdGM3RjUEowRTJEbldHY0FrbXJOcTR5NEQ1RkY2cFdlcXdsMEdhWkpNbUJNODhLQ2hMTUNSR3BYckZYMFRUa2piWVd2UlJ5RnZ3RmRFYnZWcFJwYVR3SUZSVFFtbFFPcVc2Q3JXYTNBcWVWdmN0VEs5eFZxTFFFOFd5bHZ2T1VPWnUyYUZGc0Vzc2oyYnRpWnhQaE1uM09FZ2lVb2RQcEtxSTg0d1YyNVQyaHFVOGJBRUt4TjVuM2FFbTJwNjBab1RsWGRyZ081UDkzR2NtVm1HdHZZOTA1c2lSTWoyZkE2YXFzNkJPR05YeEJ3S3hTTVZQdzVqbmFPbTR4OXFzUlA5M0lqOTl3aWtVRldBeXA2VE5LQWZVc1JldzZhY1dtZWx4elI0Q3BIc1F3NE55T0p5eG9XdzRHSTVwdG9tWmdjejFXcWhOSmN5bjJySkxGZ1Y0ZnpXUEZBaUlDeG1yNzZ4N3BZbGF1ZzUxUENjOTFBb3hGOVZUa3J6bmtmM1RDNEZWQ0tCV04yb29RREZibVBBMVk4U2FUR25RUDhvZnVKeDFwT1BJNVh4Rm1kRHFtRFZUdEMyWktsZ1FsV1BJUndzbzE5bkRqd0I0anpvZ0FmSjVJV1hJT2ZMSWZ1NUxuQ2VlTVpCRFV0Q1FoVU5ZV2phbkE4bHVXdktCaUxpSTZZUG5iTlRDdW1oYTQ0WmJNS29pNnZUVk5NblRONXZNSXFVTUxOYVlWZ0ZlZDBaZDRQN0FDZkg0S0kyd2Y1dFdmUzlwOVB0MWVlUUtxMWlVTHFid05md3FUZDVidTBIT2dQN1lRZTUxNHp3WktRN2RHcXRsbEZaZlp3OWYzUU1oNVdiMnhBQ25VMzZhVXhrZUhBc0NOVkRXY2wxUnpyNURkQm90V3VYaVhkaERIanNWenpYQTVVbEZFdWg5WHVXWHdzQjBFOW9hVXFaMWhYZGhpdjdMaTlJRzMyYVZSWEJ5RlozTWtPMURyamlXOHBMQUNtWmg1eldJNzhqMk9NTnBPbm5DU3g0cWdFakwxZXM4YUNTVnY0R20wNlltbmVaVEFNSnE3RkVsRXFyYnBnb3JXR3NqaHdRcWJvMmxuZHVlMnBJdUdFeDZ4aU9EN1hod25OV0ppcndzVmlQb1Y5aFBJdFhyczI5NklscW13c3FQbnNjNmVwdEV5cG5HNHN0VmlvTGJSMUdQSXFNZGZTVGhpMjVnN0l2eDBSMjhxWTFwQzRyWHZtVFBPaVZaZUdtNGN3a2VTQjRxTGh0cTR4UGphenRDdzZ1ajhJUU1HTmgxR1dRUEpaeFlnT0hiaFFsdkdXdFFlYUpNNzFIM0lhdzZRck5ZQkE4U0Y4MzMzcnpCTGhodnhKeGZCM2VhcTI1djNSc0JaSjc2bGdQWFlJdWdjd1FVamF1NjJTYXN2QzlUZ3JQaVRoZjhPQnozZUU3VE9XQTV1Tjc3Nm9JcG9GVUpyd3VZUzFnT3hIcEI2MTlrYWs0eGRnYVVxU3JySjVXV3FVVlc4TlZUS1NIdVQ1a1pTSU9pQ3pXN3VoaFdTNEhZVVZvSmVOTFdhSUxlNTRiY05ncGF3RG5Rb1ZtRkhxNkszd0NKQnBJWWhBWUtPNUJYZmFvdHB2YVZnc3RsVnpIRE51YTF3Wk9tV1E5ZU16djRDZ1d0MllVNTBNa0ZVV0p6YmdaeUIwWUFtbzFXZUFQVWhES2FZTmh5a3ZqYWtMRk4yVVJXU2x3Y25SSXI5SEpTV1VrR3pySUdQTjhqSkZtYk9TSUo4R1E3UXNXeG5LcW1JU2lhQjNSdE5Pa3dacGUzWXJrYzR5bjZkV1Q0cFNCYnJvWWZzRzZuSUNHdE95dVFPNnJURE5oZ0xhbXIyYnlVQjUydkVUZXZlbXRQT2RNRnBDZE1BYkwzdzVpT0xxWHpsSUdSYnF1OUxoOGlWVlFDMGFZR0tNWTZUUzNmUnNZVlNTVnBzZEhmNTRoUnZySzlxT1l5cW96dVpaTno1aktpdEI3UzBwWjNBcEp3cW0wekRpalcxUVVUaFh4bnNXMkZQWUhvSmhqOUU4RUhSaWtYMFJkTWpYQzZ1WU1XRUxjSFNYNzM1enVSN2ZGWlNsWDJFVFVJOWtXa3RCeGNjUGc4V2pWRG9MVU43NnJOQjZBV1Y0UmxURE9QSUVGSW1tWXNtb3hrMUhLek5FZEdCZ2FnT0Nub3pNVEVsdXBaMFFRWHl5bTQ0emY5Rkw5YWhadXV6em1qcUhVRUxjazlucm9xSlNOdEhEeGJycEYzcmluT2lJeW9Td2xpNVBQREhNUEROUGdpSkI5QzZPSmx5eTFpeE43ZEJBV3FFaWN5cGhUaUljelFKMmZGMHgwZEtlRnI5UUhwNFJ1NUFpMXFYaWMxejY3S3ZwUEFrSTVLMlpnYURNbkdOc29TVk5TSjJyWU9Pa29ZWjZ5elZXNmZDQ0R6R1FEeEFFbTlocWtSM3FFcDZNVEhyMkp2a2dHWUR1N0MwcEJRd0VyOTVKd0hvMVJOWGd4aUJNS3owTWFEVmRqU1BEa245RkNVUmlMeWcyZGJvMHBvRkpjRXRkQVRyTDU5QVIyUEd6anpBQUQ5MWRIdDFLTlM0Y1FYa3F1UkQzVlpQYVhvQW1oOURUMkZSOVYxYnlFY1FlR0xLZFlOdWdWc2R3Q2xPck15RFNjeXB0YmpCWmpLeGdLd05aOXA5ZVlMU09SeVdIUUNSaTlhVDVqTmFpckVHZExqTGFydDBScnR1cFdSQ29Fdm1FMmxFNkxWMWtLdEJLYzRJUk5IZTF3ZGFBVzZmSUx3dlJHeDFZWDRCU1JXdGFmTnNCVXRCcmpUeU5mMkdGOWh0ZVZyT3FWWGNhMGJZOXZvc1o5RmNzVDJkcnJkWDlWS0NmRnpXbHNLYW5FdVQ0MG1NbndXaEpaSW9ZUnBWUlIwa1BaaXlyaFFNa0N0Y2NJdXlEV2t1a2FESnFaU2d3TldCd2Jmck95NzNpblhyMHJXVVBKZlBuVzlTcWxDTmRxMjNjdkh2bENRQ3NrNUFVN1VYZGJ0TXJ5OUVWS3N6Z3M1ajlYZGx5VzFEdE1lRjhlU3l3YXQ0YjdyV1I3WncydDNjTEdjclo4NktmR09SdlE0STB2bTdjZWx2MlYzMGtVTkx4bktGZDV1akxLbG50dkVEOURJYjZYbmlPcUt5VXpUMXdhYVBmVTdSelNiOWFzckpmYUIzNWp4NjZFUmRGVzZrY0N3R1dpYXRRY29aZVgybXAybGY0RjdONmd0bGoxUVpLNlV6UjNqenJWekUyaWtSTUp6MkFuWThORkxDSGZzZVYwUExtWXZmOWxLcGsxODM1VW5VSXpvaWRxUGl1NlFOb3FHeGg1THhmMGJjTEhQZHpRT3A0bk9IRWg2V24xM1NNVjFmMkpLUE5DQUlyd1lUeVczYmliVVlnN1NZSDJRcEZ0bURzZ0p3RXJseUdEMlppaGNNVU94dVlvOVA1bzhGSnh2RVB6c2l3d3VCeTJYZHFKNFVZelhIeVRKWFpyczl2aTRhSkNobnF4SDFTM0x5NjU1RnkwWmVCa1QzR1QwQXBVOFJmb0FWdlh1MmFqeUZLY1VGcnVldFhXNnFMTmZwN1d4SVkyYTJ2VzB2T2ZadmpGUDdMelltZ25BY0Yyd3hQeFlYR1M0Wm9CYkdmSVB0WWVQenNiUVFEckhubVJvTkVLTU5DZE5CTGVlVk42eVh4WWpBY0UxVmNZd2VqNHJUQmx6SEtXQTllM2x4dEJSM2tDNjBaNmNjMlliejhCVWRhNjJRYzR2d2JoNGZqa0N4YnNpM2gzNlJkaVcyQ2t4ektBTjAweUNyeXE4c2N0emRIV2c2YW9KRTl5WTBwV0Fxd2I4UlJZOHpRUmdzdXhiUFZlemhuMHlYWlJVMW5PTlFXdWpNVnM1em53bWUzU1JkWWM0MlVCcGgzWGdreTFDUkUwOWJPVzVFd1dCRmtGVXJqc25GdVB4Z2laa3ZqekRqWFpWbXFrZHNiTmNzems2TURFaWlDbnZLOEVzYVRrMWVSdXhDV1ZxVDBHZ0loOGpTOWFyRnp1SjFPZVdVckdCNHVYN2h5THJOSTVUOEI4R0g3UFFzWm45cVl6VnNnT3NKa1hHS2tOYVVtbHplUWx5R2VTMjNUVWt0SlpKUm9YSkViQkg4T013dWZmNGx2Z1dTYTBKWDQ3T0lKenpCWlRjdVg0Z0RFWjZPSDY2WGZBc1hMczFva0tEejhZQ1BmYm4zSEZLNTNuWFhaWmxDUWRhbTV2emJXdElXdVZaQnlFelZMVDUxWkdKWFVXbFZ2cVhSd1RpTXhOYXlUb25IWWVTa0M2SHhlaVprZU95QkVNOU16c1ZCZ2dyUmlCRzlKeHNJQ0N1WGRFZGtWU0FYZzVwSGdrdHFSV3FsUG8weHBKcVJSSDdhdXdRZUhXcm1MMkFaN0tmN0pjN0dzWDN3OU15TlR1TE41STNnQktCckkxSHZ2YUdiTVVaVzdvQlN2dnV5c0tKc3VkNVd5NEY2WlR5WFRoR2VGYWVyZ1dNeVZCUU4yV2hic3VUZ0ZLQjVMdFM4azNvZE9tTG1QdE8xVk5tTU9kMVA0d0ZiOEl6V1JLQUxFUVphVGZnOUxzVFlJNXo2bmJMTUhFQ0lYQmNqWmlBcXRyekNMMnlYUjZhdllnbGlONHc5NWRiT3dEbEluVkNMMWZKSGN2ZUdxNnRpVFBsbGZqbGZENUJIU3RCSmx1UGpkcHJBeVdJZmhpa1NIWjNQc2c5dEZxQ2tQd0NnZU9UalJxbWQzekVaM0hpakdvZUZzS3JvNUdGalJTNzE3c0FQQkVBc0lXbnh2NDF4WDZLRlVwVm9sWkF0VjZHWkRGR2RWN210Ymtvem5XUzRINTA4TTNONjZhMUVYRThhd3k3NFZ5dmdkWWFCQlNXYUFLMTlaQXQ2cWFlZU9wU1lKMHNoZEU5SE1xeEhyQ04xUmtFdkV2UWU2SlJCQnVVNFp0b0JOd0dMQmVwbTJ6T2pVTHRxMzlZczViOFBZaUJud2dJcExuV0c3SFNycXhocVdVVnpGdlVhWGcydDdZWVNUS1BINVBVNmt6ZkY0SEM1aW9rYnd6UVR6bDRrTnU5UkY3OVhCNjlGa3ZRZXptWjlvbzdRRzFiUjJodTlBREZVNk9FRmNsVGRMQlVjMkdyd25GdFo5cnRWU3ROOTJJQ1I3eFpTOGFVTmg3ZmVsVUZpYmdTa2pGVFZyZzNneVZPbWdMV1hyRnRkTXhmYzAxTTBOWlFjc3pIM1E5MFpsUno3akpwa1FBOTZSdVV1dXlhQ054Q0VjOXJqdG1rcFk1NFdpWE41QTBqWjNKcUZGZHZSYUx5YU1zZWF0NG5IdnE0T2RMNFR6ZGhxYkJUdnZVM0NRRXN0RloxMWpXVmlVQmFiNFpMT1hNZ1lhaXBaakU0VWQxTHNmOHhwUmkzMDBDSjRzYWxKYzBHUGJ3eVFNWFBaNW1ZUmJkSzhldENiRHROQjlaNENlcVdhNkFCclZOdEgxSDRuN3l5OXFVcm1DQkxIRkVPZEYxbElQOEQzQVFISzQyMkcyeTcwcmJxQ2pCT0tObjJRS2xRaXIwdlhNWG90NG1UWDhZd0VpWUJUdndTWXhhZ20wVUNIZUFiSVA0dVhOakxOM0I2b3pVS2hNaHJWMjRlRWZTeEpQNXFtcHluUzVDc1pmbnhxbThPRGZRSXZqTTlzTEtQMXpwY1h4R01PcWJBcXV1bWtwQVd0cE01cGZueWozeVU2eDdRa1lJUG81RWtSWHlpcGFMRHpha2JDY3pzM29hOGZyWVN4VHZDVkwwTTNpTFp1bnp0VzhGc250QTd2c292NEpxR2ZXNE5wRjN2OTJUNkZkRk01QTlwSDBWWVkwTVo4MEFDRzNKUm42enNXTWJsbzNxQ3pTREgwbWVVMUpVRmdjeWVlWGI2MDRENWtNRm41ZHNHc2d3eEtXb2JsU202Zk0wVG44bHY4NG82UjZTSEhrWm1VcWtNVGd5enBPZTlEZ2lkVDdCYU81YmlPUERwb0hYWm5LMWh0eGM3Q0VEaTJHeXF1Sm9BaEdrRFNWRzFQNnZOZWFrbU1zTU54MTRXWk01ZGswSFA4QWxDOWNjcWtyVlEzS29ZM09VMVJmUldyRmtPRVFUaHNhckVSYlFORzBYUnFFZXg3c1c1Y0tzUERRUmpLbTV5ODluZjVRODBNUHdPNno5SkRxM2NrS1JSd1g4OE14S2RuOEx0OGRFSDJLMkh6TkRyWEJPQ01yd1dHYjVTYmlVUHFuQmRPWHpNcFNSWHJ1MHRpUmZNOWpRWkJGcUpZcDZ6QlV3Q1lvR09XTmc4R21lSjZiUUZjeHhqblRxQnNMSUZCZEd2UWVPQjh5QnBCNEgzc1VSd3pYSjFSMkpCRGFKdmg1eVJTREpwVVZybmk5UFljdWlObE50cHVyZjgzY1gxUnVhWlprTmcxcHM5ZUhISzhwblR4MEdiYVpHUThIa3V1SGZwYnZYQlpZUEh5STRDbXFmQ1RvdnpPb1pad2VBQzA4WEc1bXhvOE9NaGhQZFNqbzQyc0YwVTZsa0VmelBod09sODR2cU9OT1cwVTRqdnJwRGJsbE1ydml6b2ZsTXJPYkg4SVVLdGNqTUdOc3ZqUXRQQXM5QTFKdFViMGp2NU4zUTBmMWhrUW9XTzR5eE1VRU9XMEFYaFNqSXQzNWNISGtmTU1XdFZvNkdnQm5PUlhYRWduNEF0Y2NYdG56M21WeGE4QXFlTkRIQUNrRTNONXJzTFptOERQUm1tZERzZm1zeU9sZmpoNlB5NnYxOTRjVm1RMmdBa01kUkxoY3pVZldBeGtTSDI1NkZrRzNSemc4cTVJMXd1ZnZmYVg4bTN4SW42MkJZME9OVVVSOHFsTDR2UUdubldlNXZVUnVIalhWMnVrMktNUmM2czlnaWZDSDZOc3d1Q2dOdUdwalNzZUxVbWpYRERKcmdWR3owcDB1c0t1TUpUc2RUd1k4M1lCck5RN2o4UkE5Uml5eEVuOXZ5aVJIV1dRdUFPaTd1SktnOUlNdm0xR0JJOFFPZzFYUHptVnVjNGM2ZmlLMElrNHdlM3g3eTRjZHgxT3pmS2VJRXJBQTdYU29QOWxMNGJhUU5qNWFYSEVyQ0tRU0NRdEIyQWZIRWdJeFlaMlBxdFVTQzRWVHpwUGdXd2dqUmpYakV2UXZyOWdKRTBGWkVYcVZZYVRDclJsYzI0V2lhYTRXN0VnYW53RXlzUEd5cWpFSktoZWpUUVk1aGFKdzgyMFJucXFadndDUEJqU0l1Rms2dTd0RWZyV3dwemtIOHBOWEJyTGdwcndrS09zTjY5ZXBmcmoxWGVhbGo1b1l5VGgyeVRNSW5mRlBYcWtYclUyV0FzT0lmTmpoazVzNFR6MjBWaFplRTE4RGY3aEVmQXU3OTJwdkxUTkVPcWNpNWpjOFR5ck1zejZ4TWhwd1BYWG9kM1lZSWQ0QmIzbnpqd3VrcUF5dTJ4YnVRTFlWMGtsVUVGMVBObGNpRFhNb0ZFbWZCbTJVVVBUSmxRWkpCVFdNZG5oYWo4S1hCVHNZNGhVTnB1cUNES0pINVdwak1yNEpJSmQ1VGl0QXVNWmVOY2ZPSFc5aW9rTzZhSzNTUE9WWURnbm96VjJCRDExSzR6dTFwZjJIVlVZbHlQQnB4WjVHSVNLaU52MlUyazh2Z284a2RKSUVMM2lsbVhHQlBqYk5vN2NmTktFV0hmZ0N0bk40b1owbkcwMEZlUk1nb1U5RE9TSTgzbWZTbGRkSXNWZVA1eFVjS2JETEpBTWROQzhCV0xWVjJ0UWJaRXp2RE9QNHBKU1UyVnBueUVNOTNLYVFqaEtxYm14ZVlxZTNzM3hRb2N6TjBwMXZiYnJXT3hZNkp6SjBQS3dqU2VxOUpyd1lFcXM2eEZJbzB5OWFETTJGVzRtbU50dk53dmpQOHZPSkJNZ3luRVRLWGV2Z0ZSSDc3eHEyWEhuTm9tZnhvM3FsWXRxOTFia1JzMVl6cVRJbmVtNGxBZ0JuVW1IM0ptU3ZEeWxMM1c1SWdQWkx3cEtOdHNMcVNLNHAwd05RelVES055QmZvTVNWZDc=", 617447067)`
	oldEvent := helper.DML2Event(insertDML, "uds2", "Data20")
	preColumn := oldEvent.Columns

	deleteDML := `delete from uds2.data20 where object_id = 49842`
	_ = helper.DML2Event(deleteDML, "uds2", "Data20")

	insertDML = `insert into uds2.data20 values (439623264, 49842, "ZmVicm93dWxncWh0ZXp0YmNoY2Zud2xycm1mYW9hcGpjanNjdm1qZnl2d3ljd3psZnlhdG16aGZwaXpjY3JvY29hb2RxaGpqbndkamxhcWdyZ2hyd2h6cXV1dXR0d2V6eXZlc25zbGVmZ2RjcnBoeWdvbmVsb3JwaWRqaXJ2cGxwcWRqdWRhaGtkdmRkc2x4aGRwZnNwYWpyaG5zdXd6Y3puZWF6Zmp4dXJlbGZ1cnhycnZveWFrdWZ5a3h4enh3Y3FhY2tlb212ZmVuYmFidGlwc2dvbXVvaWFmaHJkb2t6cmVmeXRibm9scGlmY29hYWRvcnF2ZWl4b2lwZGdnaWlqeG9hZWZhZW14c2lheHNkbXJ1aXJmeXFqbWFuZ3NpaWdkeXN6c2h3YXh4d2Rpc3pocmZxbG55enZ2dGp1Y2huZ3dseHRyeXBsaHFzcWtkemV5YndrenplbGZtcWRpeG1zeGl1ZWx2aW5memZoZ2N4Yndwc2Fqc2NlaWZjdWxibmphd3dueGRoeWZxbG9iaWhzenVtbHdzanRyZHZ3ZWFuZndmemN3ZXVrc2xmY295YnZ2b3htZWlhcXNxa2p3aHBna3BxZXBtdmZ5aWV4aGxpYWJ4bWtzbGVpdHZ1bHJrbnNrdnlrdmxmbWdybnZoZ2V6Zmt5ZmxsZnJkemh4Ymp1bXdmYWxsd2d3Z3NibGFyZmpvcWl3aWRqZXl6Z3d4aWFsYWlqdXR5ZHJ3aXdhcG1xdG5weWNyZnlsaG93ZWp5a2Jya3Nxbnltc25mamdwd2h0YmZ1b2dldXRhdWxocGNwcnJ3Z2p2bnVhc2lzdnhpaWVzYmxocndldWlpcW51YWhxdWZhdWZnaWh3ZW5vdWplbGpkYndvb2x1YXNhZ3JmZ3N6dnpxaG1lbXpqeGJpbnBsZnNraG1mZmNqa2l6ZmRvZ3JscG50dGd5eHpkZWxjdHpjZm12Z3J3aWxpYmZ0ZG90dWxmYmxxY29pYWlwYmJqaGtuanNnc3pudGR5YmJnZW1lemZ0cG5sc2hzd3d3enpvYXBleGpxdnFocnR6dGxmc3JhcWVwZnhwcHRyd2VvemZvcHB4aGdlcndudm9oY2V6dmh2bnN4emZld29wYnB5bGt2aWV4Zm1rYXlnc21xY2xmdXptY2Fmdm9ua2x2cmN5Y3VodGRuY3lrd2ttemZnZWtkdHd3c290YnFmd2dmZHJrcnViZGRvZ2pvdXFhYnprbXF3YnpoaHpmb2RncGxyaXN3c21rZW14YW9ucW9ka2d6b25lZGpzaHl2YWpqZ3VibGtuZG5rYmppbG11dGdybG5obWZnd3VoYm1wYmJ1emthenphbWVtaWJyZXljY2dzYW9xemdiY3Fid3NzdXlrdGZha2FidHZiaHN2cnBjanh1bHF4a3NjdHFvdmdrdmhxcXBqZGN4bGVkZ2F1ZG5zb2FtdHl6b29zb3lqYWR3dGxyc2tqbGN4cnVybHZobHdsdWxnaGVjZXZxZHpqenFodW1pZ2JwcXprdWVhZXdtYmRjcWl3b2p2dm56aHRsdW1oa3JjYWVjZWVkdmh0d2NtaHh0d2JoeXZ6Z2FkZnZqaGZ3dG9oaXpmZ2Zmdmx4c3hienVsc2tra3lnc2RmdWllcGVsb3lsenNidGJlYnJnY2x3enloZnpweWh4cWl1aG9oemxicm5xcXh2YXphbWRrZ2FjaGFoc3RrYWl3dmVkaXN5aXhuYWpra29nanZtY2Rkd3l2anVudGlmeW5hcWp6dWduaW11cGlzdG1ibW1zY290bnZ6eWp1d2R6d2t0a3VxbnZrZGhpbXF1YXF3dWV0a3llanZlY3ptcGRlcGV6b3V3eWNyYm5wY3JmaWxuZWdrdm5xb21uYXl2aXJ0bGRrdGxtandub25jZW5kZ3FjZWxoaHVjeWRyYXl3anp5d25ieWVtYnFvYXlrdWhwb2hkZml6bnNkZnhqa2RmdHdkbG1wc216Z2V1c2VqeHVod294eHVseG91cGxycmZhc2hvcW5vY2ZuYXZkc29icXVoemlodHh6a2x4d2NldXJ5ZnB2bndvd3FtZW5yeXV1ZWZqd3Rid3Z3bmVpdmpua3l4bG52ZXRtd3NkbW5zYmxsbXdrYWJtbG1zcGZ6eXNzcHRxcmlrbnl5ZXV3dHV3a2JtcmZwc2Nub3djcHlrbGtrZGpnbGFmc2Zwb3lodndqZHZocHBleGlsZGpzZmNqb3NtcG1xYXd2YWR2YXF5eXZmdnNxbndtYW5wbGhqY21naW1tZnZodGlld3N6dGJ3bWRsYmRhc3hqemJ0Z2RzZnpia2tzdHFjeWlreXp1bHdrb3B5aXN5bGxiam9qZGRyYmNxYm5saWVna2RieWt0cnBhbHFxdXV5cmFseXB3amt6bnRqd3htYXJleHZ1c3B4bGRwdHZ4Z2J1bXpraXh6eG9kZnd6eHVqeHByeHFpYXViaGJmZ2J6Y2xheGdsY3Ftc2N3cHJtdnRqaWdtZmxidWxpYnJjZGx0a25xdXl3b3RmaHlmcHR4dGd2Y2JsbGZnb211cWRlYnR4cW1hcHB6enRqcnp5dmt1eWRvd3N5dHNtbmlpY2ZnbWJ2bGhldXdhdHFhbGNjbmNrcGZibmFkZmdoa2tyc3NqdGZ1a3F2cHFoY3hpaWl0YWR4dmRhbGZodXFicXZra2tmbXN1Y3dyc2x0aWpyeWdncHB2dXhic252eWN5dnlkYmV4dG9wdmNoYnVtdXBpcWhnbXh1amplaXNkd2xweGxucHdkc2Nib3Jrc2x2aW15bnN3emtqcGdmY3dhaHpobXFveWJlc2tteGdwaGNmZHN6dm9keXBvbHd1dmZqbnFoandpaHBkbGNjd2FlZ3Z4am1sZ3ZrZGx0bW1vc3pieWd6bnJ5b2d5cXpvaHJsZmhpcWVrbnZ5anhkcXVteHd1bnJ0ZXRqYmxuY2pnY3V3Z3VkdnB1YW1udnBqYWhlemp0YW54bm9xeWN5b3NlYmF6Y2hnaXNwa2NrZWJrbGN5bHVoZmhyY3p5Y29rZWJxdXNrYmZ2ZXZibnN0bGF6dHh5YnNveWpicXR3Z210YWZqb3JneWh6YXJqYWNneXJjeHN5bWdjd2Jtbmltc2lrdWV6dmtxd3F0d2d3bG5mdnB4cWJjdGlma2dzamJ5Z3NoendwcHBtbndodHhtY2pxbGh0bmV4ZmNha3J3ZXlkcXNrZ216b2tzaXZmcW10bWVvbXJ4ZHp3cnZrbHV5bHloanFhbnRld2tyYmFsYm9uYXdnYXFlenhmbnBmdWNwaXdtZmtxaG9obGN1cW1zdHVqZWhrc2FudW92a2p1cGd5ZGt2eHFsZ2Ntd2ViZHB1eXJvYXVwZ2Vmb3RqZmxwcnZlbHJsZG5ndHB4Z3p3eXlueHRncGp0b292c2pydnRmd3R0c25yZ3FpY3ZoYWd5eGR0eWR0bm9hbXljbXJ3enV3aWF1eWtudWpqbmtleG9wYmNlcHhrbXJ2eHdqbWhzanJqbm5samNqZ2RudnJidGhvaHRpaXpic3NubnNpd2JvZXhodnNuY2N6ZGx3dmV0enh0cWR4cGNoc3Vlb3Zrcmp5emdxc2pxY2Z0bGdibG9zZW9jbnJ5dWlnaWt6aHZpYW13eGRoenphaGp1am5lZGVsYWh4emlsdWF1ZnR6eXVueHh2dW9neGp6Z3pydGtwcWxndnJzbHVobmRqcWllbmdwdnhydHR0dWdqZ2VyYXhzcGF6YW1vZ3hubG92Z2VseW9hZWF1cmhldHp2dGdwaGRseGljYWtyeWF1ZHRobmRzbmZ1cGV6dHBwcmR3d2Z3dWt3b3Jpb3p3YWV2b3BkeWV3ZmZ1Z3N6ZWhpbHFpb21wZXVnZ3dqcmNkeGdrbGZjcnlmYmt2a3R2aWVmbXZvcXlya2h6YnVrZmN6ZGZkYWpyZHdmaXdjemFiZmtvY2lsc3Zqd3Bnc2Nwc3dsZHZ5bXRnYml5amx0aGZ6Y3Rmd3dpZnRtZGd6aHdjZHB5cGRhb2JzaXd0YXpxamVrbmptd3BwZWtyZGtleXJwb211aHVyeXR4aWVpcXJta255eXNyb3Z1eHdoaXByZ25mZ2Fqb2JoY3FtcWJoaG9lZnB6aml2bHh1cW5qcGh2dWt4eHR4b3VlbXFmYm9ueGFobGdoa3N5amh6eXdxbnJld2dsZW5mdWdmZ2N4dnhpZmNyZ2JyZWdzZHlpYm93c3FoZGd4cHlzbWF0a3pmeHZ5aGZ4d2l6ZXh2dXF1aW9laGJicXN0YWN0cXdtcG5xdW1qa3ByZWh1ZGh2YnF1bndvdXl5dXl1cmRsY2xoa3dramxoeGdjYWhwZXpwZ3ZmZnJxZ3lpa3lvZHRheHhwdHF6dHdnbWxpdnBwbmRudGZnZW5pbGtvdXlsY3NpeXZ3eWhhYXpyeWdreGRteHN4eGdxYmVrbGJwcGFubXJjd3Vmc2tyYW13c2pwb3p3aHluem94eHZuZXFjZ25hc2NsbWhyemhxeHhuY25vZGR1d3FpcHVsdmdibWJuZ2N2cmtpb3pncGdqbGlodm56bW1ha3N6Ymh4cHNjeG9ianl0dGdzZmZ3bGVjZHNiaWl1a2plYWlsZXFkZXhnZGRsYnNoZnpxb21mcWtxaGVnZ3Jxbm1ld2x6eGFmZGVnZ29uYW1qa29tY3JyZG5qYnFnbGFpYmVmbmJ1Ym5sY3RyZG5zZ2ppeWphYm9yeWZzZWdxbXZ5enVseGFocGdtZ2ZsYXpsZ3Zudnpkb3NpaHJ6Y2RlYnNhcGFldWduY3NxeGlvaXlqb25qbmRtd3h4amRsYWFlZWh6Z3dueWF3aGhtd2pvZHNoZG9ieWtzZGVic3ZrcnBma3NpcGFrY2N0ZWx0bW9mdGJycG1hcGFyYW9idGt1d2hnbW94bGhhY3piZWh0dmt1cW5sbWl0cWNkdHJxbXpkdG9idmxiYWxzYmRubXNrZHZudW1zeWdpZHV3dGJnbG5zZXRvYnl0b3p1dGZiZ3J6YWZvb3RlcXZzeGF1YmtoanFpYWZva3hkb3F0cmZoY3p6cm1ncGNidmJxaGJld215YmNjaXlza2RrbHR3aHV0aXd0eHlqZmlia3lzbG9veXNhcGJ6YW9yZXd4bG1obnFpc3lpd2tkdWdvZGVpeXV0dGxsZWZ4anF5cnNxY3pqZnBrb2phZW11amZhbW1yYnRhY3lrYXprd2FrbmFubWp3ZWtydWpnaGlxaWpkbHJhZXhjempiYmFjbWh1dnJ3dnFkYmppZnl6ZWFjamlyaHVvcWN5emZvaWF0amd1a2tyam9lcHZ1a2NzcWVmYmludHBmdndqb3puZHV1ZGlxYmhocG1maW55c254dG5ya2liYWZvdmlrcHl5aWRiYmlmZ3ZiemZ0YWdjeGVpdWd1dGhsc2t6a2N6d3hkb3l1ZHRxcWhvdWVpdWlnbXBmdnVxeGtxcGxyaHJoYmdhdXVkaHZteG52enFodW1rdWhtYm9yc2p0d2lzb3JiYmVpeG9ra2pkaWRldmZia2JocW9mamJkendsYnRvZ2VyY2Fkc2FtemxuaWJiZWRtcXllY2hzY3pzZHZieHhwYm5vbGdyZWdva25wa29qcnBpaGNza3drd3NkeWVxcXVuenZwcGl4Y3Z4eW95aWJkbGR4Y2xwa25scGllbGhpZ2dydGtpYWplYmJ6ZGVjcmhvZ2hubGZvb3J3YXZuZGV3cnZnb2RudnZrZ2F5dW94b3NnY21sYnFhcnhnZXNxa2ppemxlZXNiZGZlcWV1bnVwb25hdmV1b3hkcmtza3V5dGR6Zm1icXN5aXd5anp0d3h4c2RmdGN1bndvY2pkamhnY3Jsc25lZGRueHloZXNwcHdubWJnY3R2bWZ4eHNvamlsdXp1aWF4amZsaGhta3Rud3BvZXF3cmZ2bm1weXhoaGVhbG1id3FyeHVwbWJuYnVqZG91aXBpdmhxYWpvcHlzd3FzdHpoYnRoamJzdWdrbmVkdG90dXZ0bHBsdHd5b2Fid3lsbmt5amhqdWhjb3R4ampzbmx2bmV3ZWJkbWd6bHV3emVjcm5icWJ1eWljaGRud2t3dm9rYmhldGJlZWxxZ2FjZ2VodmpidWtpeXRjdnF6bWpha2Zzb2VobGd0YXR3ZmlqZXdsdmxlZGl0enJlbGlmamNrZmt4Z3NyeXpzZ2loeGF1aGJkZnZ5eGpnZXlteHhuc2RxZXptZ3B4dnV5eXh3dGl0bW9ldmVjbGpueHlpaHhzbmZzdHF0amV0emlmY213Y2d4b2Z4em14a3pxbWZucnNzemRmeHltcWNoZW9lcWd0emxrbnl1Y2pyZHV2b2NnbnhkZ3NvdWJ4cmJqdXVqdnVseGRhYnBpZXp4aWNvZHByaGptaXlwaGhsbmVsYWtmbmtkb2Rua2pkaG1samJkcnhpZXZvdmVma3Vvc3FncnJmdXVjdHhsdW9vc2FmY2JzanZsd3ZqbWl0bmVrcGVkaWp1cmN5YWNmcGVmemFjYnRibW5rcWV1ZHRndnhienVoeXJjZmRyYnlkbG9saHFrbnR2aWZ2dXZpbnlzZHBydXBxemh1ZmNzbnJycGRoenprZXFodHp4ZXViZnVhanp6a3F6eGdtaHlreXJtaXJnc3N0c3h1b3RyaWx6ZGpxbnZ6cmhvaWh6dXJ4aW9va2x0em9heWN3dW1lc3ZnaHg=", 1071401621)`
	event := helper.DML2Event(insertDML, "uds2", "Data20")
	event.PreColumns = preColumn

	p := NewIndexValueDispatcher("")
	origin, _, err := p.DispatchRowChangedEvent(event, 4)
	require.NoError(t, err)
	require.Equal(t, origin, int32(2))

	ctx := context.Background()
	codecConfig := common.NewConfig(config.ProtocolSimple)
	codecConfig.EncodingFormat = common.EncodingFormatAvro

	builder, err := simple.NewBuilder(ctx, codecConfig)
	require.NoError(t, err)
	encoder := builder.Build()

	decoder, err := simple.NewDecoder(ctx, codecConfig, nil)
	require.NoError(t, err)

	m, err := encoder.EncodeDDLEvent(createTableDDLEvent)
	require.NoError(t, err)

	err = decoder.AddKeyValue(m.Key, m.Value)
	require.NoError(t, err)

	ty, hasNext, err := decoder.HasNext()
	require.NoError(t, err)
	require.True(t, hasNext)
	require.Equal(t, ty, model.MessageTypeDDL)

	decodedDDL, err := decoder.NextDDLEvent()
	require.NoError(t, err)

	originFlags := createTableDDLEvent.TableInfo.ColumnsFlag
	obtainedFlags := decodedDDL.TableInfo.ColumnsFlag

	for colID, expected := range originFlags {
		name := createTableDDLEvent.TableInfo.ForceGetColumnName(colID)
		actualID := decodedDDL.TableInfo.ForceGetColumnIDByName(name)
		actual := obtainedFlags[actualID]
		require.Equal(t, expected, actual)
	}

	err = encoder.AppendRowChangedEvent(ctx, "", event, func() {})
	require.NoError(t, err)

	m = encoder.Build()[0]

	err = decoder.AddKeyValue(m.Key, m.Value)
	require.NoError(t, err)

	ty, hasNext, err = decoder.HasNext()
	require.NoError(t, err)
	require.True(t, hasNext)
	require.Equal(t, ty, model.MessageTypeRow)

	decodedEvent, err := decoder.NextRowChangedEvent()
	require.NoError(t, err)

	obtained, _, err := p.DispatchRowChangedEvent(decodedEvent, 4)
	require.NoError(t, err)
	require.Equal(t, origin, obtained)
}
