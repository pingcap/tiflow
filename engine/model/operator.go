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

package model

type OperatorType int32

type Operator []byte

const (
	TableReaderType OperatorType = iota
	HashType
	TableSinkType
	ProducerType
	BinlogType
	// Task of job master.
	JobMasterType
)

// benchmark operators
type TableReaderOp struct {
	FlowID string `json:"flow-id"`
	Addr   string `json:"address"`
}

type HashOp struct {
	TableID int32 `json:"id"`
}

type TableSinkOp struct {
	TableID int32  `json:"id"`
	File    string `json:"file"`
}

type ProducerOp struct {
	TableID      int32 `json:"tbl-num"`
	RecordCnt    int32 `json:"rcd-cnt"`
	DDLFrequency int32 `json:"ddl-freq"`
	OutputCnt    int   `json:"output-cnt"`
}

type BinlogOp struct {
	Address string `json:"addr"`
}
