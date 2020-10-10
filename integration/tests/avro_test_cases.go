// Copyright 2020 PingCAP, Inc.
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

package tests

import (
	"github.com/pingcap/ticdc/integration/framework/avro"
)

//nolint:unused
type avroAlterCase struct {
	AlterCase
}

// NewAvroAlterCase construct alter case for avro
func NewAvroAlterCase() *avroAlterCase {
	return &avroAlterCase{
		AlterCase: NewAlterCase(&avro.SingleTableTask{TableName: "test"}),
	}
}

//nolint:unused
type avroCompositePKeyCase struct {
	CompositePKeyCase
}

// NewAvroCompositePKeyCase construct composite primary key case for avro
func NewAvroCompositePKeyCase() *avroCompositePKeyCase {
	return &avroCompositePKeyCase{
		CompositePKeyCase: NewCompositePKeyCase(&avro.SingleTableTask{TableName: "test"}),
	}
}

//nolint:unused
type avroDeleteCase struct {
	DeleteCase
}

// NewAvroDeleteCase construct delete case for avro
func NewAvroDeleteCase() *avroDeleteCase {
	return &avroDeleteCase{
		DeleteCase: NewDeleteCase(&avro.SingleTableTask{TableName: "test"}),
	}
}

//nolint:unused
type avroManyTypesCase struct {
	ManyTypesCase
}

// NewAvroManyTypesCase construct many types case for avro
func NewAvroManyTypesCase() *avroManyTypesCase {
	return &avroManyTypesCase{
		ManyTypesCase: NewManyTypesCase(&avro.SingleTableTask{TableName: "test"}),
	}
}

//nolint:unused
type avroSimpleCase struct {
	SimpleCase
}

// NewAvroSimpleCase construct simple case for avro
func NewAvroSimpleCase() *avroSimpleCase {
	return &avroSimpleCase{
		SimpleCase: NewSimpleCase(&avro.SingleTableTask{TableName: "test"}),
	}
}

//nolint:unused
type avroUnsignedCase struct {
	UnsignedCase
}

// NewAvroUnsignedCase construct unsigned case for avro
func NewAvroUnsignedCase() *avroUnsignedCase {
	return &avroUnsignedCase{
		UnsignedCase: NewUnsignedCase(&avro.SingleTableTask{TableName: "test"}),
	}
}
