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

package sqlgen

import (
	"github.com/pingcap/errors"
)

var (
	// ErrUKColValueNotProvided means that some column values of the unique key are not provided.
	ErrUKColValueNotProvided = errors.New("some UK column values are not provided")
	// ErrMissingUKValue means the input unique key is nil.
	ErrMissingUKValue = errors.New("missing the UK values")
	// ErrWhereConditionsEmpty means the WHERE clause compare conditions is empty.
	// It is usually caused when there is no filter clause generated on generating a WHERE clause.
	ErrWhereFiltersEmpty = errors.New("`WHERE` condition filters is empty")
)
