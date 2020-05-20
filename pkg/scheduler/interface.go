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

package scheduler

import "github.com/pingcap/ticdc/cdc/model"

type Scheduler interface {
	ResetWorkloads(captureID model.CaptureID, workloads map[model.TableID]uint64)
	AlignCapture(captureIDs map[model.CaptureID]struct{})
	Skewness() float64
	CalRebalanceOperates(targetSkewness float64, boundaryTs model.Ts) (
		skewness float64,
		deleteOperations map[model.CaptureID]map[model.TableID]*model.TableOperation,
		addOperations map[model.CaptureID]map[model.TableID]*model.TableOperation)
	DistributeTables(tableIDs map[model.TableID]model.Ts) map[model.CaptureID]map[model.TableID]*model.TableOperation
}
