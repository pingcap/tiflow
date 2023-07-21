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

package config

// HasDump returns true if taskMode contains dump unit.
func HasDump(taskMode string) bool {
	switch taskMode {
	case ModeAll, ModeFull, ModeDump:
		return true
	default:
		return false
	}
}

// HasLoad returns true if taskMode contains load unit.
func HasLoad(taskMode string) bool {
	switch taskMode {
	case ModeAll, ModeFull, ModeLoadSync:
		return true
	default:
		return false
	}
}

// HasSync returns true if taskMode contains sync unit.
func HasSync(taskMode string) bool {
	switch taskMode {
	case ModeAll, ModeIncrement, ModeLoadSync:
		return true
	default:
		return false
	}
}
