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

package migrate

import "context"

// NoOpMigrator do nothing
type NoOpMigrator struct{}

// ShouldMigrate checks if we need to migrate metadata
func (f *NoOpMigrator) ShouldMigrate(ctx context.Context) (bool, error) {
	return false, nil
}

// Migrate migrates the cdc metadata
func (f *NoOpMigrator) Migrate(ctx context.Context) error {
	return nil
}

// WaitMetaVersionMatched wait util migration is done
func (f *NoOpMigrator) WaitMetaVersionMatched(ctx context.Context) error {
	return nil
}

// MarkMigrateDone marks migration is done
func (f *NoOpMigrator) MarkMigrateDone() {
}

// IsMigrateDone check if migration is done
func (f *NoOpMigrator) IsMigrateDone() bool {
	return true
}
