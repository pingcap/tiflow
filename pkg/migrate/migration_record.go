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

import (
	"context"
	"fmt"
)

type migration struct {
	migrate func(context.Context, *migrator) error
	version int
}

// byVersion implements sort.Interface based on the version field.
type byVersion []*migration

func (a byVersion) Len() int           { return len(a) }
func (a byVersion) Less(i, j int) bool { return a[i].version < a[j].version }
func (a byVersion) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

var migrationRecords []*migration

func addMigrateRecord(version int, m func(ctx context.Context, m *migrator) error) bool {
	checkDuplicateMigrationVersion(version, migrationRecords)
	migrationRecords = append(migrationRecords, &migration{
		version: version,
		migrate: m,
	})
	return true
}

func checkDuplicateMigrationVersion(version int, records []*migration) {
	for _, item := range records {
		if item.version == version {
			panic(fmt.Sprintf("migration version is already added: %d", version))
		}
	}
}
