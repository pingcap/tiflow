// Copyright 2026 PingCAP, Inc.
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

package framework

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

// ResolveRepoPath returns an absolute path under the tiflow repository
// without relying on git metadata. This keeps repo-local test assets
// discoverable from symlinked checkouts and git worktrees.
func ResolveRepoPath(rel string) (string, error) {
	normalizedRel := filepath.FromSlash(strings.TrimPrefix(rel, "/"))
	for _, base := range repoPathCandidates() {
		dir := base
		for dir != "." && dir != string(filepath.Separator) {
			if isRepoRoot(dir) {
				return filepath.Join(dir, normalizedRel), nil
			}
			parent := filepath.Dir(dir)
			if parent == dir {
				break
			}
			dir = parent
		}
	}

	return "", fmt.Errorf("cannot resolve repo path for %q", rel)
}

func repoPathCandidates() []string {
	var candidates []string
	if _, file, _, ok := runtime.Caller(0); ok {
		candidates = append(candidates, filepath.Dir(file))
	}
	if wd, err := os.Getwd(); err == nil {
		candidates = append(candidates, wd)
	}
	return candidates
}

func isRepoRoot(dir string) bool {
	for _, marker := range []string{
		filepath.Join(dir, "go.mod"),
		filepath.Join(dir, "deployments", "ticdc", "docker-compose"),
		filepath.Join(dir, "tests", "mq_protocol_tests", "framework"),
	} {
		if _, err := os.Stat(marker); err != nil {
			return false
		}
	}
	return true
}
