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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package master

import (
	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/openapi"
	"github.com/pingcap/tiflow/dm/pkg/terror"
)

// redactTaskForResponse only changes the response copy. Internal callers such as
// deleteTask and task template import still need the original credentials.
func redactTaskForResponse(task openapi.Task) openapi.Task {
	task.TargetConfig.Password = config.ObfuscatedPasswordForFeedback
	task.TargetConfig.Security = redactTaskSecurity(task.TargetConfig.Security)
	if task.SourceConfig.FullMigrateConf != nil {
		full := *task.SourceConfig.FullMigrateConf
		full.Security = redactTaskSecurity(full.Security)
		task.SourceConfig.FullMigrateConf = &full
	}
	return task
}

func redactTaskSecurity(security *openapi.Security) *openapi.Security {
	if security == nil {
		return nil
	}
	return &openapi.Security{CertAllowedCn: security.CertAllowedCn}
}

// Do not send stored secrets to a different endpoint during update prechecks.
// Changing connection parameters requires resubmitting the credentials instead
// of copying the placeholders from a GET response.
func validateTaskCredentialReuse(task, stored openapi.Task) error {
	target, oldTarget := task.TargetConfig, stored.TargetConfig
	if target.Password == config.ObfuscatedPasswordForFeedback || taskSecurityNeedsRestore(target.Security, oldTarget.Security) {
		if target.Host != oldTarget.Host || target.Port != oldTarget.Port || target.User != oldTarget.User {
			return terror.ErrOpenAPICommonError.Generate("provide credentials explicitly when changing target connection parameters")
		}
	}
	full, oldFull := task.SourceConfig.FullMigrateConf, stored.SourceConfig.FullMigrateConf
	if full != nil && oldFull != nil && taskSecurityNeedsRestore(full.Security, oldFull.Security) {
		pdAddrChanged := (full.PdAddr == nil) != (oldFull.PdAddr == nil) ||
			(full.PdAddr != nil && oldFull.PdAddr != nil && *full.PdAddr != *oldFull.PdAddr)
		if pdAddrChanged {
			return terror.ErrOpenAPICommonError.Generate("provide TLS content explicitly when changing the PD address")
		}
	}
	return nil
}

func taskSecurityNeedsRestore(security, stored *openapi.Security) bool {
	return security != nil && stored != nil &&
		((security.SslCaContent == "" && stored.SslCaContent != "") ||
			(security.SslCertContent == "" && stored.SslCertContent != "") ||
			(security.SslKeyContent == "" && stored.SslKeyContent != ""))
}

// restoreTaskCredentials supports editing a redacted GET response. The password
// placeholder and empty TLS content mean unchanged; a nil security object still
// means disabling TLS. Explicit nonempty credentials are left for normal update
// validation. Do not modify objects shared with the request or stored task.
func restoreTaskCredentials(task, stored openapi.Task) openapi.Task {
	if task.TargetConfig.Password == config.ObfuscatedPasswordForFeedback {
		task.TargetConfig.Password = stored.TargetConfig.Password
	}
	task.TargetConfig.Security = restoreTaskSecurity(task.TargetConfig.Security, stored.TargetConfig.Security)
	if task.SourceConfig.FullMigrateConf != nil && stored.SourceConfig.FullMigrateConf != nil {
		full := *task.SourceConfig.FullMigrateConf
		full.Security = restoreTaskSecurity(full.Security, stored.SourceConfig.FullMigrateConf.Security)
		task.SourceConfig.FullMigrateConf = &full
	}
	return task
}

func restoreTaskSecurity(security, stored *openapi.Security) *openapi.Security {
	if security == nil || stored == nil {
		return security
	}
	result := *security
	if result.SslCaContent == "" {
		result.SslCaContent = stored.SslCaContent
	}
	if result.SslCertContent == "" {
		result.SslCertContent = stored.SslCertContent
	}
	if result.SslKeyContent == "" {
		result.SslKeyContent = stored.SslKeyContent
	}
	return &result
}
