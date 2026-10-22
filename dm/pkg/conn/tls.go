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

package conn

import (
	"crypto/tls"
	"slices"
)

// MySQLTLSCipherSuites keeps legacy MySQL servers with RSA-only TLS cipher
// suites reachable after Go 1.27 removed the tlsrsakex GODEBUG setting.
// Modern cipher suites remain enabled and are preferred by crypto/tls.
func MySQLTLSCipherSuites() []uint16 {
	suites := make([]uint16, 0, len(tls.CipherSuites())+4)
	for _, suite := range tls.CipherSuites() {
		if slices.Contains(suite.SupportedVersions, tls.VersionTLS12) {
			suites = append(suites, suite.ID)
		}
	}
	return append(suites,
		tls.TLS_RSA_WITH_AES_128_GCM_SHA256,
		tls.TLS_RSA_WITH_AES_256_GCM_SHA384,
		tls.TLS_RSA_WITH_AES_128_CBC_SHA,
		tls.TLS_RSA_WITH_AES_256_CBC_SHA,
	)
}
