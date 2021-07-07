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

package security

// SaslScram holds necessary path parameter to support sasl-scram
type SaslScram struct {
	SaslUser      string `toml:"sasl-user" json:"sasl-user"`
	SaslPassword  string `toml:"sasl-password" json:"sasl-password"`
	SaslMechanism string `toml:"sasl-mechanism" json:"sasl-mechanism"`
}

// IsSaslScramEnabled checks whether SASL SCRAM is enabled or not.
func (s *SaslScram) IsSaslScramEnabled() bool {
	return len(s.SaslUser) != 0
}
