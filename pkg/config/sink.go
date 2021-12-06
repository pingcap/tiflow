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

package config

import (
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	cerror "github.com/pingcap/ticdc/pkg/errors"
)

// SinkConfig represents sink config for a changefeed
type SinkConfig struct {
	DispatchRules   []*DispatchRule   `toml:"dispatchers" json:"dispatchers"`
	Protocol        string            `toml:"protocol" json:"protocol"`
	ColumnSelectors []*ColumnSelector `toml:"column-selectors" json:"column-selectors"`
}

// DispatchRule represents partition rule for a table
type DispatchRule struct {
	Matcher    []string `toml:"matcher" json:"matcher"`
	Dispatcher string   `toml:"dispatcher" json:"dispatcher"`
}

type ColumnSelector struct {
	Matcher []string `toml:"matcher" json:"matcher"`
	Columns []string `toml:"columns" json:"columns"`
}

func (s *SinkConfig) validate(enableOldValue bool) error {
	var protocol Protocol
	protocol.FromString(s.Protocol)
	if (protocol == ProtocolCanal ||
		protocol == ProtocolCanalJSON ||
		protocol == ProtocolMaxwell) && !enableOldValue {
		log.Error(fmt.Sprintf("Old value is not enabled when using `%s` protocol. "+
			"Please update changefeed config", s.Protocol))
		return cerror.WrapError(cerror.ErrKafkaInvalidConfig,
			errors.New(fmt.Sprintf("%s protocol requires old value to be enabled", s.Protocol)))
	}

	return nil
}
