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

package dispatcher

import (
	"regexp"
	"strings"

	"github.com/pingcap/tiflow/pkg/errors"
)

var (
	// topicNameRE is used to match a valid topic expression
	topicNameRE = regexp.MustCompile(`^[A-Za-z0-9\._\-]*\{schema\}[A-Za-z0-9\._\-]*$|^\{schema\}_\{table\}$`)
	// kafkaForbidRE is used to reject the characters which are forbidden in kafka topic name
	kafkaForbidRE = regexp.MustCompile(`[^a-zA-Z0-9\._\-]`)
	// schemaRE is used to match substring '{schema}' in topic expression
	schemaRE = regexp.MustCompile(`\{schema\}`)
	// tableRE is used to match substring '{table}' in topic expression
	tableRE = regexp.MustCompile(`\{table\}`)
)

const kafkaTopicNameMaxLength = 249

// Parse convert schema/table name to kafka topic name by using a topic expression.
// A valid kafka topic name matches [a-zA-Z0-9\._\-]{1,249},
// and the topic expression only accepts two types of expressions:
// 	1. [prefix]{schema}[suffix], the prefix/suffix is optional and matches [A-Za-z0-9\._\-]*
//  2. {schema}_{table}
//
// When doing conversion, the special characters other than [A-Za-z0-9\._\-] in schema/table
// will be substituted for underscore '_'
func Substitute(topicExpr, schema, table string) (string, error) {
	// validate the topic expression
	if ok := topicNameRE.MatchString(topicExpr); !ok {
		return "", errors.ErrKafkaInvalidTopicExpression.GenWithStackByArgs()
	}

	// the upper case letters in schema/table will be converted to lower case,
	// and some of the special characters will be replaced with '_'
	replacedSchema := kafkaForbidRE.ReplaceAllString(strings.ToLower(schema), "_")
	replacedTable := kafkaForbidRE.ReplaceAllString(strings.ToLower(table), "_")

	// doing the real conversion things
	topicName := schemaRE.ReplaceAllString(topicExpr, replacedSchema)
	topicName = tableRE.ReplaceAllString(topicName, replacedTable)

	// The max length of kafka topic name is 249, so truncate topicName if necessary.
	// 	  See https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/internals/Topic.java#L35
	// And topicName '.' and '..' are also invalid, so replace them with '_'.
	//    See https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/internals/Topic.java#L46
	if len(topicName) > kafkaTopicNameMaxLength {
		return topicName[:kafkaTopicNameMaxLength], nil
	} else if topicName == "." {
		return "_", nil
	} else if topicName == ".." {
		return "__", nil
	} else {
		return topicName, nil
	}
}
