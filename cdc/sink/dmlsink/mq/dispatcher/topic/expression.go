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

package topic

import (
	"regexp"
	"strings"

	"github.com/pingcap/tiflow/pkg/errors"
)

var (
	// hardCodeTopicNameRe is used to match a topic name which is hard code in the config
	hardCodeTopicNameRe = regexp.MustCompile(`^([A-Za-z0-9\._\-]+)$`)

	// topicNameRE is used to match a valid topic expression
	topicNameRE = regexp.MustCompile(
		`^[A-Za-z0-9\._\-]*(\{schema\})?([A-Za-z0-9\._\-]*\{table\})?[A-Za-z0-9\._\-]*$`,
	)
	// kafkaForbidRE is used to reject the characters which are forbidden in kafka topic name
	kafkaForbidRE = regexp.MustCompile(`[^a-zA-Z0-9\._\-]`)
	// schemaRE is used to match substring '{schema}' in topic expression
	schemaRE = regexp.MustCompile(`\{schema\}`)
	// tableRE is used to match substring '{table}' in topic expression
	tableRE = regexp.MustCompile(`\{table\}`)
	// avro has different topic name pattern requirements, '{schema}' and '{table}' placeholders
	// are necessary
	avroTopicNameRE = regexp.MustCompile(
		`^[A-Za-z0-9\._\-]*\{schema\}[A-Za-z0-9\._\-]*\{table\}[A-Za-z0-9\._\-]*$`,
	)
	// pulsarTopicNameRE is used to match pulsar topic
	pulsarTopicNameRE = regexp.MustCompile(
		`(^((persistent|non-persistent)://)[A-Za-z0-9{}._\-]*/[A-Za-z0-9{}._\-]*/[A-Za-z0-9{}._\-]*$)|` +
			`(^[A-Za-z0-9._-]*\{schema}[A-Za-z0-9._-]*\{table}[A-Za-z0-9._-]*)$`,
	)
	// pulsarTopicNameREFull is used to match pulsar full topic name
	pulsarTopicNameREFull = regexp.MustCompile(
		`(?:persistent|non-persistent)://.*`,
	)
)

// The max length of kafka topic name is 249.
// See https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/internals/Topic.java#L35
const kafkaTopicNameMaxLength = 249

// Expression represent a kafka topic expression.
// The expression should be in form of: [prefix]{schema}[middle][{table}][suffix]
// prefix/suffix/middle are optional and should match the regex of [A-Za-z0-9\._\-]*
// {table} can also be optional.
type Expression string

// Validate checks whether a kafka topic name is valid or not.
// return true if the expression is hard coded.
func (e Expression) Validate() error {
	// validate the topic expression
	if ok := topicNameRE.MatchString(string(e)); ok {
		return nil
	}

	return errors.ErrKafkaInvalidTopicExpression.GenWithStackByArgs(e)
}

// ValidateForAvro checks whether topic pattern is {schema}_{table}, the only allowed
func (e Expression) ValidateForAvro() error {
	if ok := avroTopicNameRE.MatchString(string(e)); !ok {
		return errors.ErrKafkaInvalidTopicExpression.GenWithStackByArgs(e,
			"topic rule for Avro must contain {schema} and {table}",
		)
	}

	return nil
}

// Substitute converts schema/table name in a topic expression to kafka topic name.
// When doing conversion, the special characters other than [A-Za-z0-9\._\-] in schema/table
// will be substituted for underscore '_'.
func (e Expression) Substitute(schema, table string) string {
	// some of the special characters will be replaced with '_'
	replacedSchema := kafkaForbidRE.ReplaceAllString(schema, "_")
	replacedTable := kafkaForbidRE.ReplaceAllString(table, "_")

	topicExpr := string(e)
	// doing the real conversion things
	topicName := schemaRE.ReplaceAllString(topicExpr, replacedSchema)
	topicName = tableRE.ReplaceAllString(topicName, replacedTable)

	// topicName will be truncated if it exceed the limit.
	// And topicName '.' and '..' are also invalid, replace them with '_'.
	//    See https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/internals/Topic.java#L46
	if len(topicName) > kafkaTopicNameMaxLength {
		return topicName[:kafkaTopicNameMaxLength]
	} else if topicName == "." {
		return "_"
	} else if topicName == ".." {
		return "__"
	}
	return topicName
}

// PulsarValidate checks whether a pulsar topic name is valid or not.
func (e Expression) PulsarValidate() error {
	// validate the topic expression
	topicName := string(e)

	if len(topicName) == 0 {
		return errors.ErrPulsarInvalidTopicExpression.GenWithStackByArgs(
			"topic name is empty")
	}

	// if not full name, must be simple name
	if !pulsarTopicNameREFull.MatchString(topicName) {
		if strings.Contains(topicName, "/") {
			return errors.ErrPulsarInvalidTopicExpression.GenWithStackByArgs(
				"it should be in the format of a <topic> and topic name must contain '{schema}'" +
					"and simple topic name must not contain '/'")
		}
	} else if !pulsarTopicNameRE.MatchString(topicName) {
		return errors.ErrPulsarInvalidTopicExpression.GenWithStackByArgs(
			"it should be in the format of <tenant>/<namespace>/<topic> or <topic> " +
				"and topic name must contain '{schema}'")
	}

	return nil
}

// IsHardCode checks whether a topic name is hard code or not.
func IsHardCode(topicName string) bool {
	return hardCodeTopicNameRe.MatchString(topicName)
}
