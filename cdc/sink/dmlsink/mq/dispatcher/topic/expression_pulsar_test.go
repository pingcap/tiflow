package topic

import (
	"strings"
	"testing"
)

var testPulsarTopicList = []string{
	//failed
	"public", // no {schema}
	"__",
	"_xyz",
	"123456",
	"ABCD",
	"persistent:public_test-table",
	"persistent://",
	"persistent://public",
	"persistent://public_test-table",
	"persistent://public/_test-table",
	// if the {schema} is a not exist namespace in pulsar server, pulsar client will get an error,
	// but cdc can not check it
	"persistent://public/{schema}/_test-table",
	"persistent_public/test_{schema}_{table}", //simple name must not contain '/'
	"persistent://{schema}_{table}",
	"persistent://{schema}/{table}",
	"persistent://{schema}/{table}/test/name", // must format xxx/yyy/zzz
	"non-persistent://public/test_{schema}_{table}",
	"non-persistent://public/test_{schema}_{table}_123456aaaa",
	"non-persistent://test_{schema}_{table}_123456aaaa",

	//pass
	"{schema}",
	"AZ_{schema}",
	"{table}_{schema}",
	"{schema}_{table}",
	"123_{schema}_non-persistenttest__{table})",
	"persistent_public_test_{schema}_{table}",
	"persistent{schema}_{table}",
	"persistent://public/default/{schema}_{table}",
	"persistent://public/default/2342-{schema}_abc234",
	"persistent://{schema}/{schema}/2342-{schema}_abc234",
	"persistent://{schema}/dev/2342-{schema}_abc234", // {schema} must exist in pulsar server
	"non-persistent://public/default/test_{schema}_{table}_back_up",
	"123_{schema}_non-persistenttest__{table}",
}

func TestPulsarMatchTable(t *testing.T) {

	for _, topicName := range testPulsarTopicList {
		if pulsarTopicNameRE.MatchString(topicName) &&
			strings.Contains(topicName, "{schema}") && strings.Contains(topicName, "{table}") {
			t.Logf("MatchString(%s) , passed ok！！！\n", topicName)
		} else {
			t.Errorf("MatchString(%s) failed X", topicName)
		}
	}
}

func TestPulsarValidate(t *testing.T) {

	for _, topicName := range testPulsarTopicList {

		if err := Expression(topicName).PulsarValidate(); err == nil {
			t.Logf("MatchString(%s) , passed ok！！！\n", topicName)
		} else {
			t.Errorf("MatchString(%s) failed XXX ", topicName)
		}
	}
}
