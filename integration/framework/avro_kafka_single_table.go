package framework

import (
	"bytes"
	"database/sql"
	"fmt"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"io/ioutil"
	"net/http"
)

type AvroSingleTableTask struct {
	TableName string
}

func (a *AvroSingleTableTask) Name() string {
	log.Warn("AvroSingleTableTask should be embedded in another Task")
	return "AvroSingleTableTask-" + a.TableName
}

func (a *AvroSingleTableTask) GetCDCProfile() *CDCProfile {
	return &CDCProfile{
		PDUri:   "http://upstream-pd:2379",
		SinkUri: "kafka://kafka:9092/testdb_" + a.TableName + "?protocol=avro",
		Opts:    map[string]string{"registry": "http://schema-registry:8081"},
	}
}

func (a *AvroSingleTableTask) Prepare(taskContext *TaskContext) error {
	err := taskContext.CreateDB("testdb")
	if err != nil {
		return err
	}

	_ = taskContext.Upstream.Close()
	taskContext.Upstream, err = sql.Open("mysql", upstreamDSN + "testdb")
	if err != nil {
		return err
	}

	_ = taskContext.Downstream.Close()
	taskContext.Downstream, err = sql.Open("mysql", downstreamDSN + "testdb")
	if err != nil {
		return err
	}

	// TODO better way to generate JSON
	connectorConfigFmt := `{
	  "name": "jdbc-sink-connector",
	  "config": {
		"connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
		"tasks.max": "1",
		"topics": "testdb_%s",
		"connection.url": "jdbc:mysql://root@downstream-tidb:4000/testdb",
		"connection.ds.pool.size": 5,
		"table.name.format": "%s",
		"auto.create": true,
		"auto.evolve": true
	  }
	}`
	connectorConfig := fmt.Sprintf(connectorConfigFmt, a.TableName, a.TableName)
	log.Debug("Creating Kafka sink connector", zap.String("config", connectorConfig))

	resp, err := http.Post(
		"http://127.0.0.1:8083/connectors",
		"application/json",
		bytes.NewReader([]byte(connectorConfig)))
	if err != nil {
		return err
	}

	if resp.Body == nil {
		return errors.New("Kafka Connect Rest API returned empty body")
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		str, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		log.Warn(
			"Kafka Connect Rest API returned",
			zap.Int("status", resp.StatusCode),
			zap.ByteString("body", str))
		return errors.Errorf("Kafka Connect Rest API returned status code %d", resp.StatusCode)
	}

	if taskContext.waitForReady != nil {
		log.Info("Waiting for env to be ready")
		return taskContext.waitForReady()
	}

	return nil
}

func (a *AvroSingleTableTask) Run(taskContext *TaskContext) error {
	log.Warn("AvroSingleTableTask has been run")
	return nil
}
