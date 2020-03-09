package model

import (
	"encoding/base64"
	"encoding/json"
	"log"

	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"go.uber.org/zap"
)

// RowChangedEvent represents a row changed event
type RowChangedEvent struct {
	Ts       uint64
	Resolved bool

	Schema string
	Table  string

	Delete  bool
	Columns map[string]*Column
}

// ToMqMessage transforms to message key and value
func (e *RowChangedEvent) ToMqMessage() (*MqMessageKey, *MqMessageRow) {
	key := &MqMessageKey{
		Ts:     e.Ts,
		Schema: e.Schema,
		Table:  e.Table,
		Type:   MqMessageTypeRow,
	}
	value := &MqMessageRow{}
	if e.Delete {
		value.Delete = e.Columns
	} else {
		value.Update = e.Columns
	}
	return key, value
}

// FromMqMessage fills the values of RowChangedEvent from message key and value
func (e *RowChangedEvent) FromMqMessage(key *MqMessageKey, value *MqMessageRow) {
	e.Ts = key.Ts
	e.Resolved = false
	e.Table = key.Table
	e.Schema = key.Schema

	if len(value.Delete) != 0 {
		e.Delete = true
		e.Columns = value.Delete
	} else {
		e.Delete = false
		e.Columns = value.Update
	}
}

// Column represents a column value in row changed event
type Column struct {
	Type        byte        `json:"type"`
	WhereHandle bool        `json:"where_handle"`
	Value       interface{} `json:"value"`
}

func (c *Column) formatVal() {
	switch c.Type {
	case mysql.TypeVarchar, mysql.TypeString, mysql.TypeVarString,
		mysql.TypeTinyBlob, mysql.TypeMediumBlob,
		mysql.TypeLongBlob, mysql.TypeBlob:
		if s, ok := c.Value.(string); ok {
			var err error
			c.Value, err = base64.StdEncoding.DecodeString(s)
			if err != nil {
				log.Fatal("invalid column value, please report a bug", zap.Any("col", c), zap.Error(err))
			}
		}
	case mysql.TypeBit:
		if s, ok := c.Value.(json.Number); ok {
			intNum, err := s.Int64()
			if err != nil {
				log.Fatal("invalid column value, please report a bug", zap.Any("col", c), zap.Error(err))
			}
			c.Value = uint64(intNum)
		}

	}
}

// DDLEvent represents a DDL event
type DDLEvent struct {
	Ts     uint64
	Schema string
	Table  string
	Query  string
	Type   model.ActionType
}

// ToMqMessage transforms to message key and value
func (e *DDLEvent) ToMqMessage() (*MqMessageKey, *MqMessageDDL) {
	key := &MqMessageKey{
		Ts:     e.Ts,
		Schema: e.Schema,
		Table:  e.Table,
		Type:   MqMessageTypeDDL,
	}
	value := &MqMessageDDL{
		Query: e.Query,
		Type:  e.Type,
	}
	return key, value
}

// FromMqMessage fills the values of DDLEvent from message key and value
func (e *DDLEvent) FromMqMessage(key *MqMessageKey, value *MqMessageDDL) {
	e.Ts = key.Ts
	e.Table = key.Table
	e.Schema = key.Schema
	e.Type = value.Type
	e.Query = value.Query
}
