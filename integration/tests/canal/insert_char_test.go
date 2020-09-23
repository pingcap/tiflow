package canal

import (
	"database/sql"
	"testing"
	"upper.io/db.v3/lib/sqlbuilder"
)

const dns = "root@tcp(127.0.0.1:3306)/test"


func TestInsertChar(t *testing.T) {
	db_, err := sql.Open("mysql", dns)
	if err != nil {
		t.Fatal(err.Error())
	}
	db, err := sqlbuilder.New("mysql", db_)
	if err != nil {
		t.Fatal(err.Error())
	}
	values := make([]interface{}, 0, 1)
	values = append(values, 'a')
	keys := []string{"t_char"}
	_, err = db.InsertInto("test").Columns(keys...).Values(values...).Exec()
	if err != nil {
		t.Fatal(err.Error())
	}
}
