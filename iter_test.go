package gocqlmem

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestColumnsAndRowData(t *testing.T) {
	s := NewSession()
	assert.Nil(t, s.Query("CREATE KEYSPACE ks1").Exec())
	assert.Nil(t, s.Query("CREATE TABLE ks1.t1 (f_int int, f_text text, f_bool boolean, f_float float, primary key (f_int))").Exec())

	dest := map[string]interface{}{}
	var isApplied bool
	var err error
	isApplied, err = s.Query("INSERT INTO ks1.t1 (f_int, f_text, f_bool, f_float) VALUES (1,'1', TRUE, 1.1)").MapScanCAS(dest)
	assert.Nil(t, err)
	assert.True(t, isApplied)

	iter := s.Query(`SELECT f_int, f_text, f_bool, f_float FROM ks1.t1`).Iter()
	assert.Nil(t, iter.err)
	cols := iter.Columns()

	assert.Equal(t, "ks1", cols[0].Keyspace)
	assert.Equal(t, "t1", cols[0].Table)
	assert.Equal(t, "f_int", cols[0].Name)
	assert.Equal(t, TypeInt, cols[0].TypeInfo.Type())

	assert.Equal(t, "ks1", cols[1].Keyspace)
	assert.Equal(t, "t1", cols[1].Table)
	assert.Equal(t, "f_text", cols[1].Name)
	assert.Equal(t, TypeText, cols[1].TypeInfo.Type())

	assert.Equal(t, "ks1", cols[2].Keyspace)
	assert.Equal(t, "t1", cols[2].Table)
	assert.Equal(t, "f_bool", cols[2].Name)
	assert.Equal(t, TypeBoolean, cols[2].TypeInfo.Type())

	assert.Equal(t, "ks1", cols[3].Keyspace)
	assert.Equal(t, "t1", cols[3].Table)
	assert.Equal(t, "f_float", cols[3].Name)
	assert.Equal(t, TypeFloat, cols[3].TypeInfo.Type())

	rowData, err := iter.RowData()
	assert.Nil(t, err)
	assert.Equal(t, 4, len(rowData.Columns))
	assert.Equal(t, int64(0), rowData.Values[0])
	assert.Equal(t, "", rowData.Values[1])
	assert.Equal(t, false, rowData.Values[2])
	assert.Equal(t, float64(0.0), rowData.Values[3])
}

func TestScanner(t *testing.T) {
	s := NewSession()
	assert.Nil(t, s.Query("CREATE KEYSPACE ks1").Exec())
	assert.Nil(t, s.Query("CREATE TABLE ks1.t1 (f_int int, f_text text, f_bool boolean, f_float float, primary key (f_int))").Exec())

	dest := map[string]interface{}{}
	var isApplied bool
	var err error
	isApplied, err = s.Query("INSERT INTO ks1.t1 (f_int, f_text, f_bool, f_float) VALUES (1,'1', TRUE, 1.1)").MapScanCAS(dest)
	assert.Nil(t, err)
	assert.True(t, isApplied)

	resultInt := int64(0)
	resultText := ""
	resultBool := false
	resultFloat := float32(0.0)

	iter := s.Query(`SELECT f_int, f_text, f_bool, f_float FROM ks1.t1`).Iter()
	assert.Nil(t, err)
	scanner := iter.Scanner()
	for scanner.Next() {
		err = scanner.Scan(&resultInt, &resultText, &resultBool, &resultFloat)
		assert.Nil(t, err)
		assert.Equal(t, int64(1), resultInt)
		assert.Equal(t, "1", resultText)
		assert.Equal(t, true, resultBool)
		assert.Equal(t, float32(1.1), resultFloat)
	}
}
