package gocqlmem

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMapScanCAS(t *testing.T) {
	s := NewSession()
	assert.Nil(t, s.Query("CREATE KEYSPACE ks1").Exec())
	assert.Nil(t, s.Query("CREATE TABLE ks1.t1 (a int, b int, primary key (a))").Exec())

	dest := map[string]interface{}{}
	var isApplied bool
	var err error
	isApplied, err = s.Query("INSERT INTO ks1.t1 (a,b) VALUES (1,1)").MapScanCAS(dest)
	assert.Nil(t, err)
	assert.True(t, isApplied)
	isApplied, err = s.Query(`UPDATE ks1.t1 SET b=2 WHERE a=2 IF EXISTS`).MapScanCAS(dest)
	assert.Nil(t, err)
	assert.False(t, isApplied)
	isApplied, err = s.Query(`UPDATE ks1.t1 SET b=2 WHERE a=1 IF EXISTS`).MapScanCAS(dest)
	assert.Nil(t, err)
	assert.True(t, isApplied)

	result := []map[string]interface{}{}
	result, err = s.Query(`SELECT a,b FROM ks1.t1 WHERE a=1`).Iter().SliceMap()
	assert.Nil(t, err)
	assert.Equal(t, 1, len(result))
	assert.Equal(t, int64(1), result[0]["a"])
	assert.Equal(t, int64(2), result[0]["b"])
}

func TestPageSize(t *testing.T) {
	s := NewSession()
	assert.Nil(t, s.Query("CREATE KEYSPACE ks1").Exec())
	assert.Nil(t, s.Query("CREATE TABLE ks1.t1 (a int, b int, primary key (a))").Exec())

	dest := map[string]interface{}{}
	var isApplied bool
	var err error
	isApplied, err = s.Query("INSERT INTO ks1.t1 (a,b) VALUES (1,1)").MapScanCAS(dest)
	assert.Nil(t, err)
	assert.True(t, isApplied)
	isApplied, err = s.Query("INSERT INTO ks1.t1 (a,b) VALUES (2,2)").MapScanCAS(dest)
	assert.Nil(t, err)
	assert.True(t, isApplied)

	resultA := int64(0)
	resultB := int64(0)

	iter := s.Query(`SELECT a,b FROM ks1.t1`).PageSize(1).PageState([]byte{}).Iter()
	assert.Nil(t, err)
	nextPageState := iter.PageState()
	scanner := iter.Scanner()
	for scanner.Next() {
		err = scanner.Scan(&resultA, &resultB)
		assert.Nil(t, err)
		assert.Equal(t, int64(1), resultA)
		assert.Equal(t, int64(1), resultB)
	}

	iter = s.Query(`SELECT a,b FROM ks1.t1`).PageSize(1).PageState(nextPageState).Iter()
	assert.Nil(t, err)
	scanner = iter.Scanner()
	for scanner.Next() {
		err = scanner.Scan(&resultA, &resultB)
		assert.Nil(t, err)
		assert.Equal(t, int64(2), resultA)
		assert.Equal(t, int64(2), resultB)
	}
}
