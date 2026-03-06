package gocqlmem

import (
	"errors"
	"fmt"
)

type Iter struct {
	err     error
	pos     int
	meta    resultMetadata
	numRows int
	next    *nextIter
	//host    *HostInfo

	//framer *framer
	closed int32

	//RetrievedTypes  []CqlDataTypeType
	RetrievedNames  []string
	RetrievedValues [][]any
	//LastSelectedRowIdx int
}

func (iter *Iter) Columns() []ColumnInfo {
	return iter.meta.columns
}

// func (iter *Iter) readColumn() ([]byte, error) {
// 	return iter.framer.readBytesInternal()
// }

func (iter *Iter) RowData() (RowData, error) {
	if iter.err != nil {
		return RowData{}, iter.err
	}

	rowData := RowData{
		Columns: iter.RetrievedNames,
		Values:  make([]interface{}, len(iter.RetrievedNames)),
	}

	return rowData, nil
}

func (iter *Iter) SliceMap() ([]map[string]interface{}, error) {
	if iter.err != nil {
		return nil, iter.err
	}

	// Not checking for the error because we just did
	rowData, _ := iter.RowData()
	dataToReturn := make([]map[string]interface{}, 0)
	for iter.Scan(rowData.Values...) {
		m := make(map[string]interface{}, len(rowData.Columns))
		for i, column := range rowData.Columns {
			m[column] = rowData.Values[i]
		}
		dataToReturn = append(dataToReturn, m)
	}
	if iter.err != nil {
		return nil, iter.err
	}
	return dataToReturn, nil
}

func (iter *Iter) Close() error {
	return iter.err
}

func (iter *Iter) Scanner() Scanner {
	if iter == nil {
		return nil
	}

	return &iterScanner{iter: iter, cols: make([]interface{}, len(iter.RetrievedNames))}
}

func (iter *Iter) checkErrAndNotFound() error {
	if iter.err != nil {
		return iter.err
		//} else if iter.numRows == 0 {
	} else if len(iter.RetrievedValues) == 0 {
		return errors.New("not found")
	}
	return nil
}

func (iter *Iter) Scan(dest ...interface{}) bool {
	if iter.err != nil || iter.pos >= len(iter.RetrievedValues) {
		return false
	}

	if len(dest) != len(iter.RetrievedNames) {
		iter.err = fmt.Errorf("gocqlmem: not enough columns to scan into: have %d want %d", len(dest), len(iter.RetrievedNames))
		return false
	}

	for i := range len(iter.RetrievedNames) {
		dest[i] = iter.RetrievedValues[iter.pos][i]
	}

	iter.pos++
	return true

	// if iter.err != nil {
	// 	return false
	// }

	// if iter.pos >= iter.numRows {
	// 	if iter.next != nil {
	// 		*iter = *iter.next.fetch()
	// 		return iter.Scan(dest...)
	// 	}
	// 	return false
	// }

	// if iter.next != nil && iter.pos >= iter.next.pos {
	// 	iter.next.fetchAsync()
	// }

	// // currently only support scanning into an expand tuple, such that its the same
	// // as scanning in more values from a single column
	// if len(dest) != iter.meta.actualColCount {
	// 	iter.err = fmt.Errorf("gocql: not enough columns to scan into: have %d want %d", len(dest), iter.meta.actualColCount)
	// 	return false
	// }

	// // i is the current position in dest, could posible replace it and just use
	// // slices of dest
	// i := 0
	// for _, col := range iter.meta.columns {
	// 	colBytes, err := iter.readColumn()
	// 	if err != nil {
	// 		iter.err = err
	// 		return false
	// 	}

	// 	n, err := scanColumn(colBytes, col, dest[i:])
	// 	if err != nil {
	// 		iter.err = err
	// 		return false
	// 	}
	// 	i += n
	// }

	// iter.pos++
	// return true
}

func (iter *Iter) MapScan(m map[string]interface{}) bool {
	if iter.err != nil {
		return false
	}

	rowDataValues := make([]any, len(iter.RetrievedNames))
	if iter.Scan(rowDataValues...) {
		for i, name := range iter.RetrievedNames {
			m[name] = rowDataValues[i]
		}
		return true
	}
	return false

	// // Not checking for the error because we just did
	// rowData, _ := iter.RowData()

	// for i, col := range rowData.Columns {
	// 	if dest, ok := m[col]; ok {
	// 		rowData.Values[i] = dest
	// 	}
	// }

	// if iter.Scan(rowData.Values...) {
	// 	rowData.rowMap(m)
	// 	return true
	// }
	// return false
}

func (iter *Iter) PageState() []byte {
	return iter.meta.pagingState
}
