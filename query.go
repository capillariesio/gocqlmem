package gocqlmem

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

type Query struct {
	stmt                string
	values              []interface{}
	session             *Session
	pageSize            int
	pageState           []byte
	disableAutoPage     bool
	disableSkipMetadata bool
}

func (q *Query) PageSize(n int) *Query {
	q.pageSize = n
	return q
}

func (q *Query) PageState(state []byte) *Query {
	q.pageState = state
	q.disableAutoPage = true
	return q
}

func (q *Query) Iter() *Iter {
	cmds, err := ParseCommands(q.stmt)
	if err != nil {
		return &Iter{err: err}
	}
	if len(cmds) != 1 {
		return &Iter{err: fmt.Errorf("exactly one CQL cmd expected, got: %s", q.stmt)}
	}

	switch cmd := cmds[0].(type) {
	case *CommandCreateKeyspace:
		return &Iter{err: q.session.createKeyspace(cmd)}
	case *CommandUseKeyspace:
		return &Iter{}
	case *CommandDropKeyspace:
		return &Iter{err: q.session.dropKeyspace(cmd)}
	case *CommandCreateTable:
		return &Iter{err: q.session.createTable(cmd)}
	case *CommandTruncateTable:
		return &Iter{err: q.session.truncateTable(cmd)}
	case *CommandDropTable:
		return &Iter{err: q.session.dropTable(cmd)}
	case *CommandInsert:
		isApplied, err := q.session.execInsert(cmd)
		if err != nil {
			return &Iter{err: err}
		}
		return &Iter{
			//RetrievedTypes:  []CqlDataTypeType{CqlDataTypeBool},
			RetrievedNames:  []string{"[applied]"},
			RetrievedValues: [][]any{{isApplied}}}
	case *CommandSelect:
		var lastSelectedRowIdx int32
		if len(q.pageState) == 0 {
			lastSelectedRowIdx = -1
		} else {
			err := binary.Read(bytes.NewReader(q.pageState), binary.LittleEndian, &lastSelectedRowIdx)
			if err != nil {
				return &Iter{err: fmt.Errorf("cannot convert page state %v to int: %s", q.pageState, err.Error())}
			}
		}
		names, values, newLastSelectedRowIdx, err := q.session.execSelect(cmd, int(lastSelectedRowIdx), q.pageSize)
		if err != nil {
			return &Iter{err: err}
		}
		buf := new(bytes.Buffer)
		err = binary.Write(buf, binary.LittleEndian, int32(newLastSelectedRowIdx))
		if err != nil {
			return &Iter{err: fmt.Errorf("cannot convert int %d to byte slice: %s", lastSelectedRowIdx, err.Error())}
		}

		return &Iter{
			//RetrievedTypes:  types,
			meta:            resultMetadata{pagingState: buf.Bytes()},
			RetrievedNames:  names,
			RetrievedValues: values}
	case *CommandUpdate:
		isApplied, err := q.session.execUpdate(cmd)
		if err != nil {
			return &Iter{err: err}
		}
		return &Iter{
			//RetrievedTypes:  []CqlDataTypeType{CqlDataTypeBool},
			RetrievedNames:  []string{"[applied]"},
			RetrievedValues: [][]any{{isApplied}}}

	case *CommandDelete:
		isApplied, err := q.session.execDelete(cmd)
		if err != nil {
			return &Iter{err: err}
		}
		return &Iter{
			//RetrievedTypes:  []CqlDataTypeType{CqlDataTypeBool},
			RetrievedNames:  []string{"[applied]"},
			RetrievedValues: [][]any{{isApplied}}}

	default:
		return &Iter{err: fmt.Errorf("Iter() does not support cmd %v", cmd)}
	}
}

// INSERT, UPDATE, DELETE
func (q *Query) MapScanCAS(dest map[string]interface{}) (applied bool, err error) {
	iter := q.Iter()
	if err := iter.checkErrAndNotFound(); err != nil {
		return false, err
	}
	iter.MapScan(dest)
	applied = dest["[applied]"].(bool)
	delete(dest, "[applied]")

	return applied, iter.Close()
}

// CREATE KEYSPACE
func (q *Query) Exec() error {
	return q.Iter().Close()
}
