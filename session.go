package gocqlmem

import (
	"fmt"
	"sync"
)

type Session struct {
	KeyspaceMap map[string]*Keyspace
	Lock        sync.RWMutex
}

func NewSession() *Session {
	return &Session{
		KeyspaceMap: map[string]*Keyspace{},
	}
}

func (s *Session) createKeyspace(cmd *CommandCreateKeyspace) error {
	s.Lock.Lock()
	defer s.Lock.Unlock()

	_, alreadyExists := s.KeyspaceMap[cmd.KeyspaceName]
	if alreadyExists && cmd.IfNotExists {
		return nil
	}
	if alreadyExists && !cmd.IfNotExists {
		return fmt.Errorf("cannot create keyspace %s, it already exists and no IF NOT EXISTS were specified", cmd.KeyspaceName)
	}
	s.KeyspaceMap[cmd.KeyspaceName] = newKeyspace()
	return nil
}

func (s *Session) dropKeyspace(cmd *CommandDropKeyspace) error {
	s.Lock.Lock()
	defer s.Lock.Unlock()

	ks, alreadyExists := s.KeyspaceMap[cmd.KeyspaceName]
	if !alreadyExists && cmd.IfExists {
		return nil
	}
	if !alreadyExists && !cmd.IfExists {
		return fmt.Errorf("cannot drop keyspace %s, it was not found and no IF EXISTS were specified", cmd.KeyspaceName)
	}

	ks.Lock.Lock()
	delete(s.KeyspaceMap, cmd.KeyspaceName)
	ks.Lock.Unlock()

	return nil
}

func (s *Session) createTable(cmd *CommandCreateTable) error {
	s.Lock.RLock()

	ks, ksExists := s.KeyspaceMap[cmd.GetCtxKeyspace()]
	s.Lock.RUnlock()
	if !ksExists {
		return fmt.Errorf("keyspace %s does not exist", cmd.GetCtxKeyspace())
	}

	return ks.createTable(cmd)
}

func (s *Session) truncateTable(cmd *CommandTruncateTable) error {
	s.Lock.RLock()

	ks, ksExists := s.KeyspaceMap[cmd.GetCtxKeyspace()]
	s.Lock.RUnlock()
	if !ksExists {
		return fmt.Errorf("keyspace %s does not exist", cmd.GetCtxKeyspace())
	}

	return ks.truncateTable(cmd)
}

func (s *Session) dropTable(cmd *CommandDropTable) error {
	s.Lock.RLock()

	ks, ksExists := s.KeyspaceMap[cmd.GetCtxKeyspace()]
	s.Lock.RUnlock()
	if !ksExists {
		return fmt.Errorf("keyspace %s does not exist", cmd.GetCtxKeyspace())
	}

	return ks.dropTable(cmd)
}

func (s *Session) execInsert(cmd *CommandInsert) (bool, error) {
	s.Lock.RLock()

	ks, ksExists := s.KeyspaceMap[cmd.GetCtxKeyspace()]
	s.Lock.RUnlock()
	if !ksExists {
		return false, fmt.Errorf("keyspace %s does not exist", cmd.GetCtxKeyspace())
	}

	return ks.execInsert(cmd)
}

func (s *Session) execSelect(cmd *CommandSelect, lastSelectedRowIdx int, maxRows int) ([]string, [][]any, []TypeInfo, int, error) {
	s.Lock.RLock()

	ks, ksExists := s.KeyspaceMap[cmd.GetCtxKeyspace()]
	s.Lock.RUnlock()
	if !ksExists {
		return []string{}, [][]any{}, []TypeInfo{}, -1, fmt.Errorf("keyspace %s does not exist", cmd.GetCtxKeyspace())
	}

	return ks.execSelect(cmd, lastSelectedRowIdx, maxRows)
}

func (s *Session) execUpdate(cmd *CommandUpdate) (bool, error) {
	s.Lock.RLock()

	ks, ksExists := s.KeyspaceMap[cmd.GetCtxKeyspace()]
	s.Lock.RUnlock()
	if !ksExists {
		return false, fmt.Errorf("keyspace %s does not exist", cmd.GetCtxKeyspace())
	}

	return ks.execUpdate(cmd)
}

func (s *Session) execDelete(cmd *CommandDelete) (bool, error) {
	s.Lock.RLock()

	ks, ksExists := s.KeyspaceMap[cmd.GetCtxKeyspace()]
	s.Lock.RUnlock()
	if !ksExists {
		return false, fmt.Errorf("keyspace %s does not exist", cmd.GetCtxKeyspace())
	}

	return ks.execDelete(cmd)
}

// This is a gocql-exported struct, leave it as it is
type RowData struct {
	Columns []string
	Values  []interface{}
}

// func dereference(i interface{}) interface{} {
// 	return reflect.Indirect(reflect.ValueOf(i)).Interface()
// }

// func (r *RowData) rowMap(m map[string]interface{}) {
// 	for i, column := range r.Columns {
// 		val := dereference(r.Values[i])
// 		if valVal := reflect.ValueOf(val); valVal.Kind() == reflect.Slice {
// 			valCopy := reflect.MakeSlice(valVal.Type(), valVal.Len(), valVal.Cap())
// 			reflect.Copy(valCopy, valVal)
// 			m[column] = valCopy.Interface()
// 		} else {
// 			m[column] = val
// 		}
// 	}
// }

type nextIter struct {
	qry   *Query
	pos   int
	oncea sync.Once
	once  sync.Once
	next  *Iter
}

// func (n *nextIter) fetch() *Iter {
// 	n.once.Do(func() {
// 		// if the query was specifically run on a connection then re-use that
// 		// connection when fetching the next results
// 		if n.qry.conn != nil {
// 			n.next = n.qry.conn.executeQuery(n.qry.Context(), n.qry)
// 		} else {
// 			n.next = n.qry.session.executeQuery(n.qry)
// 		}
// 	})
// 	return n.next
// }

// func (n *nextIter) fetchAsync() {
// 	n.oncea.Do(func() {
// 		go n.fetch()
// 	})
// }

type resultMetadata struct {
	flags int

	// only if flagPageState
	pagingState []byte

	columns  []ColumnInfo
	colCount int

	// this is a count of the total number of columns which can be scanned,
	// it is at minimum len(columns) but may be larger, for instance when a column
	// is a UDT or tuple.
	actualColCount int
}

// TupeColumnName will return the column name of a tuple value in a column named
// c at index n. It should be used if a specific element within a tuple is needed
// to be extracted from a map returned from SliceMap or MapScan.
func TupleColumnName(c string, n int) string {
	return fmt.Sprintf("%s[%d]", c, n)
}

// func scanColumn(p []byte, col ColumnInfo, dest []interface{}) (int, error) {
// 	if dest[0] == nil {
// 		return 1, nil
// 	}

// 	if col.TypeInfo.Type() == TypeTuple {
// 		// this will panic, actually a bug, please report
// 		tuple := col.TypeInfo.(TupleTypeInfo)

// 		count := len(tuple.Elems)
// 		// here we pass in a slice of the struct which has the number number of
// 		// values as elements in the tuple
// 		if err := Unmarshal(col.TypeInfo, p, dest[:count]); err != nil {
// 			return 0, err
// 		}
// 		return count, nil
// 	} else {
// 		if err := Unmarshal(col.TypeInfo, p, dest[0]); err != nil {
// 			return 0, err
// 		}
// 		return 1, nil
// 	}
// }

func (s *Session) Query(stmt string, values ...interface{}) *Query {
	qry := Query{}
	qry.session = s
	qry.stmt = stmt
	qry.values = values
	return &qry
}

/**/
