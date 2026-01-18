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

func (s *Session) DropKeyspace(cmd *CommandDropKeyspace) error {
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

func (s *Session) CreateTable(cmd *CommandCreateTable) error {
	s.Lock.Lock()

	ks, ksExists := s.KeyspaceMap[cmd.GetCtxKeyspace()]
	s.Lock.Unlock()
	if !ksExists {
		return fmt.Errorf("keyspace %s does not exist", cmd.GetCtxKeyspace())
	}

	return ks.createTable(cmd)
}

func (s *Session) DropTable(cmd *CommandDropTable) error {
	s.Lock.Lock()

	ks, ksExists := s.KeyspaceMap[cmd.GetCtxKeyspace()]
	s.Lock.Unlock()
	if !ksExists {
		return fmt.Errorf("keyspace %s does not exist", cmd.GetCtxKeyspace())
	}

	return ks.dropTable(cmd)
}

/*
type RowData struct {
	Columns []string
	Values  []interface{}
}

func dereference(i interface{}) interface{} {
	return reflect.Indirect(reflect.ValueOf(i)).Interface()
}

func (r *RowData) rowMap(m map[string]interface{}) {
	for i, column := range r.Columns {
		val := dereference(r.Values[i])
		if valVal := reflect.ValueOf(val); valVal.Kind() == reflect.Slice {
			valCopy := reflect.MakeSlice(valVal.Type(), valVal.Len(), valVal.Cap())
			reflect.Copy(valCopy, valVal)
			m[column] = valCopy.Interface()
		} else {
			m[column] = val
		}
	}
}

type nextIter struct {
	qry   *Query
	pos   int
	oncea sync.Once
	once  sync.Once
	next  *Iter
}

func (n *nextIter) fetch() *Iter {
	n.once.Do(func() {
		// if the query was specifically run on a connection then re-use that
		// connection when fetching the next results
		if n.qry.conn != nil {
			n.next = n.qry.conn.executeQuery(n.qry.Context(), n.qry)
		} else {
			n.next = n.qry.session.executeQuery(n.qry)
		}
	})
	return n.next
}

type Type int

const (
	TypeCustom    Type = 0x0000
	TypeAscii     Type = 0x0001
	TypeBigInt    Type = 0x0002
	TypeBlob      Type = 0x0003
	TypeBoolean   Type = 0x0004
	TypeCounter   Type = 0x0005
	TypeDecimal   Type = 0x0006
	TypeDouble    Type = 0x0007
	TypeFloat     Type = 0x0008
	TypeInt       Type = 0x0009
	TypeText      Type = 0x000A
	TypeTimestamp Type = 0x000B
	TypeUUID      Type = 0x000C
	TypeVarchar   Type = 0x000D
	TypeVarint    Type = 0x000E
	TypeTimeUUID  Type = 0x000F
	TypeInet      Type = 0x0010
	TypeDate      Type = 0x0011
	TypeTime      Type = 0x0012
	TypeSmallInt  Type = 0x0013
	TypeTinyInt   Type = 0x0014
	TypeDuration  Type = 0x0015
	TypeList      Type = 0x0020
	TypeMap       Type = 0x0021
	TypeSet       Type = 0x0022
	TypeUDT       Type = 0x0030
	TypeTuple     Type = 0x0031
)

// TypeInfo describes a Cassandra specific data type.
type TypeInfo interface {
	Type() Type
	Version() byte
	Custom() string

	// New creates a pointer to an empty version of whatever type
	// is referenced by the TypeInfo receiver.
	//
	// If there is no corresponding Go type for the CQL type, New panics.
	//
	// Deprecated: Use NewWithError instead.
	New() interface{}

	// NewWithError creates a pointer to an empty version of whatever type
	// is referenced by the TypeInfo receiver.
	//
	// If there is no corresponding Go type for the CQL type, NewWithError returns an error.
	NewWithError() (interface{}, error)
}

type ColumnInfo struct {
	Keyspace string
	Table    string
	Name     string
	TypeInfo TypeInfo
}

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

type Iter struct {
	err     error
	pos     int
	meta    resultMetadata
	numRows int
	next    *nextIter
	//host    *HostInfo

	framer *framer
	closed int32
}

func (iter *Iter) Columns() []ColumnInfo {
	return iter.meta.columns
}

func (iter *Iter) RowData() (RowData, error) {
	if iter.err != nil {
		return RowData{}, iter.err
	}

	columns := make([]string, 0, len(iter.Columns()))
	values := make([]interface{}, 0, len(iter.Columns()))

	for _, column := range iter.Columns() {
		if c, ok := column.TypeInfo.(TupleTypeInfo); !ok {
			val, err := column.TypeInfo.NewWithError()
			if err != nil {
				return RowData{}, err
			}
			columns = append(columns, column.Name)
			values = append(values, val)
		} else {
			for i, elem := range c.Elems {
				columns = append(columns, TupleColumnName(column.Name, i))
				val, err := elem.NewWithError()
				if err != nil {
					return RowData{}, err
				}
				values = append(values, val)
			}
		}
	}

	rowData := RowData{
		Columns: columns,
		Values:  values,
	}

	return rowData, nil
}

func (iter *Iter) Scan(dest ...interface{}) bool {
	if iter.err != nil {
		return false
	}

	if iter.pos >= iter.numRows {
		if iter.next != nil {
			*iter = *iter.next.fetch()
			return iter.Scan(dest...)
		}
		return false
	}

	if iter.next != nil && iter.pos >= iter.next.pos {
		iter.next.fetchAsync()
	}

	// currently only support scanning into an expand tuple, such that its the same
	// as scanning in more values from a single column
	if len(dest) != iter.meta.actualColCount {
		iter.err = fmt.Errorf("gocql: not enough columns to scan into: have %d want %d", len(dest), iter.meta.actualColCount)
		return false
	}

	// i is the current position in dest, could posible replace it and just use
	// slices of dest
	i := 0
	for _, col := range iter.meta.columns {
		colBytes, err := iter.readColumn()
		if err != nil {
			iter.err = err
			return false
		}

		n, err := scanColumn(colBytes, col, dest[i:])
		if err != nil {
			iter.err = err
			return false
		}
		i += n
	}

	iter.pos++
	return true
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
		rowData.rowMap(m)
		dataToReturn = append(dataToReturn, m)
	}
	if iter.err != nil {
		return nil, iter.err
	}
	return dataToReturn, nil
}

func (iter *Iter) MapScan(m map[string]interface{}) bool {
	if iter.err != nil {
		return false
	}

	// Not checking for the error because we just did
	rowData, _ := iter.RowData()

	for i, col := range rowData.Columns {
		if dest, ok := m[col]; ok {
			rowData.Values[i] = dest
		}
	}

	if iter.Scan(rowData.Values...) {
		rowData.rowMap(m)
		return true
	}
	return false
}

func (iter *Iter) Close() error {
	return iter.err
}

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
		q.session.createKeyspace(cmd)

	}
	return &Iter{}
}

// INSERT, UPDATE, DELETE
func (q *Query) MapScanCAS(dest map[string]interface{}) (applied bool, err error) {
	q.disableSkipMetadata = true
	iter := q.Iter()
	if err := iter.checkErrAndNotFound(); err != nil {
		return false, err
	}
	iter.MapScan(dest)
	applied = dest["[applied]"].(bool)
	delete(dest, "[applied]")

	return applied, iter.Close()
}

func (q *Query) Exec() error {
	return q.Iter().Close()
}

func (s *Session) Query(stmt string, values ...interface{}) *Query {
	qry := Query{}
	qry.session = s
	qry.stmt = stmt
	qry.values = values
	return &qry
}
*/
