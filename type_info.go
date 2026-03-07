package gocqlmem

import (
	"fmt"
)

type CqlDataType string

const (
	DataTypeAscii     CqlDataType = "ascii"
	DataTypeBigint    CqlDataType = "bigint"
	DataTypeBlob      CqlDataType = "blob"
	DataTypeBoolean   CqlDataType = "boolean"
	DataTypeCounter   CqlDataType = "counter"
	DataTypeDate      CqlDataType = "date"
	DataTypeDecimal   CqlDataType = "decimal"
	DataTypeDouble    CqlDataType = "double"
	DataTypeDuration  CqlDataType = "duration"
	DataTypeFloat     CqlDataType = "float"
	DataTypeInet      CqlDataType = "inet"
	DataTypeInt       CqlDataType = "int"
	DataTypeSmallint  CqlDataType = "smallint"
	DataTypeText      CqlDataType = "text"
	DataTypeTime      CqlDataType = "time"
	DataTypeTimestamp CqlDataType = "timestamp"
	DataTypeTimeuuid  CqlDataType = "timeuuid"
	DataTypeTinyint   CqlDataType = "tinyint"
	DataTypeUuid      CqlDataType = "uuid"
	DataTypeVarchar   CqlDataType = "varchar"
	DataTypeVarint    CqlDataType = "varint"
	DataTypeUnknown   CqlDataType = "unknown"
)

// From gocql
type Type int

const (
	TypeUnknown   Type = -0x0001
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

// String returns the name of the identifier.
func (t Type) String() string {
	switch t {
	case TypeCustom:
		return "custom"
	case TypeAscii:
		return "ascii"
	case TypeBigInt:
		return "bigint"
	case TypeBlob:
		return "blob"
	case TypeBoolean:
		return "boolean"
	case TypeCounter:
		return "counter"
	case TypeDecimal:
		return "decimal"
	case TypeDouble:
		return "double"
	case TypeFloat:
		return "float"
	case TypeInt:
		return "int"
	case TypeText:
		return "text"
	case TypeTimestamp:
		return "timestamp"
	case TypeUUID:
		return "uuid"
	case TypeVarchar:
		return "varchar"
	case TypeTimeUUID:
		return "timeuuid"
	case TypeInet:
		return "inet"
	case TypeDate:
		return "date"
	case TypeDuration:
		return "duration"
	case TypeTime:
		return "time"
	case TypeSmallInt:
		return "smallint"
	case TypeTinyInt:
		return "tinyint"
	case TypeList:
		return "list"
	case TypeMap:
		return "map"
	case TypeSet:
		return "set"
	case TypeVarint:
		return "varint"
	case TypeTuple:
		return "tuple"
	default:
		return fmt.Sprintf("unknown_type_%d", t)
	}
}

type UUID [16]byte

// TypeInfo describes a Cassandra specific data type and handles marshalling
// and unmarshalling.
type TypeInfo interface {
	// Type returns the Type id for the TypeInfo.
	Type() Type

	// Zero returns the Go zero value. For types that directly map to a Go type like
	// list<integer> it should return []int(nil) but for complex types like a
	// tuple<integer, boolean> it should be []interface{}{int(0), bool(false)}.
	Zero() interface{}

	// Marshal should marshal the value for the given TypeInfo into a byte slice
	Marshal(value interface{}) ([]byte, error)

	// Unmarshal should unmarshal the byte slice into the value for the given
	// TypeInfo.
	Unmarshal(data []byte, value interface{}) error
}

// gocql has a group of types like varcharLikeTypeInfo etc, but we are ok with just one for now
type scalarType struct {
	typ Type
}

func newScalarType(typ Type) *scalarType {
	return &scalarType{typ: typ}
}

func (t *scalarType) Type() Type {
	return t.typ
}

func (t *scalarType) Zero() interface{} {
	switch t.typ {
	case TypeInt, TypeBigInt, TypeSmallInt, TypeTinyInt:
		return int64(0)
	case TypeFloat:
		return float64(0)
	case TypeText, TypeVarchar, TypeAscii:
		return ""
	case TypeBoolean:
		return false
	case TypeBlob:
		return []byte(nil)
	default:
		// TODO: raise an alarm
		return nil
	}
}

func (t *scalarType) Marshal(value interface{}) ([]byte, error) {
	// Not implemented, do we need it in our project?
	return nil, nil
}

func (t *scalarType) Unmarshal(data []byte, value interface{}) error {
	// Not implemented, do we need it in our project?
	return nil
}

type ColumnInfo struct {
	Keyspace string
	Table    string
	Name     string
	TypeInfo TypeInfo
}

func columnInfosToColumnNames(columnInfos []ColumnInfo) []string {
	result := make([]string, len(columnInfos))
	for i := range len(columnInfos) {
		result[i] = columnInfos[i].Name
	}
	return result
}

func namesAndTypeInfosTocolumnInfos(ks string, table string, columnNames []string, typeInfos []TypeInfo) []ColumnInfo {
	result := make([]ColumnInfo, len(columnNames))
	for i := range len(columnNames) {
		result[i] = ColumnInfo{
			Keyspace: ks,
			Table:    table,
			Name:     columnNames[i],
			TypeInfo: typeInfos[i],
		}
	}
	return result
}

/*
type NativeType struct {
	typ Type
}

func NewNativeType(typ Type) NativeType {
	return NativeType{typ}
}

func (t NativeType) NewWithError() (interface{}, error) {
	typ, err := goType(t)
	if err != nil {
		return nil, err
	}
	return reflect.New(typ).Interface(), nil
}

func (t NativeType) New() interface{} {
	val, err := t.NewWithError()
	if err != nil {
		panic(err.Error())
	}
	return val
}

func (s NativeType) Type() Type {
	return s.typ
}

func (s NativeType) Version() byte {
	return 0
}

func (s NativeType) Custom() string {
	return ""
}

func (s NativeType) String() string {
	switch s.typ {
	case TypeCustom:
		return fmt.Sprintf("%s(custom)", s.typ)
	default:
		return s.typ.String()
	}
}

type CollectionType struct {
	NativeType
	Key  TypeInfo // only used for TypeMap
	Elem TypeInfo // only used for TypeMap, TypeList and TypeSet
}

func (t CollectionType) NewWithError() (interface{}, error) {
	typ, err := goType(t)
	if err != nil {
		return nil, err
	}
	return reflect.New(typ).Interface(), nil
}

func (t CollectionType) New() interface{} {
	val, err := t.NewWithError()
	if err != nil {
		panic(err.Error())
	}
	return val
}

func (c CollectionType) String() string {
	switch c.typ {
	case TypeMap:
		return fmt.Sprintf("%s(%s, %s)", c.typ, c.Key, c.Elem)
	case TypeList, TypeSet:
		return fmt.Sprintf("%s(%s)", c.typ, c.Elem)
	case TypeCustom:
		return fmt.Sprintf("%s(custom)", c.typ)
	default:
		return c.typ.String()
	}
}
*/
/*
type Duration struct {
	Months      int32
	Days        int32
	Nanoseconds int64
}

func goType(t TypeInfo) (reflect.Type, error) {
	switch t.Type() {
	case TypeVarchar, TypeAscii, TypeInet, TypeText:
		return reflect.TypeOf(*new(string)), nil
	case TypeBigInt, TypeCounter:
		return reflect.TypeOf(*new(int64)), nil
	case TypeTime:
		return reflect.TypeOf(*new(time.Duration)), nil
	case TypeTimestamp:
		return reflect.TypeOf(*new(time.Time)), nil
	case TypeBlob:
		return reflect.TypeOf(*new([]byte)), nil
	case TypeBoolean:
		return reflect.TypeOf(*new(bool)), nil
	case TypeFloat:
		return reflect.TypeOf(*new(float32)), nil
	case TypeDouble:
		return reflect.TypeOf(*new(float64)), nil
	case TypeInt:
		return reflect.TypeOf(*new(int)), nil
	case TypeSmallInt:
		return reflect.TypeOf(*new(int16)), nil
	case TypeTinyInt:
		return reflect.TypeOf(*new(int8)), nil
	case TypeDecimal:
		return reflect.TypeOf(*new(*inf.Dec)), nil
	case TypeUUID, TypeTimeUUID:
		return reflect.TypeOf(*new(UUID)), nil
	case TypeList, TypeSet:
		elemType, err := goType(t.(CollectionType).Elem)
		if err != nil {
			return nil, err
		}
		return reflect.SliceOf(elemType), nil
	case TypeMap:
		keyType, err := goType(t.(CollectionType).Key)
		if err != nil {
			return nil, err
		}
		valueType, err := goType(t.(CollectionType).Elem)
		if err != nil {
			return nil, err
		}
		return reflect.MapOf(keyType, valueType), nil
	case TypeVarint:
		return reflect.TypeOf(*new(*big.Int)), nil
	case TypeTuple:
		// what can we do here? all there is to do is to make a list of interface{}
		tuple := t.(TupleTypeInfo)
		return reflect.TypeOf(make([]interface{}, len(tuple.Elems))), nil
	case TypeUDT:
		return reflect.TypeOf(make(map[string]interface{})), nil
	case TypeDate:
		return reflect.TypeOf(*new(time.Time)), nil
	case TypeDuration:
		return reflect.TypeOf(*new(Duration)), nil
	default:
		return nil, fmt.Errorf("cannot create Go type for unknown CQL type %s", t)
	}
}
*/
/*
type TupleTypeInfo struct {
	NativeType
	Elems []TypeInfo
}

func (t TupleTypeInfo) String() string {
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("%s(", t.typ))
	for _, elem := range t.Elems {
		buf.WriteString(fmt.Sprintf("%s, ", elem))
	}
	buf.Truncate(buf.Len() - 2)
	buf.WriteByte(')')
	return buf.String()
}

func (t TupleTypeInfo) NewWithError() (interface{}, error) {
	typ, err := goType(t)
	if err != nil {
		return nil, err
	}
	return reflect.New(typ).Interface(), nil
}

func (t TupleTypeInfo) New() interface{} {
	val, err := t.NewWithError()
	if err != nil {
		panic(err.Error())
	}
	return val
}
*/
