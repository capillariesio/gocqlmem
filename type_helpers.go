package gocqlmem

import (
	"cmp"
	"fmt"
	"strings"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/capillariesio/gocqlmem/eval"
	"github.com/shopspring/decimal"
)

func stringToType(s string) (gocql.Type, error) {
	switch strings.ToLower(s) {
	case string(DataTypeAscii):
		return gocql.TypeAscii, nil
	case string(DataTypeBigint):
		return gocql.TypeBigInt, nil
	case string(DataTypeBlob):
		return gocql.TypeBlob, nil
	case string(DataTypeBoolean):
		return gocql.TypeBoolean, nil
	case string(DataTypeCounter):
		return gocql.TypeCounter, nil
	case string(DataTypeDate):
		return gocql.TypeDate, nil
	case string(DataTypeDecimal):
		return gocql.TypeDecimal, nil
	case string(DataTypeDouble):
		return gocql.TypeDouble, nil
	case string(DataTypeDuration):
		return gocql.TypeDuration, nil
	case string(DataTypeFloat):
		return gocql.TypeFloat, nil
	case string(DataTypeInet):
		return gocql.TypeInet, nil
	case string(DataTypeInt):
		return gocql.TypeInt, nil
	case string(DataTypeSmallint):
		return gocql.TypeSmallInt, nil
	case string(DataTypeText):
		return gocql.TypeText, nil
	case string(DataTypeTime):
		return gocql.TypeTime, nil
	case string(DataTypeTimestamp):
		return gocql.TypeTimestamp, nil
	case string(DataTypeTimeuuid):
		return gocql.TypeTimeUUID, nil
	case string(DataTypeTinyint):
		return gocql.TypeTinyInt, nil
	case string(DataTypeUuid):
		return gocql.TypeUUID, nil
	case string(DataTypeVarchar):
		return gocql.TypeVarchar, nil
	case string(DataTypeVarint):
		return gocql.TypeVarint, nil
	default:
		return gocql.TypeCustom, fmt.Errorf("unknown type %s", s)
	}
}

func isValidDataType(typ string) bool {
	_, err := stringToType(typ)
	return err == nil
}

func castToInternalType(val any, cqlType gocql.Type) (any, error) {
	switch cqlType {
	case gocql.TypeInt, gocql.TypeBigInt, gocql.TypeTinyInt, gocql.TypeSmallInt, gocql.TypeCounter, gocql.TypeTimestamp, gocql.TypeDate, gocql.TypeTime:
		return eval.CastToInt64(val)

	case gocql.TypeDouble, gocql.TypeFloat:
		return eval.CastToFloat64(val)

	case gocql.TypeText, gocql.TypeVarchar:
		typedVal, ok := any(val).(string)
		if !ok {
			return 0, fmt.Errorf("cast %v to string failed", val)
		}
		return typedVal, nil
	default:
		return 0, fmt.Errorf("unknown column type %v", cqlType)
	}
}

func compareInternalType(left any, right any, cqlType gocql.Type) (int, error) {
	if left == nil {
		return 0, fmt.Errorf("left is nil, not allowed in partition/clustering key comparison, dev error")
	}
	if right == nil {
		return 0, fmt.Errorf("right is nil, not allowed in partition/clustering key comparison, dev error")
	}
	switch cqlType {
	case gocql.TypeInt, gocql.TypeBigInt, gocql.TypeTinyInt, gocql.TypeSmallInt, gocql.TypeCounter, gocql.TypeTimestamp, gocql.TypeDate, gocql.TypeTime:
		typedLeft, okLeft := any(left).(int64)
		if !okLeft {
			return 0, fmt.Errorf("left cast %v to int64 failed", left)
		}
		typedRight, okRight := any(right).(int64)
		if !okRight {
			return 0, fmt.Errorf("right cast %v to int64 failed", right)
		}
		return cmp.Compare(typedLeft, typedRight), nil

	case gocql.TypeDouble, gocql.TypeFloat:
		typedLeft, okLeft := any(left).(float64)
		if !okLeft {
			return 0, fmt.Errorf("left cast %v to float64 failed", left)
		}
		typedRight, okRight := any(right).(float64)
		if !okRight {
			return 0, fmt.Errorf("right cast %v to float64 failed", right)
		}
		return cmp.Compare(typedLeft, typedRight), nil
	case gocql.TypeBoolean:
		typedLeft, okLeft := any(left).(bool)
		if !okLeft {
			return 0, fmt.Errorf("left cast %v to bool failed", left)
		}
		typedRight, okRight := any(right).(bool)
		if !okRight {
			return 0, fmt.Errorf("right cast %v to bool failed", right)
		}
		if typedLeft == true && typedRight == false {
			return 1, nil
		} else if typedLeft == false && typedRight == true {
			return -1, nil
		} else {
			return 0, nil
		}
	case gocql.TypeText, gocql.TypeVarchar:
		typedLeft, okLeft := any(left).(string)
		if !okLeft {
			return 0, fmt.Errorf("left cast %v to string failed", left)
		}
		typedRight, okRight := any(right).(string)
		if !okRight {
			return 0, fmt.Errorf("right cast %v to string failed", right)
		}
		return cmp.Compare(typedLeft, typedRight), nil
	default:
		return 0, fmt.Errorf("unknown column type %v", cqlType)
	}
}

func internalValueToProvidedPtr(src any, destPtr any) error {
	switch typedSrc := src.(type) {
	case int64:
		switch typedDestPtr := destPtr.(type) {
		case *int64:
			*typedDestPtr = typedSrc
		case *int32:
			*typedDestPtr = int32(typedSrc)
		case *int16:
			*typedDestPtr = int16(typedSrc)
		case *int8:
			*typedDestPtr = int8(typedSrc)
		case *int:
			*typedDestPtr = int(typedSrc)
		case *uint64:
			*typedDestPtr = uint64(typedSrc)
		case *uint32:
			*typedDestPtr = uint32(typedSrc)
		case *uint16:
			*typedDestPtr = uint16(typedSrc)
		case *uint8:
			*typedDestPtr = uint8(typedSrc)
		case *uint:
			*typedDestPtr = uint(typedSrc)
		default:
			return fmt.Errorf("cannot store integer %v(%T) to %T", typedSrc, typedSrc, destPtr)
		}
	case float64:
		switch typedDestPtr := destPtr.(type) {
		case *float64:
			*typedDestPtr = typedSrc
		case *float32:
			*typedDestPtr = float32(typedSrc)
		default:
			return fmt.Errorf("cannot store float %v(%T) to %T", typedSrc, typedSrc, destPtr)
		}
	case bool:
		switch typedDestPtr := destPtr.(type) {
		case *bool:
			*typedDestPtr = typedSrc
		default:
			return fmt.Errorf("cannot store bool %v(%T) to %T", typedSrc, typedSrc, destPtr)
		}
	case string:
		switch typedDestPtr := destPtr.(type) {
		case *string:
			*typedDestPtr = typedSrc
		default:
			return fmt.Errorf("cannot store string %v(%T) to %T", typedSrc, typedSrc, destPtr)
		}
	case decimal.Decimal:
		switch typedDestPtr := destPtr.(type) {
		case *decimal.Decimal:
			*typedDestPtr = typedSrc
		default:
			return fmt.Errorf("cannot store decimal %v(%T) to %T", typedSrc, typedSrc, destPtr)
		}
	default:
		return fmt.Errorf("cannot store %v(%T) to %T, type not supported", src, src, destPtr)
	}
	return nil
}

func guessInternalValueType(val any) (gocql.Type, error) {
	switch val.(type) {
	case int64:
		return gocql.TypeInt, nil
	case float64:
		return gocql.TypeFloat, nil
	case bool:
		return gocql.TypeBoolean, nil
	case string:
		return gocql.TypeText, nil
	case decimal.Decimal:
		return gocql.TypeDecimal, nil
	default:
		return gocql.TypeCustom, fmt.Errorf("unexpected internal type %T", val)
	}
}

func columnInfosToColumnNames(columnInfos []gocql.ColumnInfo) []string {
	result := make([]string, len(columnInfos))
	for i := range len(columnInfos) {
		result[i] = columnInfos[i].Name
	}
	return result
}

func namesAndTypeInfosTocolumnInfos(ks string, table string, columnNames []string, typeInfos []gocql.TypeInfo) []gocql.ColumnInfo {
	result := make([]gocql.ColumnInfo, len(columnNames))
	for i := range len(columnNames) {
		result[i] = gocql.ColumnInfo{
			Keyspace: ks,
			Table:    table,
			Name:     columnNames[i],
			TypeInfo: typeInfos[i],
		}
	}
	return result
}
