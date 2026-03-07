package gocqlmem

import (
	"cmp"
	"fmt"
	"strings"

	"github.com/capillariesio/gocqlmem/eval"
)

func StringToType(s string) Type {
	switch strings.ToLower(s) {
	case string(DataTypeAscii):
		return TypeAscii
	case string(DataTypeBigint):
		return TypeBigInt
	case string(DataTypeBlob):
		return TypeBlob
	case string(DataTypeBoolean):
		return TypeBoolean
	case string(DataTypeCounter):
		return TypeCounter
	case string(DataTypeDate):
		return TypeDate
	case string(DataTypeDecimal):
		return TypeDecimal
	case string(DataTypeDouble):
		return TypeDouble
	case string(DataTypeDuration):
		return TypeDuration
	case string(DataTypeFloat):
		return TypeFloat
	case string(DataTypeInet):
		return TypeInet
	case string(DataTypeInt):
		return TypeInt
	case string(DataTypeSmallint):
		return TypeSmallInt
	case string(DataTypeText):
		return TypeText
	case string(DataTypeTime):
		return TypeTime
	case string(DataTypeTimestamp):
		return TypeTimestamp
	case string(DataTypeTimeuuid):
		return TypeTimeUUID
	case string(DataTypeTinyint):
		return TypeTinyInt
	case string(DataTypeUuid):
		return TypeUUID
	case string(DataTypeVarchar):
		return TypeVarchar
	case string(DataTypeVarint):
		return TypeVarint
	default:
		return TypeUnknown
	}
}

func IsValidDataType(s string) bool {
	return StringToType(s) != TypeUnknown
}

func CastToInternalType(val any, cqlType Type) (any, error) {
	switch cqlType {
	case TypeInt, TypeBigInt, TypeTinyInt, TypeSmallInt, TypeCounter:
		return eval.CastToInt64(val)

	case TypeDouble, TypeFloat:
		return eval.CastToFloat64(val)

	case TypeText, TypeVarchar:
		typedVal, ok := any(val).(string)
		if !ok {
			return 0, fmt.Errorf("cast %v to string failed", val)
		}
		return typedVal, nil
	default:
		return 0, fmt.Errorf("unknown column type %v", cqlType)
	}
}

func CompareInternalType(left any, right any, cqlType Type) (int, error) {
	if left == nil {
		return 0, fmt.Errorf("left is nil, not allowed in partition/clustering key comparison, dev error")
	}
	if right == nil {
		return 0, fmt.Errorf("right is nil, not allowed in partition/clustering key comparison, dev error")
	}
	switch cqlType {
	case TypeInt, TypeBigInt, TypeTinyInt, TypeSmallInt:
		typedLeft, okLeft := any(left).(int64)
		if !okLeft {
			return 0, fmt.Errorf("left cast %v to int64 failed", left)
		}
		typedRight, okRight := any(right).(int64)
		if !okRight {
			return 0, fmt.Errorf("right cast %v to int64 failed", right)
		}
		return cmp.Compare(typedLeft, typedRight), nil

	case TypeDouble, TypeFloat:
		typedLeft, okLeft := any(left).(float64)
		if !okLeft {
			return 0, fmt.Errorf("left cast %v to float64 failed", left)
		}
		typedRight, okRight := any(right).(float64)
		if !okRight {
			return 0, fmt.Errorf("right cast %v to float64 failed", right)
		}
		return cmp.Compare(typedLeft, typedRight), nil
	case TypeBoolean:
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
	case TypeText, TypeVarchar:
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

func InternalValueToProvidedPtr(src any, destPtr any) error {
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
	default:
		return fmt.Errorf("cannot store %v(%T) to %T, type not supported", src, src, destPtr)
	}
	return nil
}

func GuessInternalValueType(val any) Type {
	switch val.(type) {
	case int64:
		return TypeInt
	case float64:
		return TypeFloat
	case bool:
		return TypeBoolean
	case string:
		return TypeText
	default:
		return TypeUnknown
	}
}
