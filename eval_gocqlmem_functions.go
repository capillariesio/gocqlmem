package gocqlmem

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash"
	"math"
	"strconv"
	"time"

	"github.com/capillariesio/gocqlmem/eval"
	"github.com/shopspring/decimal"
	"github.com/twmb/murmur3"
)

var GocqlmemEvalFunctions = map[string]eval.EvalFunction{
	"cast":              callCast,
	"token":             callToken,
	"current_timestamp": callCurrentTimestamp,
	"current_date":      callCurrentDate,
	"current_time":      callCurrentTime,
	"abs":               callAbs,
	"exp":               callExp,
	"log":               callLog,
	"log10":             callLog10,
	"round":             callRound,
}

/*
https://cassandra.apache.org/doc/latest/cassandra/developing/cql/functions.html#cast

YES ascii	text, varchar
YES bigint	tinyint, smallint, int, float, double, decimal, varint, text, varchar
YES boolean	text, varchar
YES counter	tinyint, smallint, int, bigint, float, double, decimal, varint, text, varchar
TODO date	timestamp
YES decimal	tinyint, smallint, int, bigint, float, double, varint, text, varchar
YES double	tinyint, smallint, int, bigint, float, decimal, varint, text, varchar
YES float	tinyint, smallint, int, bigint, double, decimal, varint, text, varchar
TODO inet	text, varchar
YES int	tinyint, smallint, bigint, float, double, decimal, varint, text, varchar
YES smallint	tinyint, int, bigint, float, double, decimal, varint, text, varchar
TODO time	text, varchar
TODO timestamp	date, text, varchar
TODO timeuuid	timestamp, date, text, varchar
YES tinyint	tinyint, smallint, int, bigint, float, double, decimal, varint, text, varchar
TODO uuid	text, varchar
YES varint	tinyint, smallint, int, bigint, float, double, decimal, text, varchar
*/
func callCast(args []any) (any, error) {
	if err := eval.CheckArgs("cast", 2, len(args)); err != nil {
		return nil, err
	}

	dataType, ok := args[1].(CqlDataType)
	if !ok {
		return nil, fmt.Errorf("cannot convert cast() arg %v to DataType", args[1])
	}

	switch typedVal := args[0].(type) {
	case int64, int32, int16, int8:
		typedValInt64, err := eval.CastToInt64(typedVal)
		if err != nil {
			return nil, fmt.Errorf("cannot cast %v to %v int: %s", typedVal, dataType, err.Error())
		}
		switch dataType {
		case DataTypeBigint, DataTypeSmallint, DataTypeTinyint, DataTypeInt, DataTypeVarint:
			return typedValInt64, nil
		case DataTypeFloat, DataTypeDouble:
			return float64(typedValInt64), nil
		case DataTypeDecimal:
			return decimal.NewFromInt(typedValInt64), nil
		case DataTypeText, DataTypeVarchar:
			return strconv.FormatInt(typedValInt64, 10), nil
		default:
			return nil, fmt.Errorf("cannot cast int %v to %v", typedVal, dataType)
		}
	case float32, float64:
		typedValFloat64, err := eval.CastToFloat64(typedVal)
		if err != nil {
			return nil, fmt.Errorf("cannot cast float %v to %v: %s", typedVal, dataType, err.Error())
		}
		switch dataType {
		case DataTypeBigint, DataTypeSmallint, DataTypeTinyint, DataTypeInt, DataTypeVarint:
			return int64(typedValFloat64), nil
		case DataTypeFloat, DataTypeDouble:
			return float64(typedValFloat64), nil
		case DataTypeDecimal:
			return decimal.NewFromFloat(typedValFloat64), nil
		case DataTypeText, DataTypeVarchar:
			return strconv.FormatFloat(typedValFloat64, 'f', -1, 64), nil
		default:
			return nil, fmt.Errorf("cannot cast float %v to %v", typedVal, dataType)
		}

	case bool:
		switch dataType {
		case DataTypeText, DataTypeVarchar:
			if typedVal {
				return "TRUE", nil
			} else {
				return "FALSE", nil
			}
		default:
			return nil, fmt.Errorf("cannot cast bool %v to %v", typedVal, dataType)
		}
	case decimal.Decimal:
		switch dataType {
		case DataTypeBigint, DataTypeSmallint, DataTypeTinyint, DataTypeInt, DataTypeVarint:
			return typedVal.BigInt().Int64(), nil
		case DataTypeFloat, DataTypeDouble:
			floatVal, _ := typedVal.Float64()
			return floatVal, nil
		case DataTypeDecimal:
			return typedVal, nil
		case DataTypeText, DataTypeVarchar:
			return typedVal.String(), nil
		default:
			return nil, fmt.Errorf("cannot cast decimal %v to %v", typedVal, dataType)
		}

	case string:
		switch dataType {
		case DataTypeText, DataTypeVarchar:
			return typedVal, nil
		default:
			return nil, fmt.Errorf("cannot cast string %v to %v", typedVal, dataType)
		}

	default:
		return nil, fmt.Errorf("cannot cast %v to %v, unsupported source type", args[0], dataType)
	}
}

func callToken(args []any) (any, error) {
	if err := eval.CheckArgs("token", 1, len(args)); err != nil {
		return nil, err
	}
	switch typedVal := args[0].(type) {
	case int, int64, int32, int16, int8:
		typedValInt64, err := eval.CastToInt64(typedVal)
		if err != nil {
			return nil, fmt.Errorf("cannot token int %v: %s", typedVal, err.Error())
		}

		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, uint64(typedValInt64))

		var h64 hash.Hash64 = murmur3.New64()
		h64.Write(b)
		return h64.Sum64(), nil

	case float32, float64:
		typedValFloat64, err := eval.CastToFloat64(typedVal)
		if err != nil {
			return nil, fmt.Errorf("cannot token float %v: %s", typedVal, err.Error())
		}

		var buf bytes.Buffer
		err = binary.Write(&buf, binary.LittleEndian, typedValFloat64)
		if err != nil {
			return nil, fmt.Errorf("cannot token float %v, binary fails: %s", typedVal, err.Error())
		}

		var h64 hash.Hash64 = murmur3.New64()
		h64.Write(buf.Bytes())
		return h64.Sum64(), nil

	case bool:
		typedValInt64 := 0
		if typedVal {
			typedValInt64 = 1
		}
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, uint64(typedValInt64))

		var h64 hash.Hash64 = murmur3.New64()
		h64.Write(b)
		return h64.Sum64(), nil

	case decimal.Decimal:
		b, err := typedVal.MarshalBinary()
		if err != nil {
			return nil, fmt.Errorf("cannot token decimal %v: %s", typedVal, err.Error())
		}
		var h64 hash.Hash64 = murmur3.New64()
		h64.Write(b)
		return h64.Sum64(), nil

	case string:
		var h64 hash.Hash64 = murmur3.New64()
		h64.Write([]byte(typedVal))
		return h64.Sum64(), nil
	default:
		return nil, fmt.Errorf("cannot token %v, unsupported source type %T", args[0], args[0])
	}
}

func callCurrentTimestamp(args []any) (any, error) {
	if err := eval.CheckArgs("current_timestamp", 0, len(args)); err != nil {
		return nil, err
	}

	return time.Now().UnixMilli(), nil // Cassandra: millis from epoch
}

func callCurrentDate(args []any) (any, error) {
	if err := eval.CheckArgs("current_date", 0, len(args)); err != nil {
		return nil, err
	}

	dur := time.Since(time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC))
	return int64(dur.Hours() / 24), nil // Cassandra: days since epoch (32-bit, but we keep using int64)
}

func callCurrentTime(args []any) (any, error) {
	if err := eval.CheckArgs("current_time", 0, len(args)); err != nil {
		return nil, err
	}

	// Local time
	ti := time.Now()
	return int64(((ti.Hour()*60+ti.Minute())*60+ti.Second())*1000000000 + ti.Nanosecond()), nil // Cassandra: nanos from midnight
}

func callAbs(args []any) (any, error) {
	if err := eval.CheckArgs("abs", 1, len(args)); err != nil {
		return nil, err
	}

	switch typedVal := args[0].(type) {
	case int, int64, int32, int16, int8:
		typedValInt64, err := eval.CastToInt64(typedVal)
		if err != nil {
			return nil, fmt.Errorf("cannot abs int %v: %s", typedVal, err.Error())
		}

		if typedValInt64 < 0 {
			typedValInt64 = -typedValInt64
		}
		return typedValInt64, nil

	case float32, float64:
		typedValInt64, err := eval.CastToFloat64(typedVal)
		if err != nil {
			return nil, fmt.Errorf("cannot abs float %v: %s", typedVal, err.Error())
		}

		if typedValInt64 < 0 {
			typedValInt64 = -typedValInt64
		}
		return typedValInt64, nil

	case decimal.Decimal:
		typedValFloat64, err := eval.CastToFloat64(typedVal)
		if err != nil {
			return nil, fmt.Errorf("cannot abs decimal %v: %s", typedVal, err.Error())
		}

		if typedValFloat64 < 0 {
			typedValFloat64 = -typedValFloat64
		}
		return decimal.NewFromFloat(typedValFloat64), nil
	default:
		return nil, fmt.Errorf("cannot abs %v, unsupported source type", args[0])
	}
}

func callExp(args []any) (any, error) {
	if err := eval.CheckArgs("exp", 1, len(args)); err != nil {
		return nil, err
	}

	switch typedVal := args[0].(type) {
	case int, int64, int32, int16, int8:
		typedValInt64, err := eval.CastToInt64(typedVal)
		if err != nil {
			return nil, fmt.Errorf("cannot exp int %v: %s", typedVal, err.Error())
		}
		return math.Exp(float64(typedValInt64)), nil

	case float32, float64:
		typedValFloat64, err := eval.CastToFloat64(typedVal)
		if err != nil {
			return nil, fmt.Errorf("cannot exp float %v: %s", typedVal, err.Error())
		}
		return math.Exp(typedValFloat64), nil

	case decimal.Decimal:
		typedValFloat64, err := eval.CastToFloat64(typedVal)
		if err != nil {
			return nil, fmt.Errorf("cannot exp decimal %v: %s", typedVal, err.Error())
		}
		typedValFloat64 = math.Exp(typedValFloat64)
		return decimal.NewFromFloat(typedValFloat64), nil

	default:
		return nil, fmt.Errorf("cannot exp %v, unsupported source type", args[0])
	}
}

func callLog(args []any) (any, error) {
	if err := eval.CheckArgs("log", 1, len(args)); err != nil {
		return nil, err
	}

	switch typedVal := args[0].(type) {
	case int, int64, int32, int16, int8:
		typedValInt64, err := eval.CastToInt64(typedVal)
		if err != nil {
			return nil, fmt.Errorf("cannot log int %v: %s", typedVal, err.Error())
		}
		return math.Log(float64(typedValInt64)), nil

	case float32, float64:
		typedValFloat64, err := eval.CastToFloat64(typedVal)
		if err != nil {
			return nil, fmt.Errorf("cannot log float %v: %s", typedVal, err.Error())
		}
		return math.Log(typedValFloat64), nil

	case decimal.Decimal:
		typedValFloat64, err := eval.CastToFloat64(typedVal)
		if err != nil {
			return nil, fmt.Errorf("cannot log decimal %v: %s", typedVal, err.Error())
		}
		typedValFloat64 = math.Log(typedValFloat64)
		return decimal.NewFromFloat(typedValFloat64), nil

	default:
		return nil, fmt.Errorf("cannot log %v, unsupported source type", args[0])
	}
}

func callLog10(args []any) (any, error) {
	if err := eval.CheckArgs("log10", 1, len(args)); err != nil {
		return nil, err
	}

	switch typedVal := args[0].(type) {
	case int, int64, int32, int16, int8:
		typedValInt64, err := eval.CastToInt64(typedVal)
		if err != nil {
			return nil, fmt.Errorf("cannot log10 int %v: %s", typedVal, err.Error())
		}
		return math.Log10(float64(typedValInt64)), nil

	case float32, float64:
		typedValFloat64, err := eval.CastToFloat64(typedVal)
		if err != nil {
			return nil, fmt.Errorf("cannot log10 float %v: %s", typedVal, err.Error())
		}
		return math.Log10(typedValFloat64), nil

	case decimal.Decimal:
		typedValFloat64, err := eval.CastToFloat64(typedVal)
		if err != nil {
			return nil, fmt.Errorf("cannot log10 decimal %v: %s", typedVal, err.Error())
		}
		typedValFloat64 = math.Log10(typedValFloat64)
		return decimal.NewFromFloat(typedValFloat64), nil

	default:
		return nil, fmt.Errorf("cannot log10 %v, unsupported source type", args[0])
	}
}

func callRound(args []any) (any, error) {
	if err := eval.CheckArgs("round", 1, len(args)); err != nil {
		return nil, err
	}

	switch typedVal := args[0].(type) {
	case int, int64, int32, int16, int8:
		typedValInt64, err := eval.CastToInt64(typedVal)
		if err != nil {
			return nil, fmt.Errorf("cannot round int %v: %s", typedVal, err.Error())
		}
		return typedValInt64, nil

	case float32, float64:
		typedValFloat64, err := eval.CastToFloat64(typedVal)
		if err != nil {
			return nil, fmt.Errorf("cannot round float %v: %s", typedVal, err.Error())
		}
		return math.Round(typedValFloat64), nil

	case decimal.Decimal:
		typedValFloat64, err := eval.CastToFloat64(typedVal)
		if err != nil {
			return nil, fmt.Errorf("cannot round decimal %v: %s", typedVal, err.Error())
		}
		typedValFloat64 = math.Round(typedValFloat64)
		return decimal.NewFromFloat(typedValFloat64), nil

	default:
		return nil, fmt.Errorf("cannot round %v, unsupported source type", args[0])
	}
}
