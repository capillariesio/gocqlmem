package gocqlmem

import (
	"go/ast"
	"go/parser"
	"testing"
	"time"

	"github.com/capillariesio/gocqlmem/eval"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
)

func TestCast(t *testing.T) {
	var err error
	var exp ast.Expr
	var eCtx *eval.EvalCtx
	var val any

	vars := eval.VarValuesMap{
		"": {
			"f1": nil,
		},
	}

	exp, _ = parser.ParseExpr(`cast(f1,INT)`)
	eCtx = eval.NewPlainEvalCtx(GocqlmemEvalFunctions, GocqlmemEvalConstants, vars)

	// nil

	val, err = eCtx.Eval(exp)
	assert.Contains(t, "cannot cast <nil> to int, unsupported source type", err.Error())

	// to INT, BIGINT, TINYINT, SMALLINT, VARINT

	vars[""]["f1"] = "hello"
	val, err = eCtx.Eval(exp)
	assert.Contains(t, "cannot cast string hello to int, unsupported source type", err.Error())

	vars[""]["f1"] = int64(1)
	val, err = eCtx.Eval(exp)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), val)

	vars[""]["f1"] = float64(1.1)
	val, err = eCtx.Eval(exp)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), val)

	vars[""]["f1"] = decimal.NewFromFloat(1.1)
	val, err = eCtx.Eval(exp)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), val)

	// to FLOAT, DOUBLE

	exp, _ = parser.ParseExpr(`cast(f1,FLOAT)`)
	eCtx = eval.NewPlainEvalCtx(GocqlmemEvalFunctions, GocqlmemEvalConstants, vars)

	vars[""]["f1"] = "hello"
	val, err = eCtx.Eval(exp)
	assert.Contains(t, "cannot cast string hello to float, unsupported source type", err.Error())

	vars[""]["f1"] = int64(1)
	val, err = eCtx.Eval(exp)
	assert.Nil(t, err)
	assert.Equal(t, float64(1), val)

	vars[""]["f1"] = float64(1.1)
	val, err = eCtx.Eval(exp)
	assert.Nil(t, err)
	assert.Equal(t, float64(1.1), val)

	vars[""]["f1"] = decimal.NewFromFloat(1.1)
	val, err = eCtx.Eval(exp)
	assert.Nil(t, err)
	assert.Equal(t, float64(1.1), val)

	// to TEXT, VARCHAR

	exp, _ = parser.ParseExpr(`cast(f1,TEXT)`)
	eCtx = eval.NewPlainEvalCtx(GocqlmemEvalFunctions, GocqlmemEvalConstants, vars)

	vars[""]["f1"] = "hello"
	val, err = eCtx.Eval(exp)
	assert.Nil(t, err)
	assert.Equal(t, "hello", val)

	vars[""]["f1"] = true
	val, err = eCtx.Eval(exp)
	assert.Nil(t, err)
	assert.Equal(t, "TRUE", val)

	vars[""]["f1"] = false
	val, err = eCtx.Eval(exp)
	assert.Nil(t, err)
	assert.Equal(t, "FALSE", val)

	vars[""]["f1"] = float64(1.1)
	val, err = eCtx.Eval(exp)
	assert.Nil(t, err)
	assert.Equal(t, "1.1", val)

	vars[""]["f1"] = decimal.NewFromFloat(1.1)
	val, err = eCtx.Eval(exp)
	assert.Nil(t, err)
	assert.Equal(t, "1.1", val)

	// to DECIMAL

	exp, _ = parser.ParseExpr(`cast(f1,DECIMAL)`)
	eCtx = eval.NewPlainEvalCtx(GocqlmemEvalFunctions, GocqlmemEvalConstants, vars)

	vars[""]["f1"] = int64(1)
	val, err = eCtx.Eval(exp)
	assert.Nil(t, err)
	assert.Equal(t, decimal.NewFromInt(1), val)

	vars[""]["f1"] = float64(1.1)
	val, err = eCtx.Eval(exp)
	assert.Nil(t, err)
	assert.Equal(t, decimal.NewFromFloat(1.1), val)

	vars[""]["f1"] = decimal.NewFromFloat(1.1)
	val, err = eCtx.Eval(exp)
	assert.Nil(t, err)
	assert.Equal(t, decimal.NewFromFloat(1.1), val)
}

func TestToken(t *testing.T) {
	var err error
	var exp ast.Expr
	var eCtx *eval.EvalCtx
	var val any

	vars := eval.VarValuesMap{
		"": {
			"f1": nil,
		},
	}

	exp, _ = parser.ParseExpr(`token(f1)`)
	eCtx = eval.NewPlainEvalCtx(GocqlmemEvalFunctions, nil, vars)
	val, err = eCtx.Eval(exp)
	assert.Contains(t, "cannot token <nil>, unsupported source type <nil>", err.Error())

	vars[""]["f1"] = "hello"
	val, err = eCtx.Eval(exp)
	assert.Nil(t, err)
	assert.Equal(t, uint64(0xcbd8a7b341bd9b02), val)

	vars[""]["f1"] = int64(1)
	val, err = eCtx.Eval(exp)
	assert.Nil(t, err)
	assert.Equal(t, uint64(0x4403b7fb05c44a), val)

	vars[""]["f1"] = float64(1.1)
	val, err = eCtx.Eval(exp)
	assert.Nil(t, err)
	assert.Equal(t, uint64(0xedc37c19d7aa9352), val)

	vars[""]["f1"] = decimal.NewFromFloat(1.1)
	val, err = eCtx.Eval(exp)
	assert.Nil(t, err)
	assert.Equal(t, uint64(0x49331c269b0bc618), val)

	vars[""]["f1"] = true
	val, err = eCtx.Eval(exp)
	assert.Nil(t, err)
	assert.Equal(t, uint64(0x4403b7fb05c44a), val)
}

func TestDatetimeFunctions(t *testing.T) {
	var err error
	var exp ast.Expr
	var eCtx *eval.EvalCtx
	var val any

	exp, _ = parser.ParseExpr(`current_timestamp()`)
	eCtx = eval.NewPlainEvalCtx(GocqlmemEvalFunctions, nil, nil)
	now := time.Now().UnixMilli()
	val, err = eCtx.Eval(exp)
	assert.Nil(t, err)
	assert.Equal(t, now/10, val.(int64)/10) // Give it some slack

	exp, _ = parser.ParseExpr(`current_date()`)
	eCtx = eval.NewPlainEvalCtx(GocqlmemEvalFunctions, nil, nil)
	now = time.Now().UnixMilli()
	val, err = eCtx.Eval(exp)
	assert.Nil(t, err)
	assert.Equal(t, int64(time.Since(time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)).Hours())/24, val)

	exp, _ = parser.ParseExpr(`current_time()`)
	eCtx = eval.NewPlainEvalCtx(GocqlmemEvalFunctions, nil, nil)
	now = time.Now().UnixMilli()
	val, err = eCtx.Eval(exp)
	ti := time.Now()
	curTime := int64(((ti.Hour()*60+ti.Minute())*60+ti.Second())*1000000000 + ti.Nanosecond())
	assert.Nil(t, err)
	assert.Equal(t, curTime/1000000, val.(int64)/1000000) // Give it some slack
}

func TestMathFunctions(t *testing.T) {
	var err error
	var exp ast.Expr
	var eCtx *eval.EvalCtx
	var val any

	vars := eval.VarValuesMap{
		"": {
			"f1": nil,
		},
	}

	// Abs

	exp, _ = parser.ParseExpr(`abs(f1)`)
	eCtx = eval.NewPlainEvalCtx(GocqlmemEvalFunctions, GocqlmemEvalConstants, vars)

	vars[""]["f1"] = int64(-1)
	val, err = eCtx.Eval(exp)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), val)

	vars[""]["f1"] = float64(-1.1)
	val, err = eCtx.Eval(exp)
	assert.Nil(t, err)
	assert.Equal(t, float64(1.1), val)

	vars[""]["f1"] = decimal.NewFromFloat(-1.1)
	val, err = eCtx.Eval(exp)
	assert.Nil(t, err)
	assert.Equal(t, decimal.NewFromFloat(1.1), val)

	// Exp

	exp, _ = parser.ParseExpr(`exp(f1)`)
	eCtx = eval.NewPlainEvalCtx(GocqlmemEvalFunctions, GocqlmemEvalConstants, vars)

	vars[""]["f1"] = int64(-1)
	val, err = eCtx.Eval(exp)
	assert.Nil(t, err)
	assert.Equal(t, float64(0.36787944117144233), val)

	vars[""]["f1"] = float64(-1.1)
	val, err = eCtx.Eval(exp)
	assert.Nil(t, err)
	assert.Equal(t, float64(0.3328710836980795), val)

	vars[""]["f1"] = decimal.NewFromFloat(-1.1)
	val, err = eCtx.Eval(exp)
	assert.Nil(t, err)
	assert.Equal(t, decimal.NewFromFloat(0.3328710836980795), val)

	// Log

	exp, _ = parser.ParseExpr(`log(f1)`)
	eCtx = eval.NewPlainEvalCtx(GocqlmemEvalFunctions, GocqlmemEvalConstants, vars)

	vars[""]["f1"] = int64(1)
	val, err = eCtx.Eval(exp)
	assert.Nil(t, err)
	assert.Equal(t, float64(0), val)

	vars[""]["f1"] = float64(1.1)
	val, err = eCtx.Eval(exp)
	assert.Nil(t, err)
	assert.Equal(t, float64(0.09531017980432493), val)

	vars[""]["f1"] = decimal.NewFromFloat(1.1)
	val, err = eCtx.Eval(exp)
	assert.Nil(t, err)
	assert.Equal(t, decimal.NewFromFloat(0.09531017980432493), val)

	// Log10

	exp, _ = parser.ParseExpr(`log10(f1)`)
	eCtx = eval.NewPlainEvalCtx(GocqlmemEvalFunctions, GocqlmemEvalConstants, vars)

	vars[""]["f1"] = int64(1)
	val, err = eCtx.Eval(exp)
	assert.Nil(t, err)
	assert.Equal(t, float64(0), val)

	vars[""]["f1"] = float64(1.1)
	val, err = eCtx.Eval(exp)
	assert.Nil(t, err)
	assert.Equal(t, float64(0.04139268515822507), val)

	vars[""]["f1"] = decimal.NewFromFloat(1.1)
	val, err = eCtx.Eval(exp)
	assert.Nil(t, err)
	assert.Equal(t, decimal.NewFromFloat(0.04139268515822507), val)

	// Round

	exp, _ = parser.ParseExpr(`round(f1)`)
	eCtx = eval.NewPlainEvalCtx(GocqlmemEvalFunctions, GocqlmemEvalConstants, vars)

	vars[""]["f1"] = int64(1)
	val, err = eCtx.Eval(exp)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), val)

	vars[""]["f1"] = float64(-1.5)
	val, err = eCtx.Eval(exp)
	assert.Nil(t, err)
	assert.Equal(t, float64(-2), val)

	vars[""]["f1"] = decimal.NewFromFloat(-1.5)
	val, err = eCtx.Eval(exp)
	assert.Nil(t, err)
	assert.Equal(t, decimal.NewFromFloat(-2), val)
}
