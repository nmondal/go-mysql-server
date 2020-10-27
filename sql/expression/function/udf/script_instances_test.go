package udf

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestScripting_Appropriate_Instance(t *testing.T) {
	assertions := require.New(t)
	// js
	si := GetScriptInstance("js", "42")
	assertions.Equal("ECMAScript5.1", si.Dialect())
	// nothing
	si = GetScriptInstance("", "42")
	assertions.Equal("ECMAScript5.1", si.Dialect())
	// expr
	si = GetScriptInstance("expr", "42")
	assertions.Equal("expr", si.Dialect())
}

func TestScripting_JS_Expressions_No_Params(t *testing.T) {

	assertions := require.New(t)
	si := GetScriptInstance("js", "42")
	res, _ := si.EvalFromString("42")
	// this is for int
	assertions.True(42 == res.(int64))
	// now double
	res, _ = si.EvalFromString("42.23")
	assertions.True(42.23 == res.(float64))
	// now string
	res, _ = si.EvalFromString("x='42';")
	assertions.True("42" == res.(string))
	// list of primitives
	res, _ = si.EvalFromString(" x =[ 42, 42, 42 ] ;")
	assertions.Equal(3, len(res.([]interface{})))
	// a map ?
	res, _ = si.EvalFromString(" x = { 'i' : 42 }  ;")
	assertions.Equal(1, len(res.(map[string]interface{})))

}

func TestScripting_JS_Expressions_With_Params(t *testing.T) {
	assertions := require.New(t)
	si := GetScriptInstance("js", "x + y ;")
	res, _ := si.ScriptEval(map[string]interface{}{"x": 32, "y": 10})
	assertions.True(42 == res.(int64))
	res, _ = si.ScriptEval(map[string]interface{}{"x": 12, "y": 30})
	assertions.True(42 == res.(int64))
	res, _ = si.ScriptEval(map[string]interface{}{"x": -1, "y": 1})
	assertions.True(0 == res.(int64))
}

func TestScripting_EXPR_EVAL_Expressions_No_Params(t *testing.T) {
	assertions := require.New(t)
	si := GetScriptInstance("expr", "42")
	res, _ := si.EvalFromString("42")
	assertions.Equal(42, res)
}

func TestScripting_EXPR_Expressions_With_Params(t *testing.T) {
	assertions := require.New(t)
	si := GetScriptInstance("expr", "x + y")
	res, _ := si.ScriptEval(map[string]interface{}{"x": 32, "y": 10})
	assertions.Equal(42, res)
	res, _ = si.ScriptEval(map[string]interface{}{"x": 12, "y": 30})
	assertions.Equal(42, res)
	res, _ = si.ScriptEval(map[string]interface{}{"x": -1, "y": 1})
	assertions.Equal(0, res)
}

func TestScripting_V8_EVAL_Expressions_No_Params(t *testing.T) {
	assertions := require.New(t)
	si := GetScriptInstance("v8", "42")
	res, _ := si.EvalFromString("42")
	assertions.Equal(float64(42), res)
	// test JS V8 mapping function ...
	res, _ = si.EvalFromString("[1,2,3,4].reduce((sum, x) => sum + x);")
	assertions.Equal(float64(10), res)
}

func TestScripting_V8_Expressions_With_Params(t *testing.T) {
	assertions := require.New(t)
	si := GetScriptInstance("v8", "x + y ;")
	res, _ := si.ScriptEval(map[string]interface{}{"x": 32, "y": 10})
	assertions.True(42 == res.(float64))
}
