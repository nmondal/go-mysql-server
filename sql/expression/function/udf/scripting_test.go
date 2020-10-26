package udf

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMacroProcessor_NormalUDFs(t *testing.T) {
	assertions := require.New(t)
	// 1 match
	s := "SELECT  <? @{mytable.phone_numbers}.length ?> FROM mytable;"
	tq, udfs := MacroProcessor(s, 0)
	assertions.Equal(1, len(udfs))
	assertions.NotEqual(s, tq)
	assertions.False(udfs[0].udfType.IsAggregator)
	// 2 match
	s = "SELECT  <? @{mytable.phone_numbers}.length ?> ,  <? @{mytable.address}.firstLine ?> FROM mytable;"
	tq, udfs = MacroProcessor(s, 0)
	assertions.Equal(2, len(udfs))
	assertions.NotEqual(s, tq)
	// 3 match
	s = "SELECT  <? @{mytable.phone_numbers}.length ?> ,  <? @{mytable.address}.firstLine ?> , <? @{mytable.x} ?> FROM mytable;"
	tq, udfs = MacroProcessor(s, 0)
	assertions.Equal(3, len(udfs))
	assertions.NotEqual(s, tq)
	// no match
	s = "SELECT mytable.name FROM mytable;"
	tq, udfs = MacroProcessor(s, 0)
	assertions.Equal(0, len(udfs))
	assertions.Equal(s, tq)
	// issue found by Sandy
	s = "SELECT  <? x = @{mytable.phone_numbers}; y = []; y.concat(x); y ?> FROM mytable"
	tq, udfs = MacroProcessor(s, 0)
	assertions.Equal(1, len(udfs))
	assertions.NotEqual(s, tq)
}

func TestMacroProcessor_Agg_LST_SET(t *testing.T) {
	// list type
	lt := reflect.TypeOf(make([]interface{}, 0))
	// set type
	st := reflect.TypeOf(make(map[interface{}]bool))
	assertions := require.New(t)
	// list
	s := "SELECT  <?L__@ @{mytable.phone_numbers}.length ?> FROM mytable;"
	tq, udfs := MacroProcessor(s, 0)
	assertions.Equal(1, len(udfs))
	assertions.NotEqual(s, tq)
	assertions.Equal(lt, reflect.TypeOf(udfs[0].initial))
	assertions.Equal(0, len(udfs[0].initial.([]interface{})))
	assertions.Equal(ListAggregator, udfs[0].udfType.AggregatorType)
	// set
	s = "SELECT  <?S__@ @{mytable.phone_numbers}.length ?> FROM mytable;"
	tq, udfs = MacroProcessor(s, 0)
	assertions.Equal(1, len(udfs))
	assertions.NotEqual(s, tq)
	assertions.Equal(st, reflect.TypeOf(udfs[0].initial))
	assertions.Equal(0, len(udfs[0].initial.(map[interface{}]bool)))
	assertions.Equal(SetAggregator, udfs[0].udfType.AggregatorType)
}

func TestMacroProcessor_Agg_Generic(t *testing.T) {
	assertions := require.New(t)
	// list
	s := "SELECT  <?AGG@ 42 # @{mytable.phone_numbers}.length ?> FROM mytable;"
	tq, udfs := MacroProcessor(s, 0)
	assertions.Equal(1, len(udfs))
	assertions.NotEqual(s, tq)
	assertions.NotEmpty(udfs[0].initial.(string))
	assertions.Equal(GenericAggregator, udfs[0].udfType.AggregatorType)
}

func TestMacroProcessor_Agg_Pivot_Generic(t *testing.T) {
	assertions := require.New(t)
	// list
	s := "SELECT  <?AGT@ [] # l = @{mytable.phone_numbers}.length; $_ = $_.concat() ?> FROM mytable;"
	tq, udfs := MacroProcessor(s, 0)
	assertions.Equal(1, len(udfs))
	assertions.NotEqual(s, tq)
	assertions.NotEmpty(udfs[0].initial.(string))
	assertions.Equal(GenericAggregator, udfs[0].udfType.AggregatorType)
	assertions.True(udfs[0].udfType.Transpose)

}

func TestScripting_JS_Expressions_No_Params(t *testing.T) {
	assertions := require.New(t)
	udf := &ScriptUDF{Id: "dummy", Lang: "js", Body: "42", initial: nil}
	s := Scriptable{Meta: udf, args: nil}
	res, _ := s.EvalScript(nil, nil, nil)
	// this is for int
	assertions.True(42 == res.(int64))
	// now double
	s.Meta.Body = "42.23"
	res, _ = s.EvalScript(nil, nil, nil)
	assertions.True(42.23 == res.(float64))
	// now string
	s.Meta.Body = "x='42';"
	res, _ = s.EvalScript(nil, nil, nil)
	assertions.True("42" == res.(string))
	// list of primitives
	s.Meta.Body = " x =[ 42, 42, 42 ] ;"
	res, _ = s.EvalScript(nil, nil, nil)
	assertions.Equal(3, len(res.([]interface{})))
	// a map ?
	s.Meta.Body = " x = { 'i' : 42 }  ;"
	res, _ = s.EvalScript(nil, nil, nil)
	assertions.Equal(1, len(res.(map[string]interface{})))

}

func TestScripting_EXPR_EVAL_Expressions_No_Params(t *testing.T) {
	assertions := require.New(t)
	udf := &ScriptUDF{Id: "dummy", Lang: "expr", Body: "42", initial: nil}
	s := Scriptable{Meta: udf, args: nil}
	res, _ := s.EvalScript(nil, nil, nil)
	// this is for int
	assertions.True(42 == res)
}
