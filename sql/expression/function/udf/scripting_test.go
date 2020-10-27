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
	tq, udfs := MacroProcessor(s, 0, "")
	assertions.Equal(1, len(udfs))
	assertions.NotEqual(s, tq)
	assertions.False(udfs[0].UdfType.IsAggregator)
	// 2 match
	s = "SELECT  <? @{mytable.phone_numbers}.length ?> ,  <? @{mytable.address}.firstLine ?> FROM mytable;"
	tq, udfs = MacroProcessor(s, 0, "")
	assertions.Equal(2, len(udfs))
	assertions.NotEqual(s, tq)
	// 3 match
	s = "SELECT  <? @{mytable.phone_numbers}.length ?> ,  <? @{mytable.address}.firstLine ?> , <? @{mytable.x} ?> FROM mytable;"
	tq, udfs = MacroProcessor(s, 0, "")
	assertions.Equal(3, len(udfs))
	assertions.NotEqual(s, tq)
	// no match
	s = "SELECT mytable.name FROM mytable;"
	tq, udfs = MacroProcessor(s, 0, "")
	assertions.Equal(0, len(udfs))
	assertions.Equal(s, tq)
	// issue found by Sandy
	s = "SELECT  <? x = @{mytable.phone_numbers}; y = []; y.concat(x); y ?> FROM mytable"
	tq, udfs = MacroProcessor(s, 0, "")
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
	tq, udfs := MacroProcessor(s, 0, "")
	assertions.Equal(1, len(udfs))
	assertions.NotEqual(s, tq)
	assertions.Equal(lt, reflect.TypeOf(udfs[0].initial))
	assertions.Equal(0, len(udfs[0].initial.([]interface{})))
	assertions.Equal(ListAggregator, udfs[0].UdfType.AggregatorType)
	// set
	s = "SELECT  <?S__@ @{mytable.phone_numbers}.length ?> FROM mytable;"
	tq, udfs = MacroProcessor(s, 0, "")
	assertions.Equal(1, len(udfs))
	assertions.NotEqual(s, tq)
	assertions.Equal(st, reflect.TypeOf(udfs[0].initial))
	assertions.Equal(0, len(udfs[0].initial.(map[interface{}]bool)))
	assertions.Equal(SetAggregator, udfs[0].UdfType.AggregatorType)
}

func TestMacroProcessor_Agg_Generic(t *testing.T) {
	assertions := require.New(t)
	// list
	s := "SELECT  <?AGG@ 42 # @{mytable.phone_numbers}.length ?> FROM mytable;"
	tq, udfs := MacroProcessor(s, 0, "")
	assertions.Equal(1, len(udfs))
	assertions.NotEqual(s, tq)
	assertions.NotEmpty(udfs[0].initial.(string))
	assertions.Equal(GenericAggregator, udfs[0].UdfType.AggregatorType)
}

func TestMacroProcessor_Agg_Pivot_Generic(t *testing.T) {
	assertions := require.New(t)
	// list
	s := "SELECT  <?AGT@ [] # l = @{mytable.phone_numbers}.length; $_ = $_.concat() ?> FROM mytable;"
	tq, udfs := MacroProcessor(s, 0, "")
	assertions.Equal(1, len(udfs))
	assertions.NotEqual(s, tq)
	assertions.NotEmpty(udfs[0].initial.(string))
	assertions.Equal(GenericAggregator, udfs[0].UdfType.AggregatorType)
	assertions.True(udfs[0].UdfType.Transpose)

}
