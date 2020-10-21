package udf

import (
	"fmt"
	"github.com/robertkrimen/otto"
	"github.com/src-d/go-mysql-server/sql"
	"regexp"
	"strings"
)

type ScriptUDF struct {
	Id      string
	Lang    string
	Body    string
	initial interface{}
}

type Scriptable struct {
	Meta *ScriptUDF
	args []sql.Expression
}

// This finds the match for where there is an UDF to be created
var UdfRegex = regexp.MustCompile("<\\?([^(<\\?)^(\\?>)]+)\\?>")

// DO NOT EVER CHANGE :: There are test cases (TestMacroProcessor_NormalUDF_2) written on top of it

// within UDF this does parameter extraction
var ParamRegex = regexp.MustCompile(`@{([^(@{)^}]+)}`)

func AggregatorType(macroStart string) interface{} {
	if strings.HasPrefix(macroStart, "<?LST@") {
		return make([]interface{}, 0)
	}
	if strings.HasPrefix(macroStart, "<?SET@") {
		return make(map[interface{}]bool)
	}
	if strings.HasPrefix(macroStart, "<?STR@") {
		return ""
	}
	if strings.HasPrefix(macroStart, "<?DBL@") {
		return 0.0
	}
	if strings.HasPrefix(macroStart, "<?INT@") {
		return 0
	}
	if strings.HasPrefix(macroStart, "<?AGG@") {
		i := strings.Index(macroStart, "#")
		return macroStart[5 : i+1]
	}
	return nil
}

func isGenericAggregator(value interface{}) bool {
	switch value.(type) {
	case string:
		expression := value.(string)
		return strings.HasPrefix(expression, "@") && strings.HasSuffix(expression, "#")
	default:
		return false
	}
}

func MacroProcessor(query string, funcNumStart int) (string, []ScriptUDF) {
	list := UdfRegex.FindAllStringSubmatch(query, -1)
	N := len(list)
	if N == 0 {
		return query, nil
	}
	retString := query
	udfArray := make([]ScriptUDF, N)
	for i := 0; i < N; i++ {
		actual := list[i][0]
		expr := list[i][1]
		udfName := fmt.Sprintf("_auto_%d_udf_", i+1+funcNumStart)
		// check if initialAggregatorValue ?
		initialAggregatorValue := AggregatorType(actual)
		if initialAggregatorValue != nil {
			prefixInx := 4
			if isGenericAggregator(initialAggregatorValue) {
				prefixInx += len(initialAggregatorValue.(string)) - 1
			}
			expr = expr[prefixInx:]
			udfName = "fold" + udfName
		}

		myParams := ParamRegex.FindAllStringSubmatch(expr, -1)
		paramNameMap := make(map[string]bool)
		for j := 0; j < len(myParams); j++ {
			matchString := myParams[j][0]
			paramString := myParams[j][1]
			paramNameMap[paramString] = true
			expr = strings.Replace(expr, matchString, paramString, 1)
		}
		paramNames := make([]string, len(paramNameMap))
		k := 0
		for n := range paramNameMap {
			paramNames[k] = n
			k++
		}
		udfCall := fmt.Sprintf("%s(%s)", udfName, strings.Join(paramNames, ","))
		udfArray[i] = ScriptUDF{Id: udfName, Lang: "js", Body: expr, initial: initialAggregatorValue}
		retString = strings.Replace(retString, actual, udfCall, 1)
	}
	return retString, udfArray
}

func (s *ScriptUDF) Fn(args ...sql.Expression) (sql.Expression, error) {
	return &Scriptable{Meta: s, args: args}, nil
}

func (s *ScriptUDF) AsFunction() sql.FunctionN {
	return sql.FunctionN{Name: strings.ToLower(s.Id), Fn: s.Fn}
}

func (a *Scriptable) Children() []sql.Expression {
	return a.args
}

func CanBeVariableName(s string) (bool, []string) {
	arr := strings.Split(s, ".")
	for i := 0; i < len(arr); i++ {
		r, _ := regexp.MatchString("[a-zA-Z_][a-zA-Z_0-9]*", s)
		if !r {
			return r, nil
		}
	}
	return true, arr
}

func (a *Scriptable) JSRowEval(ctx *sql.Context, row sql.Row, partial interface{}) (interface{}, error) {
	myArgs := make([]interface{}, len(a.args))
	vm := otto.New()
	params := make(map[string]map[string]interface{})
	for i := 0; i < len(a.args); i++ {
		o, e := a.args[i].Eval(ctx, row)
		if e != nil {
			return nil, e
		}
		myArgs[i] = o
		varName := a.args[i].String()
		isVar, paths := CanBeVariableName(varName)
		if isVar { // setup the named parameters
			if val, ok := params[paths[0]]; ok {
				//do something here
				val[paths[1]] = o
			} else {
				co := make(map[string]interface{})
				co[paths[1]] = o
				params[paths[0]] = co
			}
		}
	}
	// put named parameters back
	for k, v := range params {
		_ = vm.Set(k, v)
	}

	// rest of the world
	_ = vm.Set("$ROW", row)
	_ = vm.Set("$CONTEXT", ctx)
	_ = vm.Set("$", myArgs)
	if partial != nil {
		_ = vm.Set("$_", partial)
	}
	value, err := vm.Run(a.Meta.Body)
	if err != nil {
		return nil, err
	}
	exportedValue, _ := value.Export()
	return exportedValue, nil
}

func (a *Scriptable) String() string {
	return fmt.Sprintf("%s(...)", strings.ToLower(a.Meta.Id))
}

// Resolved implements AggregationExpression interface. (AggregationExpression[Expression[Resolvable]]])
func (a *Scriptable) Resolved() bool {
	return true
}

// Type implements AggregationExpression interface. (AggregationExpression[Expression]])
func (a *Scriptable) Type() sql.Type {
	return sql.JSON
}

// IsNullable implements AggregationExpression interface. (AggregationExpression[Expression]])
func (a *Scriptable) IsNullable() bool {
	return true
}

// Eval implements AggregationExpression interface. (AggregationExpression[Expression]])
func (a *Scriptable) Eval(ctx *sql.Context, buffer sql.Row) (interface{}, error) {
	if a.Meta.initial == nil {
		// this is where we are non aggregated
		return a.JSRowEval(ctx, buffer, nil)
	}
	// now aggregated ....
	switch a.Meta.initial.(type) {
	case map[interface{}]bool:
		dataMap := buffer[0].(map[interface{}]bool)
		retList := make([]interface{}, len(dataMap))
		k := 0
		for v := range dataMap {
			retList[k] = v
			k++
		}
		return retList, nil
	default:
		return buffer[0], nil
	}
}

// WithChildren implements the Expression interface.
func (a *Scriptable) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return &Scriptable{args: children, Meta: a.Meta}, nil
}

// NewBuffer implements AggregationExpression interface. (AggregationExpression)
func (a *Scriptable) NewBuffer() sql.Row {
	switch a.Meta.initial.(type) {
	case string:
		if isGenericAggregator(a.Meta.initial) {
			initExpr := a.Meta.initial.(string)
			initExpr = initExpr[1 : len(initExpr)-1]
			vm := otto.New()
			value, err := vm.Run(initExpr)
			if err != nil {
				fmt.Printf("Invalid Expression for Aggregate query '%s' \n", initExpr)
			} else {
				a.Meta.initial, _ = value.Export()
			}
		}
	default:
		// do nothing
	}
	return sql.NewRow(a.Meta.initial)
}

func (a *Scriptable) accumulate(ctx *sql.Context, buffer sql.Row, row sql.Row) error {
	res, e := a.JSRowEval(ctx, row, buffer[0])
	if e != nil {
		return e
	}
	switch a.Meta.initial.(type) {

	case []interface{}:
		arr := buffer[0].([]interface{})
		arr = append(arr, res)
		buffer[0] = arr
	case map[interface{}]bool:
		dataMap := buffer[0].(map[interface{}]bool)
		dataMap[res] = true
		buffer[0] = dataMap
	default:
		buffer[0] = res
	}
	return nil
}

// Update implements AggregationExpression interface. (AggregationExpression)
func (a *Scriptable) Update(ctx *sql.Context, buffer, row sql.Row) error {
	return a.accumulate(ctx, buffer, row)
}

// Merge implements AggregationExpression interface. (AggregationExpression)
func (a *Scriptable) Merge(ctx *sql.Context, buffer, partial sql.Row) error {
	return a.accumulate(ctx, buffer, partial)
}
