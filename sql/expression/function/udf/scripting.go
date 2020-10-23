package udf

import (
	"errors"
	"fmt"
	exprEval "github.com/antonmedv/expr"
	"github.com/robertkrimen/otto"
	"github.com/src-d/go-mysql-server/sql"
	"regexp"
	"strings"
)

type TypeOfUDF int

const (
	Normal            = 0
	ListAggregator    = 1
	SetAggregator     = 2
	GenericAggregator = 3
)

type ScriptUDF struct {
	Id      string
	Lang    string
	Body    string
	initial interface{}
	udfType TypeOfUDF
}

type Scriptable struct {
	Meta *ScriptUDF
	args []sql.Expression
}

// within UDF this does parameter extraction
var ParamRegex = regexp.MustCompile(`@{([^(@{)^}]+)}`)

func AggregatorType(macroStart string) (interface{}, TypeOfUDF) {
	if strings.HasPrefix(macroStart, "<?LST@") {
		return make([]interface{}, 0), ListAggregator
	}
	if strings.HasPrefix(macroStart, "<?SET@") {
		return make(map[interface{}]bool), SetAggregator
	}
	if strings.HasPrefix(macroStart, "<?AGG@") {
		i := strings.Index(macroStart, "#")
		return macroStart[5 : i+1], GenericAggregator
	}
	return nil, Normal
}

// Extract out macros, failed with regex. TODO Sandy to find out better alternatives
func FindAllUDFStrings(query string) ([][]string, error) {
	var myRet = make([][]string, 0)
	buf := query
	start := strings.Index(buf, "<?")
	end := start
	for start >= 0 {
		end = strings.Index(buf, "?>")
		if end < start {
			return nil, errors.New(fmt.Sprintf("Invalid Macro!!! <?%d : %d?> !!!", start, end))
		}
		values := make([]string, 2)
		values[0] = buf[start : end+2]
		values[1] = values[0][2 : len(values[0])-2]
		myRet = append(myRet, values)
		buf = buf[end+2:]
		start = strings.Index(buf, "<?")
	}
	return myRet, nil
}

func MacroProcessor(query string, funcNumStart int) (string, []ScriptUDF) {
	list, _ := FindAllUDFStrings(query)
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
		initialAggregatorValue, udfType := AggregatorType(actual)
		if initialAggregatorValue != nil {
			prefixInx := 4
			if udfType == GenericAggregator {
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
		udfArray[i] = ScriptUDF{Id: udfName, Lang: "js", Body: expr, initial: initialAggregatorValue, udfType: udfType}
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

func (a *Scriptable) EvalScript(ctx *sql.Context, row sql.Row, partial interface{}) (interface{}, error) {
	switch a.Meta.Lang {
	case "expr":
		return a.__expr(ctx, row, partial)
	default:
		return a.__js(ctx, row, partial)
	}
}

func (a *Scriptable) __createArgs(ctx *sql.Context, row sql.Row) ([]interface{}, map[string]map[string]interface{}, error) {
	myArgs := make([]interface{}, len(a.args))
	params := make(map[string]map[string]interface{})
	for i := 0; i < len(a.args); i++ {
		o, e := a.args[i].Eval(ctx, row)
		if e != nil {
			return nil, nil, e
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
	return myArgs, params, nil
}

func (a *Scriptable) __js(ctx *sql.Context, row sql.Row, partial interface{}) (interface{}, error) {
	myArgs, params, e := a.__createArgs(ctx, row)
	if e != nil {
		return nil, e
	}
	vm := otto.New()
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

func (a *Scriptable) __expr(ctx *sql.Context, row sql.Row, partial interface{}) (interface{}, error) {
	myArgs, params, e := a.__createArgs(ctx, row)
	if e != nil {
		return nil, e
	}
	// setup the environment
	env := make(map[string]interface{})
	// put named parameters back
	for k, v := range params {
		env[k] = v
	}
	// rest of the world
	env["_ROW"] = row
	env["_CONTEXT"] = ctx
	env["_A"] = myArgs
	if partial != nil {
		env["_p"] = partial
	}
	value, err := exprEval.Eval(a.Meta.Body, env)
	if err != nil {
		return nil, err
	}
	return value, nil
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
		return a.EvalScript(ctx, buffer, nil)
	}
	// now aggregated ....
	if a.Meta.udfType == TypeOfUDF(SetAggregator) {
		dataMap := buffer[0].(map[interface{}]bool)
		retList := make([]interface{}, len(dataMap))
		k := 0
		for v := range dataMap {
			retList[k] = v
			k++
		}
		return retList, nil
	} else {
		return buffer[0], nil
	}
}

// WithChildren implements the Expression interface.
func (a *Scriptable) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return &Scriptable{args: children, Meta: a.Meta}, nil
}

func (a *Scriptable) __initExpr(initExpr string) (interface{}, error) {
	switch a.Meta.Lang {
	case "expr":
		return exprEval.Eval(initExpr, map[string]interface{}{})
	default:
		jsVM := otto.New()
		value, e := jsVM.Run(initExpr)
		if e != nil {
			return nil, e
		}
		v, _ := value.Export()
		return v, nil
	}
}

// NewBuffer implements AggregationExpression interface. (AggregationExpression)
func (a *Scriptable) NewBuffer() sql.Row {
	if a.Meta.udfType == TypeOfUDF(GenericAggregator) {
		initExpr := a.Meta.initial.(string)
		initExpr = initExpr[1 : len(initExpr)-1]
		value, err := a.__initExpr(initExpr)
		if err != nil {
			fmt.Printf("Invalid Expression for Aggregate query '%s' \n", initExpr)
		} else {
			a.Meta.initial = value
		}
	}
	return sql.NewRow(a.Meta.initial)
}

func (a *Scriptable) accumulate(ctx *sql.Context, buffer sql.Row, row sql.Row) error {
	res, e := a.EvalScript(ctx, row, buffer[0])
	if e != nil {
		return e
	}
	switch a.Meta.udfType {
	case ListAggregator:
		arr := buffer[0].([]interface{})
		arr = append(arr, res)
		buffer[0] = arr
	case SetAggregator:
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
