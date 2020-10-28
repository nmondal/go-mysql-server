package udf

import (
	"errors"
	"fmt"
	"github.com/src-d/go-mysql-server/sql"
	"reflect"
	"regexp"
	"strings"
)

type AggregatorTypeOfUDF int

const (
	NotAnAggregator   AggregatorTypeOfUDF = 0
	ListAggregator    AggregatorTypeOfUDF = 1
	SetAggregator     AggregatorTypeOfUDF = 2
	GenericAggregator AggregatorTypeOfUDF = 3
)

type TypeOfUDF struct {
	IsAggregator   bool
	AggregatorType AggregatorTypeOfUDF
	Flatten        bool
	Transpose      bool
}

type ScriptUDF struct {
	Id      string
	Script  ScriptInstance
	initial interface{}
	UdfType TypeOfUDF
}

type Scriptable struct {
	Meta *ScriptUDF
	args []sql.Expression
}

var AggregatorRegex = regexp.MustCompile(`^<\?([LS][F_][T_])|(AG[GT])@.+`)

// within UDF this does parameter extraction
var ParamRegex = regexp.MustCompile(`@{([^(@{)^}]+)}`)

/**
Let's define the protocol : Here
L__ -> List, no flatten, no transpose
L_T -> List, no flatten, Transpose
LFT -> List, Flatten, Transpose

S__ -> Set, no flatten, no transpose
S_T -> Set, no flatten, Transpose
SFT -> Set, Flatten, Transpose

--> AGG can not have flatten it is upto author
AGG -> Aggregate, no flatten, no transpose
AGT -> AGG, no flatten, Transpose
*/
func AggregatorType(macroStart string) (interface{}, TypeOfUDF) {
	typeOfUDF := TypeOfUDF{IsAggregator: false, AggregatorType: NotAnAggregator, Flatten: false, Transpose: false}
	if !AggregatorRegex.MatchString(macroStart) {
		return nil, typeOfUDF
	}
	typeOfUDF.IsAggregator = true

	identifier := macroStart[2:5]

	if identifier[1] == 'F' {
		typeOfUDF.Flatten = true
	}

	if identifier[2] == 'T' {
		typeOfUDF.Transpose = true
	}

	switch identifier[0] {
	case 'L':
		typeOfUDF.AggregatorType = ListAggregator
		return make([]interface{}, 0), typeOfUDF
	case 'S':
		typeOfUDF.AggregatorType = SetAggregator
		return make(map[interface{}]bool), typeOfUDF
	case 'A':
		typeOfUDF.AggregatorType = GenericAggregator
		i := strings.Index(macroStart, "#")
		return macroStart[5 : i+1], typeOfUDF
	default:
		typeOfUDF.IsAggregator = false
		typeOfUDF.AggregatorType = NotAnAggregator
		// should not come here
	}
	return nil, typeOfUDF
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

func MacroProcessor(query string, funcNumStart int, langDialect string) (string, []ScriptUDF) {
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
			if udfType.AggregatorType == GenericAggregator {
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
		udfArray[i] = ScriptUDF{Id: udfName, Script: GetScriptInstance(langDialect, expr),
			initial: initialAggregatorValue, UdfType: udfType}
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
	env["$ROW"] = row
	env["$CONTEXT"] = ctx
	env["$ARGS"] = myArgs
	if partial != nil {
		env["$_"] = partial
	}
	value, err := a.Meta.Script.ScriptEval(env)
	if err != nil {
		return nil, err
	}
	return value, nil
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
	if a.Meta.UdfType.AggregatorType == SetAggregator {
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

// NewBuffer implements AggregationExpression interface. (AggregationExpression)
func (a *Scriptable) NewBuffer() sql.Row {
	if a.Meta.UdfType.AggregatorType == GenericAggregator {
		initExpr := a.Meta.initial.(string)
		initExpr = initExpr[1 : len(initExpr)-1]
		value, err := a.Meta.Script.EvalFromString(initExpr)
		if err != nil {
			fmt.Printf("Invalid Expression for Aggregate query '%s' \n", initExpr)
		} else {
			a.Meta.initial = value
		}
	}
	return sql.NewRow(a.Meta.initial)
}

func (a *Scriptable) accumulate(ctx *sql.Context, buffer sql.Row, row sql.Row) error {
	res, e := a.getData(ctx, buffer, row)
	if e != nil {
		return e
	}
	switch a.Meta.UdfType.AggregatorType {
	case ListAggregator:
		arr := buffer[0].([]interface{})
		for _, v := range res {
			arr = append(arr, v)
		}
		buffer[0] = arr
	case SetAggregator:
		dataMap := buffer[0].(map[interface{}]bool)
		for _, v := range res {
			dataMap[v] = true
		}
		buffer[0] = dataMap
	default:
		buffer[0] = res[0]
	}
	return nil
}

// Evaluates script and flattens the data if the Flatten flag is set
func (a *Scriptable) getData(ctx *sql.Context, buffer sql.Row, row sql.Row) ([]interface{}, error) {
	res, e := a.EvalScript(ctx, row, buffer[0])
	if e != nil {
		return nil, e
	}
	var data []interface{}
	if a.Meta.UdfType.Flatten && res != nil {
		switch val := reflect.ValueOf(res); val.Type().Kind() {
		case reflect.Slice:
			for i := 0; i < val.Len(); i++ {
				v := val.Index(i)
				data = append(data, v.Interface())
			}
		default:
			data = append(data, res)
		}
	} else {
		data = append(data, res)
	}
	return data, nil
}

// Update implements AggregationExpression interface. (AggregationExpression)
func (a *Scriptable) Update(ctx *sql.Context, buffer, row sql.Row) error {
	return a.accumulate(ctx, buffer, row)
}

// Merge implements AggregationExpression interface. (AggregationExpression)
func (a *Scriptable) Merge(ctx *sql.Context, buffer, partial sql.Row) error {
	return a.accumulate(ctx, buffer, partial)
}
