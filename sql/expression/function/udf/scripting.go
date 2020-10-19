package udf

import (
	"fmt"
	"github.com/robertkrimen/otto"
	"github.com/src-d/go-mysql-server/sql"
	"regexp"
	"strings"
)

type ScriptUDF struct {
	Id   string
	Lang string
	Body string
}

type Scriptable struct {
	Meta *ScriptUDF
	args []sql.Expression
}

// This finds the match for where there is an UDF to be created
var UdfRegex = regexp.MustCompile(`<\?([^(<\?)].+[^(>\?)])+\?>`)

// within UDF this does parameter extraction
var ParamRegex = regexp.MustCompile(`@{([^(@{)^}]+)}`)

func MacroProcessor(query string) (string, []ScriptUDF) {
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
		myParams := ParamRegex.FindAllStringSubmatch(expr, -1)
		paramNameMap := make(map[string]bool)
		for j := 0; j < len(myParams); j++ {
			matchString := myParams[j][0]
			paramString := myParams[j][1]
			paramNameMap[paramString] = true
			expr = strings.Replace(expr, matchString, paramString, 1)
		}
		udfName := fmt.Sprintf("_auto_%d_udf_", i+1)
		paramNames := make([]string, len(paramNameMap))
		k := 0
		for n := range paramNameMap {
			paramNames[k] = n
			k++
		}
		udfCall := fmt.Sprintf("%s(%s)", udfName, strings.Join(paramNames, ","))
		udfArray[i] = ScriptUDF{Id: udfName, Lang: "js", Body: expr}
		retString = strings.Replace(retString, actual, udfCall, 1)
	}
	return retString, udfArray
}

func (s *ScriptUDF) Fn(args ...sql.Expression) (sql.Expression, error) {
	return &Scriptable{s, args}, nil
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

func (a *Scriptable) JSRowEval(ctx *sql.Context, row sql.Row) (interface{}, error) {
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
	value, err := vm.Run(a.Meta.Body)
	return value, err
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
	return a.JSRowEval(ctx, buffer)
}

// WithChildren implements the Expression interface.
func (a *Scriptable) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return &Scriptable{args: children, Meta: a.Meta}, nil
}

// NewBuffer implements AggregationExpression interface. (AggregationExpression)
func (a *Scriptable) NewBuffer() sql.Row {
	contextMap := make(map[string]interface{})
	return sql.NewRow(contextMap)
}

// Update implements AggregationExpression interface. (AggregationExpression)
func (a *Scriptable) Update(ctx *sql.Context, buffer, row sql.Row) error {

	return nil
}

// Merge implements AggregationExpression interface. (AggregationExpression)
func (a *Scriptable) Merge(ctx *sql.Context, buffer, partial sql.Row) error {
	return nil
}
