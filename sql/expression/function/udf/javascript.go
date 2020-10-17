package udf

import (
	"fmt"
	"github.com/robertkrimen/otto"
	"github.com/src-d/go-mysql-server/sql"
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

func (s *ScriptUDF) Fn(args ...sql.Expression) (sql.Expression, error) {
	return &Scriptable{s, args}, nil
}

func (s *ScriptUDF) AsFunction() sql.FunctionN {
	return sql.FunctionN{Name: strings.ToLower(s.Id), Fn: s.Fn}
}

func (a *Scriptable) Children() []sql.Expression {
	return a.args
}

func (a *Scriptable) JSRowEval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	myArgs := make([]interface{}, len(a.args))
	for i := 0; i < len(a.args); i++ {
		o, e := a.args[i].Eval(ctx, row)
		if e != nil {
			return nil, e
		}
		myArgs[i] = o
	}

	vm := otto.New()
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
