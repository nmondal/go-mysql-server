package aggregation

import "C"
import (
	"fmt"
	"github.com/src-d/go-mysql-server/sql"
	"strings"
)

// Fold node to count how many rows are in the result set.
type Fold struct {
	args    []sql.Expression
	initial interface{}
}

func (f *Fold) Children() []sql.Expression {
	return f.args
}

// NewFoldString creates a new Fold String node.
func NewFoldString(args ...sql.Expression) (sql.Expression, error) {
	return &Fold{args: args, initial: ""}, nil
}

// NewFoldDouble creates a new Fold Double node.
func NewFoldDouble(args ...sql.Expression) (sql.Expression, error) {
	return &Fold{args: args, initial: 0.0}, nil
}

// NewFoldInt creates a new Fold Integer node.
func NewFoldInt(args ...sql.Expression) (sql.Expression, error) {
	return &Fold{args: args, initial: 0}, nil
}

// NewFoldList creates a new Fold List node.
func NewFoldList(args ...sql.Expression) (sql.Expression, error) {
	return &Fold{args: args, initial: make([]interface{}, 0)}, nil
}

// NewFoldSet creates a new Fold Set node.
func NewFoldSet(args ...sql.Expression) (sql.Expression, error) {
	return &Fold{args: args, initial: make(map[interface{}]bool)}, nil
}

// NewBuffer creates a new buffer for the aggregation.
func (f *Fold) NewBuffer() sql.Row {
	return sql.NewRow(f.initial)
}

// Type returns the type of the result.
func (f *Fold) Type() sql.Type {
	return sql.JSON
}

// IsNullable returns whether the return value can be null.
func (f *Fold) IsNullable() bool {
	return false
}

// Resolved implements the Expression interface.
func (f *Fold) Resolved() bool {
	for _, arg := range f.args {
		if !arg.Resolved() {
			return false
		}
	}
	return true
}

func (f *Fold) String() string {
	var args = make([]string, len(f.args))
	for i, arg := range f.args {
		args[i] = arg.String()
	}
	typeName := ""
	switch v := f.initial.(type) {
	default:
		fmt.Printf("unexpected type %T \n", v)
	case int64:
		typeName = "i"
	case float64:
		typeName = "f"
	case string:
		typeName = "s"
	case []interface{}:
		typeName = "a"
	case map[interface{}]bool:
		typeName = "m"
	}
	return fmt.Sprintf("fold_%s(%s)", typeName, strings.Join(args, ", "))
}

// WithChildren implements the Expression interface.
func (f *Fold) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(f, len(children), 1)
	}
	return &Fold{args: children, initial: f.initial}, nil
}

func (f *Fold) accumulate(ctx *sql.Context, buffer, row sql.Row) error {
	switch v := f.initial.(type) {
	case int64:
		buffer[0] = buffer[0].(int64) + row[0].(int64)
	case float64:
		buffer[0] = buffer[0].(float64) + row[0].(float64)
	case string:
		buffer[0] = buffer[0].(string) + row[0].(string)
	case []interface{}:
		arr := buffer[0].([]interface{})
		arr = append(arr, row[0])
		buffer[0] = arr
	case map[interface{}]bool:
		dataMap := buffer[0].(map[interface{}]bool)
		dataMap[row[0]] = true
		buffer[0] = dataMap
	default:
		fmt.Printf("unexpected type %T \n", v)
	}
	return nil
}

// Update implements the Aggregation interface.
func (f *Fold) Update(ctx *sql.Context, buffer, row sql.Row) error {
	return f.accumulate(ctx, buffer, row)
}

// Merge implements the Aggregation interface.
func (f *Fold) Merge(ctx *sql.Context, buffer, partial sql.Row) error {
	return f.accumulate(ctx, buffer, partial)
}

// Eval implements the Aggregation interface.
func (f *Fold) Eval(ctx *sql.Context, buffer sql.Row) (interface{}, error) {
	accValue := buffer[0]
	switch f.initial.(type) {
	case map[interface{}]bool:
		dataMap := accValue.(map[interface{}]bool)
		retVal := make([]interface{}, len(dataMap))
		k := 0
		for n := range dataMap {
			retVal[k] = n
			k++
		}
		return retVal, nil
	default:
		return accValue, nil
	}
}
