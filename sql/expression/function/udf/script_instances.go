package udf

import (
	"github.com/antonmedv/expr"
	"github.com/antonmedv/expr/vm"
	"github.com/dop251/goja"
)

type ScriptInstance interface {
	Dialect() string
	Body() string
	EvalFromString(expressionString string) (interface{}, error)
	ScriptEval(scriptEnvironment map[string]interface{}) (
		interface{},
		error,
	)
}

type ExprScriptInstance struct {
	body    string
	program *vm.Program
}

func (exprInstance *ExprScriptInstance) EvalFromString(expressionString string) (interface{}, error) {
	return expr.Eval(expressionString, map[string]interface{}{})
}

func (exprInstance *ExprScriptInstance) ScriptEval(scriptEnvironment map[string]interface{}) (interface{}, error) {
	if exprInstance.program == nil {
		p, e := expr.Compile(exprInstance.body)
		if e != nil {
			return nil, e
		}
		exprInstance.program = p
	}
	return expr.Run(exprInstance.program, scriptEnvironment)
}

func (exprInstance *ExprScriptInstance) Dialect() string { return "expr" }

func (exprInstance *ExprScriptInstance) Body() string { return exprInstance.body }

type JSScriptInstance struct {
	body     string
	runtTime *goja.Runtime
	program  *goja.Program
}

func (jsScriptInstance *JSScriptInstance) EvalFromString(expressionString string) (interface{}, error) {
	value, e := jsScriptInstance.runtTime.RunString(expressionString)
	if e != nil {
		return nil, e
	}
	return value.Export(), nil
}

func (jsScriptInstance *JSScriptInstance) ScriptEval(scriptEnvironment map[string]interface{}) (interface{}, error) {
	if jsScriptInstance.program == nil {
		p, e := goja.Compile("_js_", jsScriptInstance.body, false)
		if e != nil {
			return nil, e
		}
		jsScriptInstance.program = p
	}
	// setup the params ???
	for name := range scriptEnvironment {
		jsScriptInstance.runtTime.Set(name, scriptEnvironment[name])
	}
	value, e := jsScriptInstance.runtTime.RunProgram(jsScriptInstance.program)
	if e != nil {
		return nil, e
	}
	return value.Export(), nil
}

func (jsScriptInstance *JSScriptInstance) Dialect() string { return "ECMAScript5.1" }

func (jsScriptInstance *JSScriptInstance) Body() string { return jsScriptInstance.body }

func GetScriptInstance(langString string, bodyString string) ScriptInstance {
	switch langString {
	case "expr":
		return &ExprScriptInstance{body: bodyString}
	default:
		return &JSScriptInstance{runtTime: goja.New(), body: bodyString}
	}
}
