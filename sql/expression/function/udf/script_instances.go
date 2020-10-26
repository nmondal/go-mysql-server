package udf

import (
	"fmt"
	"github.com/antonmedv/expr"
	"github.com/antonmedv/expr/vm"
	"github.com/dop251/goja"
	"rogchap.com/v8go"
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

type V8EcmaScript6 struct {
	body     string
	ctx      *v8go.Context
	runtTime *goja.Runtime
	fromV8   *goja.Program
	toV8     *goja.Program
}

func (v8Instance *V8EcmaScript6) EvalFromString(expressionString string) (interface{}, error) {
	value, e := v8Instance.ctx.RunScript(expressionString, "")
	if e != nil {
		return nil, e
	}
	if v8Instance.fromV8 == nil {
		p, _ := goja.Compile("_v8_", "JSON.parse(_input_);", false)
		v8Instance.fromV8 = p
	}
	v8Instance.runtTime.Set("_input_", value.String())
	v, e := v8Instance.runtTime.RunProgram(v8Instance.fromV8)
	if e != nil {
		return nil, e
	}
	return v.Export(), nil
}

func (v8Instance *V8EcmaScript6) ScriptEval(scriptEnvironment map[string]interface{}) (interface{}, error) {
	if v8Instance.toV8 == nil {
		p, _ := goja.Compile("_v8_", "JSON.parse(_input_);", false)
		v8Instance.toV8 = p
	}
	// create the marshalling mechanism ...
	finalScript := "function __anon__(){ "
	for name := range scriptEnvironment {
		v8Instance.runtTime.Set("_input_", scriptEnvironment[name])
		v, e := v8Instance.runtTime.RunProgram(v8Instance.toV8)
		if e != nil {
			return nil, e
		}
		finalScript = fmt.Sprintf("%s\nlet %s = %s; ", finalScript, name, v)
	}
	finalScript = fmt.Sprintf("%s\n return %s \n} __anon__();", finalScript, v8Instance.body)
	return v8Instance.EvalFromString(finalScript)
}

func (v8Instance *V8EcmaScript6) Dialect() string { return "V8EcmaScript6" }

func (v8Instance *V8EcmaScript6) Body() string { return v8Instance.body }

func GetScriptInstance(langString string, bodyString string) ScriptInstance {
	switch langString {
	case "expr":
		return &ExprScriptInstance{body: bodyString}
	case "v8":
		ctx, _ := v8go.NewContext(nil)
		return &V8EcmaScript6{ctx: ctx, body: bodyString, runtTime: goja.New()}
	default:
		return &JSScriptInstance{runtTime: goja.New(), body: bodyString}
	}
}
