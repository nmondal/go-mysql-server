package udf

import (
	"github.com/antonmedv/expr"
	"github.com/antonmedv/expr/vm"
	"github.com/robertkrimen/otto"
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
	runtTime *otto.Otto
	script   *otto.Script
}

func (jsScriptInstance *JSScriptInstance) EvalFromString(expressionString string) (interface{}, error) {
	value, e := jsScriptInstance.runtTime.Run(expressionString)
	if e != nil {
		return nil, e
	}
	exportedValue, _ := value.Export()
	return exportedValue, nil
}

func (jsScriptInstance *JSScriptInstance) ScriptEval(scriptEnvironment map[string]interface{}) (interface{}, error) {
	if jsScriptInstance.script == nil {
		s, e := jsScriptInstance.runtTime.Compile("_js_", jsScriptInstance.body)
		if e != nil {
			return nil, e
		}
		jsScriptInstance.script = s
	}
	// setup the params ???
	for name := range scriptEnvironment {
		jsScriptInstance.runtTime.Set(name, scriptEnvironment[name])
	}
	value, e := jsScriptInstance.runtTime.Run(jsScriptInstance.script)
	if e != nil {
		return nil, e
	}
	exportedValue, _ := value.Export()
	return exportedValue, nil
}

func (jsScriptInstance *JSScriptInstance) Dialect() string { return "ECMAScript5.1" }

func (jsScriptInstance *JSScriptInstance) Body() string { return jsScriptInstance.body }


/**
This is for integrating the v8 engine, but it is currently causing problems. Hence removing it. But we can explore this
at a later time.
**

type V8EcmaScript6 struct {
	body string
	ctx  *v8go.Context
}

func (v8Instance *V8EcmaScript6) EvalFromString(expressionString string) (interface{}, error) {
	value, e := v8Instance.ctx.RunScript(expressionString, "")
	if e != nil {
		return nil, e
	}
	var finalVal interface{}
	e = json.Unmarshal([]byte(value.String()), &finalVal)
	if e != nil {
		return nil, e
	}
	return finalVal, nil
}

func (v8Instance *V8EcmaScript6) ScriptEval(scriptEnvironment map[string]interface{}) (interface{}, error) {

	// create the marshalling mechanism ...
	finalScript := "function __anon__(){ "
	for name := range scriptEnvironment {
		v, e := json.Marshal(scriptEnvironment[name])
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
*/

func GetScriptInstance(langString string, bodyString string) ScriptInstance {
	switch langString {
	case "expr":
		return &ExprScriptInstance{body: bodyString}
	//case "v8":
	//	ctx, _ := v8go.NewContext(nil)
	//	return &V8EcmaScript6{ctx: ctx, body: bodyString}
	default:
		return &JSScriptInstance{runtTime: otto.New(), body: bodyString}
	}
}
