package main

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"text/template"
)

type GenSettings struct {
	URL    string
	Method string
	Auth   bool
}

func (s *GenSettings) Validate() error {
	if _, err := url.ParseRequestURI(s.URL); err != nil {
		return err
	}

	if s.Method == "" {
		return nil
	}

	s.Method = strings.ToUpper(s.Method)
	switch s.Method {
	case http.MethodGet, http.MethodPost:
	default:
		return fmt.Errorf("unsupport method %s to api generate", s.Method)
	}

	return nil
}

const (
	labelRequired  = "required"
	labelParamName = "paramname"
	labelEnum      = "enum"
	labelDefault   = "default"
	labelMin       = "min"
	labelMax       = "max"
)

var binaryLabels = map[string]bool{
	labelRequired:  false,
	labelParamName: true,
	labelEnum:      true,
	labelDefault:   true,
	labelMin:       true,
	labelMax:       true,
}

type TagParser struct {
	Raw string

	isRequired bool
	paramName  string
	enum       []string
	defaultVal string
	min        int
	max        int

	hasParamName bool
	hasEnum      bool
	hasDefault   bool
	hasMin       bool
	hasMax       bool
}

func (p *TagParser) Parse() error {
	labels := strings.Split(p.Raw, ",")
	for _, label := range labels {
		if label == "" {
			continue
		}

		parts := strings.SplitN(label, "=", 2)
		key, value := parts[0], ""

		isBinary := binaryLabels[key]
		if isBinary && len(parts) != 2 {
			return fmt.Errorf("label %s must have a value", key)
		}

		if isBinary {
			value = parts[1]
		}

		switch key {
		case labelRequired:
			p.isRequired = true
		case labelParamName:
			p.paramName = value
			p.hasParamName = true
		case labelEnum:
			p.enum = strings.Split(value, "|")
			p.hasEnum = true
		case labelDefault:
			p.defaultVal = value
			p.hasDefault = true
		case labelMin, labelMax:
			val, err := strconv.Atoi(value)
			if err != nil {
				return fmt.Errorf("can't convert to int label value %s", key)
			}

			if key == labelMin {
				p.min = val
				p.hasMin = true
			} else {
				p.max = val
				p.hasMax = true
			}
		}
	}

	return nil
}

func (p *TagParser) IsRequired() bool {
	return p.isRequired
}

func (p *TagParser) ParamName() string {
	return p.paramName
}

func (p *TagParser) Enum() []string {
	return p.enum
}

func (p *TagParser) Default() string {
	return p.defaultVal
}

func (p *TagParser) Min() int {
	return p.min
}

func (p *TagParser) Max() int {
	return p.max
}

func (p *TagParser) HasParamName() bool {
	return p.hasParamName
}

func (p *TagParser) HasEnum() bool {
	return p.hasEnum
}

func (p *TagParser) HasDefault() bool {
	return p.hasDefault
}

func (p *TagParser) HasMin() bool {
	return p.hasMin
}

func (p *TagParser) HasMax() bool {
	return p.hasMax
}

type Renderer interface {
	Render(out io.Writer) error
}

type RendererFunc func(out io.Writer) error

func (f RendererFunc) Renderer(out io.Writer) error {
	return f(out)
}

var funcMap = template.FuncMap{
	"lower": strings.ToLower,
	"sliceToStr": func(sl []string) string {
		return fmt.Sprintf("[%s]", strings.Join(sl, ", "))
	},
}

var (
	fieldSetterStrTmpl = template.Must(template.New("fieldSetterStrTmpl").Funcs(funcMap).Parse(`
	{{.FieldName|lower}}Str := req.FormValue("{{.ParamName}}")
	params.{{.FieldName}} = {{.FieldName|lower}}Str
`))
	fieldSetterIntTmpl = template.Must(template.New("fieldSetterIntTmpl").Funcs(funcMap).Parse(`
	{{.FieldName|lower}}Str := req.FormValue("{{.ParamName}}")
	{{.FieldName|lower}}Int, err := strconv.Atoi({{.FieldName|lower}}Str)
	if err != nil {
		err = fmt.Errorf("{{.FieldName|lower}} must be int")
		if err = respondError(w, ApiError{http.StatusBadRequest, err}); err != nil {
			log.Println(err)
		}
		return
    }
	params.{{.FieldName}} = {{.FieldName|lower}}Int
`))

	defaultFieldSetterStrTmpl = template.Must(template.New("defaultFieldSetterStrTmpl").Parse(`
	if params.{{.FieldName}} == "" {
		params.{{.FieldName}} = "{{.DefaultVal}}"
	}
`))
	defaultFieldSetterIntTmpl = template.Must(template.New("defaultFieldSetterIntTmpl").Parse(`
	if params.{{.FieldName}} == 0 {
		params.{{.FieldName}} = {{.DefaultVal}}
	}
`))

	requiredStrValidatorTmpl = template.Must(template.New("requiredStrValidatorTmpl").Funcs(funcMap).Parse(`
	if params.{{.FieldName}} == "" {
		err := fmt.Errorf("{{.FieldName|lower}} must me not empty")
		if err = respondError(w, ApiError{http.StatusBadRequest, err}); err != nil {
			log.Println(err)
		}
		return
	}
`))
	requiredIntValidatorTmpl = template.Must(template.New("requiredIntValidatorTmpl").Funcs(funcMap).Parse(`
	if params.{{.FieldName}} == 0 {
		err := fmt.Errorf("{{.FieldName|lower}} must me not empty")
		if err = respondError(w, ApiError{http.StatusBadRequest, err}); err != nil {
			log.Println(err)
		}
		return
	}
`))

	enumStrValidatorTmpl = template.Must(template.New("enumStrValidatorTmpl").Funcs(funcMap).Parse(`
	switch params.{{.FieldName}} {
	case {{range $i, $el := .Enum}}{{if (ne $i 0)}}, {{end}}"{{$el}}"{{end}}:
	default:
		err := fmt.Errorf("{{.FieldName|lower}} must be one of {{.Enum|sliceToStr}}")
		if err = respondError(w, ApiError{http.StatusBadRequest, err}); err != nil {
			log.Println(err)
		}
		return
	}
`))
	enumIntValidatorTmpl = template.Must(template.New("enumIntValidatorTmpl").Funcs(funcMap).Parse(`
	switch params.{{.FieldName}} {
	case {{range $i, $el := .Enum}}{{if (ne $i 0)}}, {{end}}{{$el}}{{end}}:
	default:
		err := fmt.Errorf("{{.FieldName|lower}} must be one of {{.Enum|sliceToStr}}")
		if err = respondError(w, ApiError{http.StatusBadRequest, err}); err != nil {
			log.Println(err)
		}
		return
	}
`))

	minStrValidatorTmpl = template.Must(template.New("minStrValidatorTmpl").Funcs(funcMap).Parse(`
	if len(params.{{.FieldName}}) < {{.MinValue}} {
		err := fmt.Errorf("{{.FieldName|lower}} len must be >= {{.MinValue}}")
		if err = respondError(w, ApiError{http.StatusBadRequest, err}); err != nil {
			log.Println(err)
		}
		return
	}
`))
	minIntValidatorTmpl = template.Must(template.New("minIntValidatorTmpl").Funcs(funcMap).Parse(`
	if params.{{.FieldName}} < {{.MinValue}} {
		err := fmt.Errorf("{{.FieldName|lower}} must be >= {{.MinValue}}")
		if err = respondError(w, ApiError{http.StatusBadRequest, err}); err != nil {
			log.Println(err)
		}
		return
	}
`))

	maxStrValidatorTmpl = template.Must(template.New("maxStrValidatorTmpl").Funcs(funcMap).Parse(`
	if len(params.{{.FieldName}}) > {{.MaxValue}} {
		err := fmt.Errorf("{{.FieldName|lower}} len must be <= {{.MaxValue}}")
		if err = respondError(w, ApiError{http.StatusBadRequest, err}); err != nil {
			log.Println(err)
		}
		return
	}
`))
	maxIntValidatorTmpl = template.Must(template.New("maxIntValidatorTmpl").Funcs(funcMap).Parse(`
	if params.{{.FieldName}} > {{.MaxValue}} {
		err := fmt.Errorf("{{.FieldName|lower}} must be <= {{.MaxValue}}")
		if err = respondError(w, ApiError{http.StatusBadRequest, err}); err != nil {
			log.Println(err)
		}
		return
	}
`))

	serveHTTPMethodTmpl = template.Must(template.New("serveHTTPMethodTmpl").Parse(`
func ({{.ReceiverName}} *{{.ReceiverType}}) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	switch req.URL.Path {
	{{range .Wrappers}}
	case "{{.Settings.URL}}":
		{{if (ne .Settings.Method "")}}
		if req.Method != "{{.Settings.Method}}" {
			err := fmt.Errorf("bad method")
			if err = respondError(w, ApiError{http.StatusNotAcceptable, err}); err != nil {
				log.Println(err)
			}
			return
		}
		{{end}}
		{{.ReceiverName}}.{{.Name}}(w, req)
	{{end}}
	default:
		err := fmt.Errorf("unknown method")
		if err = respondError(w, ApiError{http.StatusNotFound, err}); err != nil {
			log.Println(err)
		}
	}
}
`))
)

type FieldSetter struct {
	dataType  string
	FieldName string
	ParamName string
}

func (f FieldSetter) Render(out io.Writer) error {
	switch f.dataType {
	case "int":
		return fieldSetterIntTmpl.Execute(out, f)
	case "string":
		return fieldSetterStrTmpl.Execute(out, f)
	default:
		return fmt.Errorf("unknown type %s", f.dataType)
	}
}

type DefaultFieldSetter struct {
	dataType   string
	FieldName  string
	DefaultVal string
}

func (f DefaultFieldSetter) Render(out io.Writer) error {
	switch f.dataType {
	case "int":
		return defaultFieldSetterIntTmpl.Execute(out, f)
	case "string":
		return defaultFieldSetterStrTmpl.Execute(out, f)
	default:
		return fmt.Errorf("unknown type %s", f.dataType)
	}
}

type RequiredValidator struct {
	dataType  string
	FieldName string
}

func (v RequiredValidator) Render(out io.Writer) error {
	switch v.dataType {
	case "int":
		return requiredIntValidatorTmpl.Execute(out, v)
	case "string":
		return requiredStrValidatorTmpl.Execute(out, v)
	default:
		return fmt.Errorf("unknown type %s", v.dataType)
	}
}

type EnumValidator struct {
	dataType  string
	FieldName string
	Enum      []string
}

func (v EnumValidator) Render(out io.Writer) error {
	switch v.dataType {
	case "int":
		return enumIntValidatorTmpl.Execute(out, v)
	case "string":
		return enumStrValidatorTmpl.Execute(out, v)
	default:
		return fmt.Errorf("unknown type %s", v.dataType)
	}
}

type MinValidator struct {
	dataType  string
	FieldName string
	MinValue  int
}

func (v MinValidator) Render(out io.Writer) error {
	switch v.dataType {
	case "int":
		return minIntValidatorTmpl.Execute(out, v)
	case "string":
		return minStrValidatorTmpl.Execute(out, v)
	default:
		return fmt.Errorf("unknown type %s", v.dataType)
	}
}

type MaxValidator struct {
	dataType  string
	FieldName string
	MaxValue  int
}

func (v MaxValidator) Render(out io.Writer) error {
	switch v.dataType {
	case "int":
		return maxIntValidatorTmpl.Execute(out, v)
	case "string":
		return maxStrValidatorTmpl.Execute(out, v)
	default:
		return fmt.Errorf("unknown type %s", v.dataType)
	}
}

type WrapperMethod struct {
	Settings GenSettings

	fd *ast.FuncDecl

	structType *ast.StructType
	structName string

	recName string
	recType string
}

func NewWrapperMethod(fd *ast.FuncDecl, settings GenSettings) (*WrapperMethod, error) {
	wm := &WrapperMethod{fd: fd, Settings: settings}
	if err := wm.parseReceiver(); err != nil {
		return nil, err
	}
	if err := wm.parseArguments(); err != nil {
		return nil, err
	}

	return wm, nil
}

func (wm *WrapperMethod) parseReceiver() error {
	fd := wm.fd
	if fd.Recv == nil {
		return fmt.Errorf("except method with receiver, got function")
	}

	if len(fd.Recv.List) != 1 || len(fd.Recv.List[0].Names) != 1 {
		return fmt.Errorf("incorrect receiver")
	}

	field := fd.Recv.List[0]
	identName := field.Names[0]
	starExpr, ok := field.Type.(*ast.StarExpr)
	if !ok {
		return fmt.Errorf("receiver type must be a pointer")
	}

	identType, ok := starExpr.X.(*ast.Ident)
	if !ok {
		return fmt.Errorf("receiver type must be present as an identifier")
	}

	wm.recName = identName.Name
	wm.recType = identType.Name
	return nil
}

func (wm *WrapperMethod) parseArguments() error {
	fd := wm.fd
	args := fd.Type.Params.List

	if len(args) != 2 {
		return fmt.Errorf("len of method arguments must be equal 2")
	}

	ctxArg, structArg := args[0], args[1]

	selExpr, ok := ctxArg.Type.(*ast.SelectorExpr)
	if !ok {
		return fmt.Errorf("first argument must be present as a selector expression")
	}

	e, ok := selExpr.X.(*ast.Ident)
	if !ok {
		return fmt.Errorf("first argument must be present as a selector expression")
	}

	if e.Name != "context" {
		return fmt.Errorf("first argument must be a context.Context")
	}
	if selExpr.Sel.Name != "Context" {
		return fmt.Errorf("first argument must be a context.Context")
	}

	ident, ok := structArg.Type.(*ast.Ident)
	if !ok {
		return fmt.Errorf("second argument must be present as an identifier")
	}

	if ident.Obj == nil {
		return fmt.Errorf("second argument must be denote as an object")
	}

	typeSpec, ok := ident.Obj.Decl.(*ast.TypeSpec)
	if !ok {
		return fmt.Errorf("second argument must be represent as a type declaration")
	}

	structType, ok := typeSpec.Type.(*ast.StructType)
	if !ok {
		return fmt.Errorf("second argument must be present as a struct type")
	}

	wm.structType = structType
	wm.structName = typeSpec.Name.Name
	return nil
}

func (wm *WrapperMethod) Render(out io.Writer) error {
	_, _ = fmt.Fprintf(
		out,
		"func (%s *%s) %s(w http.ResponseWriter, req *http.Request) {\n",
		wm.recName, wm.recType, wm.Name(),
	)

	if wm.Settings.Auth {
		_, _ = fmt.Fprintln(out, "\t"+`if req.Header.Get("X-Auth") != "100500" {
		err := fmt.Errorf("unauthorized")
		if err = respondError(w, ApiError{http.StatusForbidden, err}); err != nil {
			log.Println(err)
		}
		return
	}
	`)
	}

	_, _ = fmt.Fprintf(out, "\tparams := %s{}\n", wm.structName)
	for _, field := range wm.structType.Fields.List {
		fieldType, ok := field.Type.(*ast.Ident)
		if !ok {
			return fmt.Errorf("field(s) %s of struct %s must be present as an identifier", field.Names, wm.structName)
		}

		tagParser := new(TagParser)
		if field.Tag != nil {
			structTag := reflect.StructTag(field.Tag.Value[1 : len(field.Tag.Value)-1])
			tagParser.Raw = structTag.Get("apivalidator")
		}

		if err := tagParser.Parse(); err != nil {
			return err
		}

		chain := make([]Renderer, 0, 1)
		for _, fieldName := range field.Names {
			paramName := tagParser.ParamName()
			if paramName == "" {
				paramName = strings.ToLower(fieldName.Name)
			}

			chain = append(chain, FieldSetter{fieldType.Name, fieldName.Name, paramName})
			if tagParser.HasDefault() {
				chain = append(chain, DefaultFieldSetter{fieldType.Name, fieldName.Name, tagParser.Default()})
			}
			if tagParser.IsRequired() {
				chain = append(chain, RequiredValidator{fieldType.Name, fieldName.Name})
			}
			if tagParser.HasEnum() {
				chain = append(chain, EnumValidator{fieldType.Name, fieldName.Name, tagParser.Enum()})
			}
			if tagParser.HasMin() {
				chain = append(chain, MinValidator{fieldType.Name, fieldName.Name, tagParser.Min()})
			}
			if tagParser.HasMax() {
				chain = append(chain, MaxValidator{fieldType.Name, fieldName.Name, tagParser.Max()})
			}

			for _, el := range chain {
				if err := el.Render(out); err != nil {
					return fmt.Errorf("failed to render %T component (field=%q) due %v", el, fieldName.Name, err)
				}
			}
		}
	}

	_, _ = fmt.Fprintf(out, "\n\tres, err := %s.%s(req.Context(), params)\n", wm.recName, wm.fd.Name.Name)
	_, _ = fmt.Fprintln(out, `	if err != nil {
		if err = respondError(w, err); err != nil {
			log.Println(err)
		}
		return
	}
	
	body := ResponseBody{Response: res}
	payload, err := json.Marshal(body)
	if err != nil {
		if err = respondError(w, err); err != nil {
			log.Println(err)
		}
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, err = w.Write(payload)
	if err != nil {
		log.Println(err)
	}`)

	_, _ = fmt.Fprintln(out, "}")
	_, _ = fmt.Fprintln(out)
	return nil
}

func (wm *WrapperMethod) ReceiverName() string {
	return wm.recName
}

func (wm *WrapperMethod) ReceiverType() string {
	return wm.recType
}

func (wm *WrapperMethod) Name() string {
	return "handle" + wm.fd.Name.Name
}

type ServeHTTPMethod struct {
	Wrappers []*WrapperMethod
	recName  string
	recType  string
}

func CreateFromAPIMethod(method *WrapperMethod) *ServeHTTPMethod {
	wrappers := make([]*WrapperMethod, 0, 1)
	wrappers = append(wrappers, method)
	return &ServeHTTPMethod{
		Wrappers: wrappers,
		recName:  method.recName,
		recType:  method.recType,
	}
}

func (sm *ServeHTTPMethod) RelateMethod(method *WrapperMethod) error {
	if sm.recName != method.ReceiverName() {
		return fmt.Errorf("failed to relate api method with serve http method due mismatch receiver name")
	}
	if sm.recType != method.ReceiverType() {
		return fmt.Errorf("failed to relate api method due with serve http method mismatch receiver type")
	}

	sm.Wrappers = append(sm.Wrappers, method)
	return nil
}

func (sm *ServeHTTPMethod) Render(out io.Writer) error {
	return serveHTTPMethodTmpl.Execute(out, sm)
}

func (sm *ServeHTTPMethod) ReceiverName() string {
	return sm.recName
}

func (sm *ServeHTTPMethod) ReceiverType() string {
	return sm.recType
}

func RenderResponseBodyStruct(out io.Writer) error {
	_, err := out.Write([]byte(`type ResponseBody struct {
	Error    string ` + "     `json:\"error\"`" + `
	Response interface{} ` + "`json:\"response,omitempty\"`" + `
}

`))
	return err
}

func RenderRespondErrorFunc(out io.Writer) error {
	_, err := out.Write([]byte(`
func respondError(w http.ResponseWriter, err error) error {
	if apiErr, ok := err.(ApiError); ok {
		view := ResponseBody{Error: apiErr.Err.Error()}

		payload, err := json.Marshal(view)
		if err != nil {
			return fmt.Errorf("failed to marshal error due %v", err)
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(apiErr.HTTPStatus)
		_, err = w.Write(payload)
		if err != nil {
			return fmt.Errorf("failed to write response body due %v", err)
		}

		return nil
	}

	return respondError(w, ApiError{
		HTTPStatus: http.StatusInternalServerError,
		Err:        err,
	})
}

`))
	return err
}

func main() {
	if len(os.Args) != 3 {
		log.Fatal("usage: ./codegen <source_file> <destination_file>")
	}

	fset := token.NewFileSet()
	srcFilePath, dstFilePath := os.Args[1], os.Args[2]
	in, err := parser.ParseFile(fset, srcFilePath, nil, parser.ParseComments)
	if err != nil {
		log.Fatalf("failed to parse file %s due %v", srcFilePath, err)
	}

	out, err := os.Create(dstFilePath)
	if err != nil {
		log.Fatalf("failed to create destination file %s due %v", dstFilePath, err)
	}

	_, _ = fmt.Fprintln(out, "package "+in.Name.Name)
	_, _ = fmt.Fprintln(out, `
import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
)`)
	_, _ = fmt.Fprintln(out)

	if err = RendererFunc(RenderResponseBodyStruct).Renderer(out); err != nil {
		log.Fatalf("failed to render ResponseBody struct due %v", err)
	}

	pattern := regexp.MustCompile(`^\/\/\s*apigen:api\s+({.+})$`)
	serveMethods := make(map[string]*ServeHTTPMethod)

	for _, node := range in.Decls {
		fd, ok := node.(*ast.FuncDecl)
		if !ok {
			log.Printf("skip %T due it's not *ast.FuncDecl", node)
			continue
		}

		if fd.Doc == nil {
			log.Printf("skip function %s due it hasn't doc comments", fd.Name.Name)
			continue
		}

		genPayload := ""
		foundCommentLabel := false
		for _, comment := range fd.Doc.List {
			groups := pattern.FindStringSubmatch(comment.Text)
			if len(groups) > 1 {
				foundCommentLabel = true
				genPayload = groups[1]
			}
		}

		if !foundCommentLabel {
			log.Printf("skip function %s due it hasn't apigen:api label", fd.Name.Name)
			continue
		}

		settings := new(GenSettings)
		if err = json.Unmarshal([]byte(genPayload), settings); err != nil {
			log.Fatalf("failed to unmarshal apigen settings %q due %v", genPayload, err)
		}

		if err = settings.Validate(); err != nil {
			log.Fatalf("apigen settings validation due %v", err)
		}

		wrapper, err := NewWrapperMethod(fd, *settings)
		if err != nil {
			log.Fatalf("failed to create wrapper method component %q due %v", fd.Name.Name, err)
		}

		if serveMethod, exist := serveMethods[wrapper.ReceiverType()]; exist {
			if err = serveMethod.RelateMethod(wrapper); err != nil {
				log.Fatal(err)
			}
		} else {
			serveMethod = CreateFromAPIMethod(wrapper)
			serveMethods[wrapper.ReceiverType()] = serveMethod
		}

		if err = wrapper.Render(out); err != nil {
			log.Fatalf("failed to render wrapper method %q due %v", wrapper.Name(), err)
		}
	}

	if err = RendererFunc(RenderRespondErrorFunc).Renderer(out); err != nil {
		log.Fatalf("failed to render respondError function due %v", err)
	}

	for _, serveMethod := range serveMethods {
		if err = serveMethod.Render(out); err != nil {
			log.Fatalf("failed to render serveHTTP method (receiver = %q) due %v", serveMethod.ReceiverType(), err)
		}
	}
}
