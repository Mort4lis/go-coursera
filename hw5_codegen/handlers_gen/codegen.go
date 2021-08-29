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
	enums      []string
	defaultVal string
	min        int
	max        int
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
		case labelEnum:
			p.enums = strings.Split(value, "|")
		case labelDefault:
			p.defaultVal = value
		case labelMin, labelMax:
			val, err := strconv.Atoi(value)
			if err != nil {
				return fmt.Errorf("can't convert to int label value %s", key)
			}

			if key == labelMin {
				p.min = val
			} else {
				p.max = val
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
	return p.enums
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

type Renderer interface {
	Render(out io.Writer) error
}

var funcMap = template.FuncMap{
	"lower": strings.ToLower,
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
		// handle error
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

	requiredStrValidatorTmpl = template.Must(template.New("requiredStrValidatorTmpl").Parse(`
	if params.{{.FieldName}} == "" {
		// handle error
		return
	}
`))
	requiredIntValidatorTmpl = template.Must(template.New("requiredIntValidatorTmpl").Parse(`
	if params.{{.FieldName}} == 0 {
		// handle error
		return
	}
`))

	enumStrValidatorTmpl = template.Must(template.New("enumStrValidatorTmpl").Parse(`
	switch params.{{.FieldName}} {
	case {{range $i, $el := .Enum}}{{if (ne $i 0)}}, {{end}}"{{$el}}"{{end}}:
	default:
		// handle error
		return
	}
`))
	enumIntValidatorTmpl = template.Must(template.New("enumIntValidatorTmpl").Parse(`
	switch params.{{.FieldName}} {
	case {{range $i, $el := .Enum}}{{if (ne $i 0)}}, {{end}}{{$el}}{{end}}:
	default:
		// handle error
		return
	}
`))

	minStrValidatorTmpl = template.Must(template.New("minStrValidatorTmpl").Parse(`
	if len(params.{{.FieldName}}) < {{.MinValue}} {
		// handle error
		return
	}
`))
	minIntValidatorTmpl = template.Must(template.New("minIntValidatorTmpl").Parse(`
	if params.{{.FieldName}} < {{.MinValue}} {
		// handle error
		return
	}
`))

	maxStrValidatorTmpl = template.Must(template.New("maxStrValidatorTmpl").Parse(`
	if len(params.{{.FieldName}}) > {{.MaxValue}} {
		// handle error
		return
	}
`))
	maxIntValidatorTmpl = template.Must(template.New("maxIntValidatorTmpl").Parse(`
	if params.{{.FieldName}} > {{.MaxValue}} {
		// handle error
		return
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
	settings GenSettings

	fd *ast.FuncDecl

	structType *ast.StructType
	structName string

	recName string
	recType string
}

func NewWrapperMethod(fd *ast.FuncDecl, settings GenSettings) (*WrapperMethod, error) {
	wm := &WrapperMethod{fd: fd, settings: settings}
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

	_, structArg := args[0], args[1]

	// TODO: check the first argument (must be a context)

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
			if defaultVal := tagParser.Default(); defaultVal != "" {
				chain = append(chain, DefaultFieldSetter{fieldType.Name, fieldName.Name, defaultVal})
			}
			if tagParser.IsRequired() {
				chain = append(chain, RequiredValidator{fieldType.Name, fieldName.Name})
			}
			if enum := tagParser.Enum(); len(enum) != 0 {
				chain = append(chain, EnumValidator{fieldType.Name, fieldName.Name, tagParser.Enum()})
			}
			if minVal := tagParser.Min(); minVal != 0 {
				chain = append(chain, MinValidator{fieldType.Name, fieldName.Name, minVal})
			}
			if maxVal := tagParser.Max(); maxVal != 0 {
				chain = append(chain, MaxValidator{fieldType.Name, fieldName.Name, maxVal})
			}
		}

		for _, el := range chain {
			if err := el.Render(out); err != nil {
				return err
			}
		}
	}

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
	wrappers []*WrapperMethod
	recName  string
	recType  string
}

func CreateFromAPIMethod(method *WrapperMethod) *ServeHTTPMethod {
	wrappers := make([]*WrapperMethod, 0, 1)
	wrappers = append(wrappers, method)
	return &ServeHTTPMethod{
		wrappers: wrappers,
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

	sm.wrappers = append(sm.wrappers, method)
	return nil
}

func (sm *ServeHTTPMethod) Render(out io.Writer) error {
	_, _ = fmt.Fprintf(out, "func (%s *%s) ServeHTTP(w http.ResponseWriter, req *http.Request) {\n", sm.recName, sm.recType)
	_, _ = fmt.Fprintln(out, "\t // handler body")
	_, _ = fmt.Fprintln(out, "}")
	_, _ = fmt.Fprintln(out)
	return nil
}

func (sm *ServeHTTPMethod) ReceiverName() string {
	return sm.recName
}

func (sm *ServeHTTPMethod) ReceiverType() string {
	return sm.recType
}

func exitWithMessage(msg string) {
	_, wErr := fmt.Fprintln(os.Stderr, msg)
	if wErr != nil {
		log.Fatal(wErr)
	}
	os.Exit(1)
}

func main() {
	if len(os.Args) != 3 {
		exitWithMessage("usage: ./codegen <source_file> <destination_file>")
	}

	fset := token.NewFileSet()
	srcFilePath, dstFilePath := os.Args[1], os.Args[2]
	in, err := parser.ParseFile(fset, srcFilePath, nil, parser.ParseComments)
	if err != nil {
		log.Println(err)
		exitWithMessage(fmt.Sprintf("failed to parse file %s", srcFilePath))
	}

	out, err := os.Create(dstFilePath)
	if err != nil {
		log.Println(err)
		exitWithMessage(fmt.Sprintf("failed to create destination file %s", dstFilePath))
	}

	_, _ = fmt.Fprintln(out, "package "+in.Name.Name)
	_, _ = fmt.Fprintln(out)
	_, _ = fmt.Fprintln(out, "import (")
	_, _ = fmt.Fprintln(out, "\t"+`"net/http"`)
	_, _ = fmt.Fprintln(out, "\t"+`"strconv"`)
	_, _ = fmt.Fprintln(out, ")")
	_, _ = fmt.Fprintln(out)

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
			log.Fatal(err)
		}

		if err = settings.Validate(); err != nil {
			log.Fatal(err)
		}

		wrapper, err := NewWrapperMethod(fd, *settings)
		if err != nil {
			log.Fatal(err)
		}

		if serveMethod, exist := serveMethods[wrapper.ReceiverType()]; exist {
			if err = serveMethod.RelateMethod(wrapper); err != nil {
				log.Fatal(err)
			}
		} else {
			serveMethod = CreateFromAPIMethod(wrapper)
			serveMethods[wrapper.ReceiverType()] = serveMethod
		}

		wrapper.Render(out)
	}

	for _, serveMethod := range serveMethods {
		serveMethod.Render(out)
	}
}