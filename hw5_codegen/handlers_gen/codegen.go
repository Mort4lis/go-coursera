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
	"regexp"
	"strings"
)

type GenAPIParams struct {
	URL    string
	Auth   bool
	Method string
}

func (p *GenAPIParams) Validate() error {
	if _, err := url.ParseRequestURI(p.URL); err != nil {
		return err
	}

	if p.Method == "" {
		return nil
	}

	p.Method = strings.ToUpper(p.Method)
	switch p.Method {
	case http.MethodGet, http.MethodPost:
	default:
		return fmt.Errorf("unsupport method %s to api generate", p.Method)
	}

	return nil
}

type WrapperMethod struct {
	fd      *ast.FuncDecl
	params  GenAPIParams
	recName string
	recType string
}

func NewWrapperMethod(fd *ast.FuncDecl, params GenAPIParams) (*WrapperMethod, error) {
	if fd.Recv == nil {
		return nil, fmt.Errorf("except method with receiver, got function")
	}

	if len(fd.Recv.List) != 1 || len(fd.Recv.List[0].Names) != 1 {
		return nil, fmt.Errorf("incorrect receiver")
	}

	field := fd.Recv.List[0]
	identName := field.Names[0]
	starExpr, ok := field.Type.(*ast.StarExpr)
	if !ok {
		return nil, fmt.Errorf("receiver type must be a pointer")
	}

	identType, ok := starExpr.X.(*ast.Ident)
	if !ok {
		return nil, fmt.Errorf("receiver type must be present as an identifier")
	}

	return &WrapperMethod{
		fd:      fd,
		params:  params,
		recName: identName.Name,
		recType: identType.Name,
	}, nil
}

func (wm *WrapperMethod) ReceiverName() string {
	return wm.recName
}

func (wm *WrapperMethod) ReceiverType() string {
	return wm.recType
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

func (sm *ServeHTTPMethod) Render(out io.Writer) {
	_, _ = fmt.Fprintf(out, "func (%s *%s) ServeHTTP(w http.ResponseWriter, req *http.Request) {\n", sm.recName, sm.recType)
	_, _ = fmt.Fprintln(out, "\t // handler body")
	_, _ = fmt.Fprintln(out, "}")
	_, _ = fmt.Fprintln(out)
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

		genParams := new(GenAPIParams)
		if err = json.Unmarshal([]byte(genPayload), genParams); err != nil {
			log.Fatal(err)
		}

		if err = genParams.Validate(); err != nil {
			log.Fatal(err)
		}

		wrapper, err := NewWrapperMethod(fd, *genParams)
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
	}

	for _, serveMethod := range serveMethods {
		serveMethod.Render(out)
	}
}
