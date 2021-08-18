package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/mailru/easyjson"
	"github.com/mailru/easyjson/jlexer"
	"github.com/mailru/easyjson/jwriter"
	"io"
	"os"
	"strings"
)

type User struct {
	Name     string
	Email    string
	Browsers []string
}

// suppress unused package warning
var (
	_ *json.RawMessage
	_ *jlexer.Lexer
	_ *jwriter.Writer
	_ easyjson.Marshaler
)

func easyjson9e1087fdDecodeGithubComMort4lisGoCourseraEz(in *jlexer.Lexer, out *User) {
	isTopLevel := in.IsStart()
	if in.IsNull() {
		if isTopLevel {
			in.Consumed()
		}
		in.Skip()
		return
	}
	in.Delim('{')
	for !in.IsDelim('}') {
		key := in.UnsafeFieldName(false)
		in.WantColon()
		if in.IsNull() {
			in.Skip()
			in.WantComma()
			continue
		}
		switch key {
		case "name":
			out.Name = in.String()
		case "email":
			out.Email = in.String()
		case "browsers":
			if in.IsNull() {
				in.Skip()
				out.Browsers = nil
			} else {
				in.Delim('[')
				if out.Browsers == nil {
					if !in.IsDelim(']') {
						out.Browsers = make([]string, 0, 4)
					} else {
						out.Browsers = []string{}
					}
				} else {
					out.Browsers = (out.Browsers)[:0]
				}
				for !in.IsDelim(']') {
					var v1 string
					v1 = in.String()
					out.Browsers = append(out.Browsers, v1)
					in.WantComma()
				}
				in.Delim(']')
			}
		default:
			in.SkipRecursive()
		}
		in.WantComma()
	}
	in.Delim('}')
	if isTopLevel {
		in.Consumed()
	}
}
func easyjson9e1087fdEncodeGithubComMort4lisGoCourseraEz(out *jwriter.Writer, in User) {
	out.RawByte('{')
	first := true
	_ = first
	{
		const prefix string = ",\"name\":"
		out.RawString(prefix[1:])
		out.String(in.Name)
	}
	{
		const prefix string = ",\"email\":"
		out.RawString(prefix)
		out.String(in.Email)
	}
	{
		const prefix string = ",\"browsers\":"
		out.RawString(prefix)
		if in.Browsers == nil && (out.Flags&jwriter.NilSliceAsEmpty) == 0 {
			out.RawString("null")
		} else {
			out.RawByte('[')
			for v2, v3 := range in.Browsers {
				if v2 > 0 {
					out.RawByte(',')
				}
				out.String(v3)
			}
			out.RawByte(']')
		}
	}
	out.RawByte('}')
}

// MarshalJSON supports json.Marshaler interface
func (v User) MarshalJSON() ([]byte, error) {
	w := jwriter.Writer{}
	easyjson9e1087fdEncodeGithubComMort4lisGoCourseraEz(&w, v)
	return w.Buffer.BuildBytes(), w.Error
}

// MarshalEasyJSON supports easyjson.Marshaler interface
func (v User) MarshalEasyJSON(w *jwriter.Writer) {
	easyjson9e1087fdEncodeGithubComMort4lisGoCourseraEz(w, v)
}

// UnmarshalJSON supports json.Unmarshaler interface
func (v *User) UnmarshalJSON(data []byte) error {
	r := jlexer.Lexer{Data: data}
	easyjson9e1087fdDecodeGithubComMort4lisGoCourseraEz(&r, v)
	return r.Error()
}

// UnmarshalEasyJSON supports easyjson.Unmarshaler interface
func (v *User) UnmarshalEasyJSON(l *jlexer.Lexer) {
	easyjson9e1087fdDecodeGithubComMort4lisGoCourseraEz(l, v)
}

func FastSearch(out io.Writer) {
	file, err := os.Open(filePath)
	if err != nil {
		panic(err)
	}

	i := -1
	sc := bufio.NewScanner(file)
	browserSet := make(map[string]bool)

	_, _ = fmt.Fprintln(out, "found users:")
	for sc.Scan() {
		user := new(User)
		if err = user.UnmarshalJSON(sc.Bytes()); err != nil {
			panic(err)
		}

		i++
		var hasAndroid, hasMSIE bool
		for _, browser := range user.Browsers {
			containsAndroid := strings.Contains(browser, "Android")
			containsMSIE := strings.Contains(browser, "MSIE")

			if containsAndroid {
				hasAndroid = true
			}
			if containsMSIE {
				hasMSIE = true
			}

			if containsAndroid || containsMSIE {
				if !browserSet[browser] {
					browserSet[browser] = true
				}
			}
		}

		if hasAndroid && hasMSIE {
			email := strings.ReplaceAll(user.Email, "@", " [at] ")
			_, _ = fmt.Fprintf(out, "[%d] %s <%s>\n", i, user.Name, email)
		}
	}

	_, _ = fmt.Fprintln(out, "\nTotal unique browsers", len(browserSet))
}
