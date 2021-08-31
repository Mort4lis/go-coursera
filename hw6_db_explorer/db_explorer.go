package main

import (
	"database/sql"
	"fmt"
	"net/http"
	"regexp"
)

type TableHandler struct {
	db          *sql.DB
	listRegex   *regexp.Regexp
	detailRegex *regexp.Regexp
}

func (t *TableHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	url := req.URL.Path

	switch {
	case url == "/":
		fmt.Fprintln(w, "root")
	case t.listRegex.MatchString(url):
		fmt.Fprintln(w, "list")
	case t.detailRegex.MatchString(url):
		groups := t.detailRegex.FindStringSubmatch(url)
		fmt.Fprintln(w, "detail", "id = ", groups[1])
	default:
		fmt.Fprintln(w, "not found")
	}
}

func NewDbExplorer(db *sql.DB) (http.Handler, error) {
	mux := http.NewServeMux()

	mux.Handle("/", &TableHandler{
		db:          db,
		listRegex:   regexp.MustCompile(`^/table/?$`),
		detailRegex: regexp.MustCompile(`^/table/(\d+)$`),
	})
	return mux, nil
}
