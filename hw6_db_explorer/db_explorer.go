package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"regexp"
	"time"
)

type Table struct {
	Name    string
	Columns []*Column
}

type Column struct {
	Name     string
	Type     string
	Nullable bool
}

func getTables(db *sql.DB) ([]*Table, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	rows, err := db.QueryContext(ctx, "SHOW TABLES")
	if err != nil {
		return nil, fmt.Errorf("error occurred during get rows due %v", err)
	}

	tables := make([]*Table, 0)
	for rows.Next() {
		table := new(Table)
		if err = rows.Scan(&table.Name); err != nil {
			return nil, fmt.Errorf("error occurred during load columns due %v", err)
		}
		tables = append(tables, table)
	}

	if err = rows.Close(); err != nil {
		return nil, fmt.Errorf("error occurred during closing rows due %v", err)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error occurred table iteration due %v", err)
	}

	return tables, nil
}

func getColumns(db *sql.DB, tableName string) ([]*Column, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	rows, err := db.QueryContext(ctx, "SHOW COLUMNS FROM "+tableName)
	if err != nil {
		return nil, fmt.Errorf("error occurred during get rows due %v", err)
	}

	rowCols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("error occurred during get column names due %v", err)
	}

	vals := make([]interface{}, len(rowCols))
	for i := range vals {
		vals[i] = new(sql.RawBytes)
	}

	columns := make([]*Column, 0)
	for rows.Next() {
		var null string
		column := new(Column)
		vals[0], vals[1], vals[2] = &column.Name, &column.Type, &null
		if err = rows.Scan(vals...); err != nil {
			return nil, fmt.Errorf("error occurred during load columns due %v", err)
		}

		if null == "YES" {
			column.Nullable = true
		}
		columns = append(columns, column)
	}

	if err = rows.Close(); err != nil {
		return nil, fmt.Errorf("error occurred during closing rows due %v", err)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error occurred table iteration due %v", err)
	}

	return columns, nil
}

type ApiError struct {
	Status int
	Err    error
}

var (
	errNotFound = ApiError{Status: http.StatusNotFound, Err: errors.New("resource is not found")}
)

func (ae ApiError) Error() string {
	return ae.Err.Error()
}

type ResponseBody struct {
	Error    string      `json:"error,omitempty"`
	Response interface{} `json:"response,omitempty"`
}

type ResponseTableBody struct {
	Tables []string `json:"tables"`
}

type TableHandler struct {
	db     *sql.DB
	tables []*Table

	listRegex   *regexp.Regexp
	detailRegex *regexp.Regexp
}

func (t *TableHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	url := req.URL.Path

	switch {
	case url == "/":
		t.handleTableList(w)
	case t.listRegex.MatchString(url):
		fmt.Fprintln(w, "list")
	case t.detailRegex.MatchString(url):
		groups := t.detailRegex.FindStringSubmatch(url)
		fmt.Fprintln(w, "detail", "id = ", groups[1])
	default:
		if err := t.respondError(w, errNotFound); err != nil {
			log.Println(err)
		}
	}
}

func (t *TableHandler) handleTableList(w http.ResponseWriter) {
	tableNames := make([]string, 0, len(t.tables))
	for _, table := range t.tables {
		tableNames = append(tableNames, table.Name)
	}

	respBody := ResponseBody{Response: ResponseTableBody{tableNames}}
	payload, err := json.Marshal(respBody)
	if err != nil {
		if err = t.respondError(w, err); err != nil {
			log.Println(err)
		}

		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, err = w.Write(payload)
	if err != nil {
		log.Printf("error occurred during write to response writer due %v", err)
	}
}

func (t *TableHandler) respondError(w http.ResponseWriter, err error) error {
	if apiErr, ok := err.(ApiError); ok {
		view := ResponseBody{Error: apiErr.Err.Error()}

		payload, err := json.Marshal(view)
		if err != nil {
			return fmt.Errorf("failed to marshal error due %v", err)
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(apiErr.Status)
		_, err = w.Write(payload)
		if err != nil {
			return fmt.Errorf("failed to write response body due %v", err)
		}

		return nil
	}

	return t.respondError(w, ApiError{
		Status: http.StatusInternalServerError,
		Err:    err,
	})
}

func NewDbExplorer(db *sql.DB) (http.Handler, error) {
	mux := http.NewServeMux()

	tables, err := getTables(db)
	if err != nil {
		return nil, fmt.Errorf("failed to get tables: %v", err)
	}
	for _, table := range tables {
		columns, err := getColumns(db, table.Name)
		if err != nil {
			return nil, fmt.Errorf("failed to get columns of table %s: %v", table.Name, err)
		}

		table.Columns = columns
	}

	mux.Handle("/", &TableHandler{
		db:          db,
		tables:      tables,
		listRegex:   regexp.MustCompile(`^/table/?$`),
		detailRegex: regexp.MustCompile(`^/table/(\d+)$`),
	})
	return mux, nil
}
