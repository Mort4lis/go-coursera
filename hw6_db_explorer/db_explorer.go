package main

import (
	"context"
	"database/sql"
	"fmt"
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
		listRegex:   regexp.MustCompile(`^/table/?$`),
		detailRegex: regexp.MustCompile(`^/table/(\d+)$`),
	})
	return mux, nil
}
