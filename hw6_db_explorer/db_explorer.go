package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type Table struct {
	Name    string
	Columns []*Column
}

func (t *Table) Primary() *Column {
	for _, column := range t.Columns {
		if column.IsPrimary {
			return column
		}
	}
	return nil
}

type Column struct {
	Name      string
	Type      string
	Nullable  bool
	IsPrimary bool
}

type Record map[string]interface{}

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
		var (
			null string
			key  sql.NullString
		)
		column := new(Column)
		vals[0], vals[1], vals[2], vals[3] = &column.Name, &column.Type, &null, &key
		if err = rows.Scan(vals...); err != nil {
			return nil, fmt.Errorf("error occurred during load columns due %v", err)
		}

		if null == "YES" {
			column.Nullable = true
		}
		if key.Valid && key.String == "PRI" {
			column.IsPrimary = true
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

type SelectQueryBuilder struct {
	builder strings.Builder
	args    []interface{}

	wasWhere bool
}

func (b *SelectQueryBuilder) From(table string) *SelectQueryBuilder {
	stmt := fmt.Sprintf("SELECT * FROM `%s`", table)
	b.builder.WriteString(stmt)
	return b
}

func (b *SelectQueryBuilder) And(column string, val interface{}) *SelectQueryBuilder {
	b.buildCondition("AND", column, val)
	return b
}

func (b *SelectQueryBuilder) buildCondition(op, column string, arg interface{}) {
	b.builder.WriteRune(' ')
	if !b.wasWhere {
		b.builder.WriteString("WHERE")
		b.wasWhere = true
	} else {
		b.builder.WriteString(op)
	}

	b.builder.WriteString(fmt.Sprintf(" `%s` = ?", column))

	b.args = append(b.args, arg)
}

func (b *SelectQueryBuilder) Limit(val int) *SelectQueryBuilder {
	stmt := fmt.Sprintf(" LIMIT %d", val)
	b.builder.WriteString(stmt)
	return b
}

func (b *SelectQueryBuilder) Offset(val int) *SelectQueryBuilder {
	stmt := fmt.Sprintf(" OFFSET %d", val)
	b.builder.WriteString(stmt)
	return b
}

func (b *SelectQueryBuilder) String() string {
	return b.builder.String()
}

func (b *SelectQueryBuilder) Args() []interface{} {
	return b.args
}

type ApiError struct {
	Status int
	Err    error
}

var (
	errBadLimit         = ApiError{Status: http.StatusBadRequest, Err: errors.New("bad limit")}
	errBadOffset        = ApiError{Status: http.StatusBadRequest, Err: errors.New("bad offset")}
	errTableNotFound    = ApiError{Status: http.StatusNotFound, Err: errors.New("unknown table")}
	errRecordNotFound   = ApiError{Status: http.StatusNotFound, Err: errors.New("record not found")}
	errResourceNotFound = ApiError{Status: http.StatusNotFound, Err: errors.New("resource is not found")}
	errNotAllowed       = ApiError{Status: http.StatusMethodNotAllowed, Err: errors.New("method is not allowed")}
	errInternal         = ApiError{Status: http.StatusInternalServerError, Err: errors.New("internal server error")}
)

func (ae ApiError) Error() string {
	return ae.Err.Error()
}

type ResponseBody struct {
	Error    string      `json:"error,omitempty"`
	Response interface{} `json:"response,omitempty"`
}

type ResponseTablesBody struct {
	Tables []string `json:"tables"`
}

type ResponseRecordBody struct {
	Record Record `json:"record"`
}

type ResponseRecordsBody struct {
	Records []Record `json:"records"`
}

type TableHandler struct {
	db *sql.DB

	tables    []*Table
	tablesMap map[string]*Table

	listRegex   *regexp.Regexp
	detailRegex *regexp.Regexp
}

func (t *TableHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	url := req.URL.Path

	switch {
	case url == "/":
		t.handleTableList(w)
	case t.listRegex.MatchString(url):
		groups := t.listRegex.FindStringSubmatch(url)
		if len(groups) != 2 {
			log.Println("submatch group len must be equal 2")
			t.respondError(w, errInternal)
			return
		}

		tableName := groups[1]
		table, exist := t.tablesMap[tableName]
		if !exist {
			t.respondError(w, errTableNotFound)
			return
		}

		switch req.Method {
		case http.MethodGet:
			t.handleRecordList(w, req, table)
		case http.MethodPut:
		default:
			t.respondError(w, errNotAllowed)
		}
	case t.detailRegex.MatchString(url):
		groups := t.detailRegex.FindStringSubmatch(url)
		if len(groups) != 3 {
			log.Println("submatch group len must be equal 3")
			t.respondError(w, errInternal)
			return
		}

		tableName := groups[1]
		table, exist := t.tablesMap[tableName]
		if !exist {
			t.respondError(w, errTableNotFound)
			return
		}
		recordID, _ := strconv.Atoi(groups[2])

		switch req.Method {
		case http.MethodGet:
			t.handleRecordDetail(w, req, table, recordID)
		case http.MethodPost:
		case http.MethodDelete:
		default:
			t.respondError(w, errNotAllowed)
		}
	default:
		t.respondError(w, errResourceNotFound)
	}
}

func (t *TableHandler) handleTableList(w http.ResponseWriter) {
	tableNames := make([]string, 0, len(t.tables))
	for _, table := range t.tables {
		tableNames = append(tableNames, table.Name)
	}

	t.successRespond(w, http.StatusOK, ResponseTablesBody{tableNames})
}

func (t *TableHandler) handleRecordList(w http.ResponseWriter, req *http.Request, table *Table) {
	query := req.URL.Query()

	var err error
	limit := 5
	if limitStr := query.Get("limit"); limitStr != "" {
		limit, err = strconv.Atoi(limitStr)
		if err != nil {
			t.respondError(w, errBadLimit)
			return
		}
	}

	offset := 0
	if offsetStr := query.Get("offset"); offsetStr != "" {
		offset, err = strconv.Atoi(offsetStr)
		if err != nil {
			t.respondError(w, errBadOffset)
			return
		}
	}

	builder := &SelectQueryBuilder{}
	builder.From(table.Name).Limit(limit).Offset(offset)

	rows, err := t.db.QueryContext(req.Context(), builder.String())
	if err != nil {
		log.Println(err)
		t.respondError(w, errInternal)
		return
	}

	records, err := t.createRecordsFromRows(rows)
	if err != nil {
		log.Println(err)
		t.respondError(w, errInternal)
		return
	}

	t.successRespond(w, http.StatusOK, ResponseRecordsBody{records})
}

func (t *TableHandler) handleRecordDetail(w http.ResponseWriter, req *http.Request, table *Table, recordID int) {
	primary := table.Primary()
	if primary == nil {
		log.Printf("table %s doesn't have the primary key", table.Name)
		t.respondError(w, errInternal)
		return
	}

	builder := &SelectQueryBuilder{}
	builder.From(table.Name).And(primary.Name, recordID).Limit(1)

	rows, err := t.db.QueryContext(req.Context(), builder.String(), builder.Args()...)
	if err != nil {
		log.Println(err)
		t.respondError(w, errInternal)
		return
	}

	records, err := t.createRecordsFromRows(rows)
	if err != nil {
		log.Println(err)
		t.respondError(w, errInternal)
		return
	}

	if len(records) == 0 {
		t.respondError(w, errRecordNotFound)
		return
	}

	t.successRespond(w, http.StatusOK, ResponseRecordBody{records[0]})
}

func (t *TableHandler) createRecordsFromRows(rows *sql.Rows) ([]Record, error) {
	columnsTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	records := make([]Record, 0)
	for rows.Next() {
		vals := make([]interface{}, len(columnsTypes))
		for i, columnType := range columnsTypes {
			dbType := columnType.DatabaseTypeName()
			if nullable, _ := columnType.Nullable(); nullable {
				switch dbType {
				case "INT":
					vals[i] = new(sql.NullInt64)
				case "FLOAT":
					vals[i] = new(sql.NullFloat64)
				case "VARCHAR", "TEXT":
					vals[i] = new(sql.NullString)
				}
			} else {
				switch dbType {
				case "INT":
					vals[i] = new(int64)
				case "FLOAT":
					vals[i] = new(float64)
				case "VARCHAR", "TEXT":
					vals[i] = new(string)
				}
			}
		}

		record := make(Record, len(columnsTypes))
		if err = rows.Scan(vals...); err != nil {
			return nil, err
		}

		for i, columnType := range columnsTypes {
			if valuer, ok := vals[i].(driver.Valuer); ok {
				val, err := valuer.Value()
				if err != nil {
					return nil, err
				}

				record[columnType.Name()] = val
			} else {
				record[columnType.Name()] = vals[i]
			}
		}
		records = append(records, record)
	}

	return records, nil
}

func (t *TableHandler) successRespond(w http.ResponseWriter, statusCode int, response interface{}) {
	respBody := ResponseBody{Response: response}
	payload, err := json.Marshal(respBody)
	if err != nil {
		log.Printf("failed to marshal response body due %v", err)
		t.respondError(w, errInternal)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_, err = w.Write(payload)
	if err != nil {
		log.Printf("error occurred during write to response writer due %v", err)
	}
}

func (t *TableHandler) respondError(w http.ResponseWriter, err error) {
	if apiErr, ok := err.(ApiError); ok {
		view := ResponseBody{Error: apiErr.Err.Error()}

		payload, err := json.Marshal(view)
		if err != nil {
			log.Printf("failed to marshal payload due %v", err)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(apiErr.Status)
		_, err = w.Write(payload)
		if err != nil {
			log.Printf("failed to write response body due %v", err)
		}

		return
	}

	t.respondError(w, ApiError{
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

	tablesMap := make(map[string]*Table, len(tables))
	for _, table := range tables {
		columns, err := getColumns(db, table.Name)
		if err != nil {
			return nil, fmt.Errorf("failed to get columns of table %s: %v", table.Name, err)
		}

		table.Columns = columns
		tablesMap[table.Name] = table
	}

	mux.Handle("/", &TableHandler{
		db:          db,
		tables:      tables,
		tablesMap:   tablesMap,
		listRegex:   regexp.MustCompile(`^/([a-zA-Z0-9_-]+)/?$`),
		detailRegex: regexp.MustCompile(`^/([a-zA-Z0-9_-]+)/(\d+)$`),
	})
	return mux, nil
}
