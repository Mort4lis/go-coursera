package main

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
)

type Paginator struct {
	offset, limit int
	begin, end    int
	elems         []interface{}
}

func NewPaginator(in interface{}, offset int, limit int) (*Paginator, error) {
	rv := reflect.ValueOf(in)
	if rv.Kind() != reflect.Slice {
		return nil, fmt.Errorf("%T is not a slice", in)
	}

	elems := make([]interface{}, 0, rv.Len())
	for i := 0; i < rv.Len(); i++ {
		elems = append(elems, rv.Index(i).Interface())
	}

	if offset > len(elems) {
		offset = 0
	}
	if limit == 0 {
		limit = len(elems)
	}

	begin := offset
	end := offset + limit

	if end > len(elems) {
		end = len(elems)
	}

	return &Paginator{
		offset, limit,
		begin, end,
		elems,
	}, nil
}

func (p *Paginator) Paginate() []interface{} {
	return p.elems[p.begin:p.end]
}

type ServerError struct {
	StatusCode int    `json:"-"`
	Message    string `json:"error"`
	err        error
}

func NewServerError(statusCode int, message string) *ServerError {
	return &ServerError{
		StatusCode: statusCode,
		Message:    message,
		err:        fmt.Errorf(message),
	}
}

func (e *ServerError) Error() string {
	return e.err.Error()
}

func (e *ServerError) Unwrap() error {
	return e.err
}

func (e *ServerError) Marshal() ([]byte, error) {
	return json.Marshal(e)
}

var (
	errBadLimit      = NewServerError(http.StatusBadRequest, "ErrorBadLimit")
	errBadOffset     = NewServerError(http.StatusBadRequest, "ErrorBadOffset")
	errBadOrderBy    = NewServerError(http.StatusBadRequest, "ErrorBadOrderBy")
	errBadOrderField = NewServerError(http.StatusBadRequest, "ErrorBadOrderField")

	errInternalServer = NewServerError(http.StatusInternalServerError, "ErrorInternalServer")
)

const (
	IdField    = "Id"
	AgeField   = "Age"
	NameField  = "Name"
	AboutField = "About"
)

type QueryParams struct {
	Query      string
	OrderField string
	OrderBy    int
	Offset     int
	Limit      int
}

type QueryParamsExtractor struct {
	params url.Values
}

func (q QueryParamsExtractor) Extract() (QueryParams, error) {
	qParams := QueryParams{}

	orderField, err := q.OrderField()
	if err != nil {
		log.Println(err)
		return qParams, errBadOrderField
	}

	orderBy, err := q.OrderBy()
	if err != nil {
		log.Println(err)
		return qParams, errBadOrderBy
	}

	offset, err := q.Offset()
	if err != nil {
		log.Println(err)
		return qParams, errBadOffset
	}

	limit, err := q.Limit()
	if err != nil {
		log.Println(err)
		return qParams, errBadLimit
	}

	qParams.Query = q.Query()
	qParams.OrderField = orderField
	qParams.OrderBy = orderBy
	qParams.Offset = offset
	qParams.Limit = limit

	return qParams, nil
}

func (q QueryParamsExtractor) Query() string {
	return q.params.Get("query")
}

func (q QueryParamsExtractor) OrderField() (string, error) {
	orderField := q.params.Get("order_field")

	switch orderField {
	case "":
		return NameField, nil
	case IdField, NameField, AboutField:
		return orderField, nil
	default:
		return "", fmt.Errorf("unknown order field %q", orderField)
	}
}

func (q QueryParamsExtractor) OrderBy() (int, error) {
	orderByStr := q.params.Get("order_by")
	if orderByStr == "" {
		return OrderByAsIs, nil
	}

	orderBy, err := strconv.Atoi(orderByStr)
	if err != nil {
		return 0, fmt.Errorf("wrong format for order_by field")
	}

	switch orderBy {
	case OrderByAsIs, OrderByAsc, OrderByDesc:
		return orderBy, nil
	default:
		return 0, fmt.Errorf("unknown order_by field %d", orderBy)
	}
}

func (q QueryParamsExtractor) Offset() (int, error) {
	return q.getPositive("offset")
}

func (q QueryParamsExtractor) Limit() (int, error) {
	return q.getPositive("limit")
}

func (q QueryParamsExtractor) getPositive(key string) (int, error) {
	strVal := q.params.Get(key)
	if strVal == "" {
		return 0, nil
	}

	val, err := strconv.Atoi(strVal)
	if err != nil {
		return 0, fmt.Errorf("wrong format for %s field", key)
	}

	if val < 0 {
		return 0, fmt.Errorf("%s can't be negative", key)
	}

	return val, nil
}

type ServerUser struct {
	Id        int    `json:"id" xml:"id"`
	FirstName string `json:"-" xml:"first_name"`
	LastName  string `json:"-" xml:"last_name"`
	Name      string `json:"name" xml:"-"`
	Age       int    `json:"age" xml:"age"`
	About     string `json:"about" xml:"about"`
	Gender    string `json:"gender" xml:"gender"`
}

func SearchServer(w http.ResponseWriter, req *http.Request) {
	extractor := QueryParamsExtractor{params: req.URL.Query()}
	qParams, err := extractor.Extract()
	if err != nil {
		sendErrorJSONResponse(w, err)
		return
	}

	users, err := getUsersFromFile("/home/mortalis/GoLandProjects/learn/go-coursera/hw4_test_coverage/dataset.xml", qParams)
	if err != nil {
		log.Println(err)
		sendErrorJSONResponse(w, errInternalServer)
		return
	}

	paginator, _ := NewPaginator(users, qParams.Offset, qParams.Limit)
	payload, err := json.Marshal(paginator.Paginate())
	if err != nil {
		log.Println(err)
		sendErrorJSONResponse(w, errInternalServer)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_, err = w.Write(payload)
	if err != nil {
		log.Println(err)
		sendErrorJSONResponse(w, errInternalServer)
		return
	}
}

func sendErrorJSONResponse(w http.ResponseWriter, err error) {
	sErr, ok := err.(*ServerError)
	if !ok {
		sendErrorJSONResponse(w, errInternalServer)
		return
	}

	payload, err := sErr.Marshal()
	if err != nil {
		log.Println(err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(sErr.StatusCode)
	_, err = w.Write(payload)
	if err != nil {
		log.Println(err)
		return
	}
}

func getUsers(reader io.Reader, qParams QueryParams) ([]*ServerUser, error) {
	users := make([]*ServerUser, 0)
	decoder := xml.NewDecoder(reader)

	for {
		token, err := decoder.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		switch se := token.(type) {
		case xml.StartElement:
			if se.Name.Local == "row" {
				user := new(ServerUser)
				if err = decoder.DecodeElement(user, &se); err != nil {
					return nil, err
				}

				user.Name = fmt.Sprintf("%s %s", user.FirstName, user.LastName)
				if qParams.Query != "" && !filterUser(user, qParams.Query) {
					continue
				}

				users = append(users, user)
			}
		}
	}

	sortFunc := getSortFunc(users, qParams.OrderField, qParams.OrderBy)
	if sortFunc != nil {
		sort.Slice(users, sortFunc)
	}

	return users, nil
}

func getUsersFromFile(filepath string, qParams QueryParams) ([]*ServerUser, error) {
	file, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}

	return getUsers(file, qParams)
}

func filterUser(user *ServerUser, query string) bool {
	return strings.Contains(user.Name, query) || strings.Contains(user.About, query)
}

type SortFunc func(i, j int) bool

func getSortFunc(users []*ServerUser, orderField string, orderBy int) SortFunc {
	if (orderBy != OrderByAsc && orderBy != OrderByDesc) || orderBy == OrderByAsIs {
		return nil
	}

	switch orderField {
	case IdField:
		if orderBy == OrderByAsc {
			return func(i, j int) bool { return users[i].Id < users[j].Id }
		}
		return func(i, j int) bool { return users[i].Id > users[j].Id }
	case AgeField:
		if orderBy == OrderByAsc {
			return func(i, j int) bool { return users[i].Age < users[j].Age }
		}
		return func(i, j int) bool { return users[i].Age > users[j].Age }
	case NameField:
		if orderBy == OrderByAsc {
			return func(i, j int) bool { return users[i].Name < users[j].Name }
		}
		return func(i, j int) bool { return users[i].Name > users[j].Name }
	}

	return nil
}
