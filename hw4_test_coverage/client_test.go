package main

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
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
	errBadLimit       = NewServerError(http.StatusBadRequest, "ErrorBadLimit")
	errBadOffset      = NewServerError(http.StatusBadRequest, "ErrorBadOffset")
	errBadOrderBy     = NewServerError(http.StatusBadRequest, "ErrorBadOrderBy")
	errBadOrderField  = NewServerError(http.StatusBadRequest, "ErrorBadOrderField")
	errUnauthorized   = NewServerError(http.StatusUnauthorized, "ErrorUnauthorized")
	errInternalServer = NewServerError(http.StatusInternalServerError, "ErrorInternalServer")
)

const (
	IdField   = "Id"
	AgeField  = "Age"
	NameField = "Name"
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
	case IdField, NameField, AgeField:
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

const AccessToken = "aGVsbG86d29ybGQ="

func SearchServer(w http.ResponseWriter, req *http.Request) {
	if req.Header.Get("AccessToken") != AccessToken {
		sendErrorJSONResponse(w, errUnauthorized)
		return
	}

	extractor := QueryParamsExtractor{params: req.URL.Query()}
	qParams, err := extractor.Extract()
	if err != nil {
		sendErrorJSONResponse(w, err)
		return
	}

	switch qParams.Query {
	case "__success_json_response":
		sendInvalidJSONResponse(w, http.StatusOK)
		return
	case "__bad_json_response":
		sendInvalidJSONResponse(w, http.StatusBadRequest)
		return
	case "__timeout":
		time.Sleep(2 * time.Second)
		fallthrough
	case "__internal_server_err":
		sendErrorJSONResponse(w, errInternalServer)
		return
	}

	users, err := getUsersFromFile("./dataset.xml", qParams)
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

func sendInvalidJSONResponse(w http.ResponseWriter, statusCode int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_, err := w.Write([]byte(`[{`))
	if err != nil {
		log.Println(err)
		return
	}
}

func getUsers(reader io.Reader, qParams QueryParams) ([]*ServerUser, error) {
	users := make([]*ServerUser, 0)
	decoder := xml.NewDecoder(reader)
	pattern, err := regexp.Compile(`\s+`)
	if err != nil {
		return nil, err
	}

	query := strings.ToLower(qParams.Query)
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
				if query != "" && !filterUser(user, query) {
					continue
				}

				about := pattern.ReplaceAllString(user.About, " ")
				if about[len(about)-1] == ' ' {
					about = about[:len(about)-1]
				}

				user.About = about
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
	name := strings.ToLower(user.Name)
	about := strings.ToLower(user.About)

	return strings.Contains(name, query) || strings.Contains(about, query)
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

type UserTestCase struct {
	QueryParams SearchRequest
	IsError     bool
}

func TestFindUsers(t *testing.T) {
	testCases := []UserTestCase{
		{
			QueryParams: SearchRequest{Query: "lorem a"},
		},
		{
			QueryParams: SearchRequest{Query: "lorem a", Limit: 20},
		},
		{
			QueryParams: SearchRequest{Limit: 100},
		},
		{
			QueryParams: SearchRequest{Limit: -1},
			IsError:     true,
		},
		{
			QueryParams: SearchRequest{Offset: -1},
			IsError:     true,
		},
		{
			QueryParams: SearchRequest{OrderBy: 10},
			IsError:     true,
		},
		{
			QueryParams: SearchRequest{OrderField: "UnknownField"},
			IsError:     true,
		},
		{
			QueryParams: SearchRequest{Query: "__success_json_response"},
			IsError:     true,
		},
		{
			QueryParams: SearchRequest{Query: "__bad_json_response"},
			IsError:     true,
		},
		{
			QueryParams: SearchRequest{Query: "__timeout"},
			IsError:     true,
		},
		{
			QueryParams: SearchRequest{Query: "__internal_server_err"},
			IsError:     true,
		},
	}

	server := httptest.NewServer(http.HandlerFunc(SearchServer))
	client := &SearchClient{
		URL:         server.URL,
		AccessToken: AccessToken,
	}

	for testNum, testCase := range testCases {
		_, err := client.FindUsers(testCase.QueryParams)

		if testCase.IsError && err == nil {
			t.Errorf("[testcase %d]: got nil, expected error", testNum)
		}
		if !testCase.IsError && err != nil {
			t.Errorf("[testcase %d]: unexpected error %v", testNum, err)
		}
	}
}

func TestFindUserUnauthorized(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(SearchServer))
	client := &SearchClient{URL: server.URL}

	if _, err := client.FindUsers(SearchRequest{Query: "tim"}); err == nil {
		t.Errorf("got nil, expected error")
	}
}

func TestFindUserBadURL(t *testing.T) {
	client := &SearchClient{URL: "http://localhost:100", AccessToken: AccessToken}
	if _, err := client.FindUsers(SearchRequest{Query: "tim"}); err == nil {
		t.Errorf("got nil, expected error")
	}
}

type ServerTestCase struct {
	StatusCode  int
	QueryParams url.Values
	Result      []ServerUser
}

var testUsers = []ServerUser{
	{
		Id:     0,
		Name:   "Boyd Wolf",
		Age:    22,
		About:  "Nulla cillum enim voluptate consequat laborum esse excepteur occaecat commodo nostrud excepteur ut cupidatat. Occaecat minim incididunt ut proident ad sint nostrud ad laborum sint pariatur. Ut nulla commodo dolore officia. Consequat anim eiusmod amet commodo eiusmod deserunt culpa. Ea sit dolore nostrud cillum proident nisi mollit est Lorem pariatur. Lorem aute officia deserunt dolor nisi aliqua consequat nulla nostrud ipsum irure id deserunt dolore. Minim reprehenderit nulla exercitation labore ipsum.",
		Gender: "male",
	},
	{
		Id:     5,
		Name:   "Beulah Stark",
		Age:    30,
		About:  "Enim cillum eu cillum velit labore. In sint esse nulla occaecat voluptate pariatur aliqua aliqua non officia nulla aliqua. Fugiat nostrud irure officia minim cupidatat laborum ad incididunt dolore. Fugiat nostrud eiusmod ex ea nulla commodo. Reprehenderit sint qui anim non ad id adipisicing qui officia Lorem.",
		Gender: "female",
	},
	{
		Id:     6,
		Name:   "Jennings Mays",
		Age:    39,
		About:  "Veniam consectetur non non aliquip exercitation quis qui. Aliquip duis ut ad commodo consequat ipsum cupidatat id anim voluptate deserunt enim laboris. Sunt nostrud voluptate do est tempor esse anim pariatur. Ea do amet Lorem in mollit ipsum irure Lorem exercitation. Exercitation deserunt adipisicing nulla aute ex amet sint tempor incididunt magna. Quis et consectetur dolor nulla reprehenderit culpa laboris voluptate ut mollit. Qui ipsum nisi ullamco sit exercitation nisi magna fugiat anim consectetur officia.",
		Gender: "male",
	},
	{
		Id:     7,
		Name:   "Leann Travis",
		Age:    34,
		About:  "Lorem magna dolore et velit ut officia. Cupidatat deserunt elit mollit amet nulla voluptate sit. Quis aute aliquip officia deserunt sint sint nisi. Laboris sit et ea dolore consequat laboris non. Consequat do enim excepteur qui mollit consectetur eiusmod laborum ut duis mollit dolor est. Excepteur amet duis enim laborum aliqua nulla ea minim.",
		Gender: "female",
	},
	{
		Id:     8,
		Name:   "Glenn Jordan",
		Age:    29,
		About:  "Duis reprehenderit sit velit exercitation non aliqua magna quis ad excepteur anim. Eu cillum cupidatat sit magna cillum irure occaecat sunt officia officia deserunt irure. Cupidatat dolor cupidatat ipsum minim consequat Lorem adipisicing. Labore fugiat cupidatat nostrud voluptate ea eu pariatur non. Ipsum quis occaecat irure amet esse eu fugiat deserunt incididunt Lorem esse duis occaecat mollit.",
		Gender: "male",
	},
	{
		Id:     11,
		Name:   "Gilmore Guerra",
		Age:    32,
		About:  "Labore consectetur do sit et mollit non incididunt. Amet aute voluptate enim et sit Lorem elit. Fugiat proident ullamco ullamco sint pariatur deserunt eu nulla consectetur culpa eiusmod. Veniam irure et deserunt consectetur incididunt ad ipsum sint. Consectetur voluptate adipisicing aute fugiat aliquip culpa qui nisi ut ex esse ex. Sint et anim aliqua pariatur.",
		Gender: "male",
	},
	{
		Id:     12,
		Name:   "Cruz Guerrero",
		Age:    36,
		About:  "Sunt enim ad fugiat minim id esse proident laborum magna magna. Velit anim aliqua nulla laborum consequat veniam reprehenderit enim fugiat ipsum mollit nisi. Nisi do reprehenderit aute sint sit culpa id Lorem proident id tempor. Irure ut ipsum sit non quis aliqua in voluptate magna. Ipsum non aliquip quis incididunt incididunt aute sint. Minim dolor in mollit aute duis consectetur.",
		Gender: "male",
	},
	{
		Id:     20,
		Name:   "Lowery York",
		Age:    27,
		About:  "Dolor enim sit id dolore enim sint nostrud deserunt. Occaecat minim enim veniam proident mollit Lorem irure ex. Adipisicing pariatur adipisicing aliqua amet proident velit. Magna commodo culpa sit id.",
		Gender: "male",
	},
	{
		Id:     23,
		Name:   "Gates Spencer",
		Age:    21,
		About:  "Dolore magna magna commodo irure. Proident culpa nisi veniam excepteur sunt qui et laborum tempor. Qui proident Lorem commodo dolore ipsum.",
		Gender: "male",
	},
	{
		Id:     34,
		Name:   "Kane Sharp",
		Age:    34,
		About:  "Lorem proident sint minim anim commodo cillum. Eiusmod velit culpa commodo anim consectetur consectetur sint sint labore. Mollit consequat consectetur magna nulla veniam commodo eu ut et. Ut adipisicing qui ex consectetur officia sint ut fugiat ex velit cupidatat fugiat nisi non. Dolor minim mollit aliquip veniam nostrud. Magna eu aliqua Lorem aliquip.",
		Gender: "male",
	},
}

func TestSearchServerHandler(t *testing.T) {
	testCases := []ServerTestCase{
		{
			StatusCode:  http.StatusOK,
			QueryParams: url.Values{"query": []string{"lorem"}},
			Result:      testUsers,
		},
		{
			StatusCode:  http.StatusOK,
			QueryParams: url.Values{"query": []string{"lorem"}, "order_field": []string{"Id"}, "order_by": []string{"-1"}},
			Result: func() []ServerUser {
				users := make([]ServerUser, len(testUsers))
				copy(users, testUsers)

				sort.Slice(users, func(i, j int) bool { return users[i].Id < users[j].Id })
				return users
			}(),
		},
		{
			StatusCode:  http.StatusOK,
			QueryParams: url.Values{"query": []string{"lorem"}, "order_field": []string{"Id"}, "order_by": []string{"1"}},
			Result: func() []ServerUser {
				users := make([]ServerUser, len(testUsers))
				copy(users, testUsers)

				sort.Slice(users, func(i, j int) bool { return users[i].Id > users[j].Id })
				return users
			}(),
		},
		{
			StatusCode:  http.StatusOK,
			QueryParams: url.Values{"query": []string{"lorem"}, "order_field": []string{"Age"}, "order_by": []string{"-1"}},
			Result: func() []ServerUser {
				users := make([]ServerUser, len(testUsers))
				copy(users, testUsers)

				sort.Slice(users, func(i, j int) bool { return users[i].Age < users[j].Age })
				return users
			}(),
		},
		{
			StatusCode:  http.StatusOK,
			QueryParams: url.Values{"query": []string{"lorem"}, "order_field": []string{"Age"}, "order_by": []string{"1"}},
			Result: func() []ServerUser {
				users := make([]ServerUser, len(testUsers))
				copy(users, testUsers)

				sort.Slice(users, func(i, j int) bool { return users[i].Age > users[j].Age })
				return users
			}(),
		},
		{
			StatusCode:  http.StatusOK,
			QueryParams: url.Values{"query": []string{"lorem"}, "order_field": []string{"Name"}, "order_by": []string{"-1"}},
			Result: func() []ServerUser {
				users := make([]ServerUser, len(testUsers))
				copy(users, testUsers)

				sort.Slice(users, func(i, j int) bool { return users[i].Name < users[j].Name })
				return users
			}(),
		},
		{
			StatusCode:  http.StatusOK,
			QueryParams: url.Values{"query": []string{"lorem"}, "order_field": []string{"Name"}, "order_by": []string{"1"}},
			Result: func() []ServerUser {
				users := make([]ServerUser, len(testUsers))
				copy(users, testUsers)

				sort.Slice(users, func(i, j int) bool { return users[i].Name > users[j].Name })
				return users
			}(),
		},
		{
			StatusCode:  http.StatusOK,
			QueryParams: url.Values{"query": []string{"lorem"}, "order_by": []string{"-1"}, "offset": []string{"2"}, "limit": []string{"3"}},
			Result: func() []ServerUser {
				users := make([]ServerUser, len(testUsers))
				copy(users, testUsers)

				sort.Slice(users, func(i, j int) bool { return users[i].Name < users[j].Name })
				return users[2:5]
			}(),
		},
		{
			StatusCode:  http.StatusBadRequest,
			QueryParams: url.Values{"order_field": []string{"unknown123"}},
		},
		{
			StatusCode:  http.StatusBadRequest,
			QueryParams: url.Values{"order_by": []string{"hello"}},
		},
		{
			StatusCode:  http.StatusBadRequest,
			QueryParams: url.Values{"order_by": []string{"123"}},
		},
		{
			StatusCode:  http.StatusBadRequest,
			QueryParams: url.Values{"offset": []string{"abc"}},
		},
		{
			StatusCode:  http.StatusBadRequest,
			QueryParams: url.Values{"offset": []string{"-10"}},
		},
		{
			StatusCode:  http.StatusBadRequest,
			QueryParams: url.Values{"limit": []string{"hello"}},
		},
		{
			StatusCode:  http.StatusBadRequest,
			QueryParams: url.Values{"limit": []string{"-30"}},
		},
	}

	for num, testCase := range testCases {
		testName := fmt.Sprintf("testcase %d", num)
		t.Run(testName, func(t *testing.T) {
			rec := httptest.NewRecorder()

			query := testCase.QueryParams.Encode()
			req := httptest.NewRequest(http.MethodGet, "/search?"+query, nil)
			req.Header.Set("AccessToken", AccessToken)

			SearchServer(rec, req)

			resp := rec.Result()
			defer func() { _ = resp.Body.Close() }()

			if resp.StatusCode != testCase.StatusCode {
				t.Errorf(
					"wrong status code, got %d, expected %d",
					resp.StatusCode, testCase.StatusCode,
				)
			}

			if testCase.Result == nil {
				return
			}

			var users []ServerUser
			if err := json.NewDecoder(resp.Body).Decode(&users); err != nil {
				t.Errorf("decoding response body unexpected error %v", err)
			}

			if !reflect.DeepEqual(users, testCase.Result) {
				t.Errorf("wrong result, got %v, expected %v", users, testCase.Result)
			}
		})
	}
}

func TestSearchServerInvalidJSONResponse(t *testing.T) {
	testCases := []ServerTestCase{
		{
			StatusCode:  http.StatusOK,
			QueryParams: url.Values{"query": []string{"__success_json_response"}},
		},
		{
			StatusCode:  http.StatusBadRequest,
			QueryParams: url.Values{"query": []string{"__bad_json_response"}},
		},
		{
			StatusCode:  http.StatusInternalServerError,
			QueryParams: url.Values{"query": []string{"__timeout"}},
		},
		{
			StatusCode:  http.StatusInternalServerError,
			QueryParams: url.Values{"query": []string{"__internal_server_err"}},
		},
	}

	for num, testCase := range testCases {
		testName := fmt.Sprintf("testcase %d", num)
		t.Run(testName, func(t *testing.T) {
			rec := httptest.NewRecorder()

			query := testCase.QueryParams.Encode()
			req := httptest.NewRequest(http.MethodGet, "/search?"+query, nil)
			req.Header.Set("AccessToken", AccessToken)

			SearchServer(rec, req)

			resp := rec.Result()
			defer func() { _ = resp.Body.Close() }()

			if resp.StatusCode != testCase.StatusCode {
				t.Errorf(
					"wrong status code, got %d, expected %d",
					resp.StatusCode, testCase.StatusCode,
				)
			}

			var users []ServerUser
			if err := json.NewDecoder(resp.Body).Decode(&users); err == nil {
				t.Errorf("expecting decoding response body error, got nil")
			}
		})
	}
}

func TestSearchServerUnauthorized(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/search", nil)

	SearchServer(rec, req)

	resp := rec.Result()
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("wrong status code, got %d, expected %d", resp.StatusCode, http.StatusUnauthorized)
	}
}
