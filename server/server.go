package server

import (
	"encoding/json"
	"fmt"
	. "github.com/flaub/kissdif"
	"github.com/flaub/kissdif/driver"
	"github.com/gorilla/mux"
	"net/http"
	"net/url"
	"strconv"
	"sync"
)

type Server struct {
	http.Server
	dbs   map[string]driver.Database
	mutex sync.RWMutex
}

func NewServer() *Server {
	router := mux.NewRouter()

	this := &Server{
		Server: http.Server{
			Addr:    ":8080",
			Handler: router,
		},
		dbs: make(map[string]driver.Database),
	}

	router.HandleFunc("/{db}", this.putDb).
		Methods("PUT")
	router.HandleFunc("/{db}/{table}/{index}", this.doQuery).
		Methods("GET")
	router.HandleFunc("/{db}/{table}/{index}/{key:.+}", this.getRecord).
		Methods("GET")
	router.HandleFunc("/{db}/{table}/_id/{key:.+}", this.putRecord).
		Methods("PUT")
	router.HandleFunc("/{db}/{table}/_id/{key:.+}", this.deleteRecord).
		Methods("DELETE")

	return this
}

func (this *Server) sendError(resp http.ResponseWriter, err *Error) {
	resp.Header().Set("Content-Type", "application/json")
	resp.WriteHeader(err.Status)
	resp.Write([]byte(err.Error()))
}

func (this *Server) sendJson(resp http.ResponseWriter, data interface{}) {
	resp.Header().Set("Content-Type", "application/json")
	json.NewEncoder(resp).Encode(data)
}

func (this *Server) findDb(name string) (driver.Database, *Error) {
	this.mutex.RLock()
	defer this.mutex.RUnlock()
	db, ok := this.dbs[name]
	if !ok {
		return nil, NewError(http.StatusNotFound, "Database not found")
	}
	return db, nil
}

func (this *Server) getVar(vars map[string]string, name string) (string, *Error) {
	raw, ok := vars[name]
	if !ok {
		return "", NewError(http.StatusInternalServerError, fmt.Sprintf("Missing route variable: %v", name))
	}
	value, err := url.QueryUnescape(raw)
	if err != nil {
		return "", NewError(http.StatusBadRequest, err.Error())
	}
	return value, nil
}

func (this *Server) getTable(vars map[string]string, create bool) (driver.Table, *Error) {
	dbName, kerr := this.getVar(vars, "db")
	if kerr != nil {
		return nil, kerr
	}
	db, kerr := this.findDb(dbName)
	if kerr != nil {
		return nil, kerr
	}
	tableName, kerr := this.getVar(vars, "table")
	if kerr != nil {
		return nil, kerr
	}
	return db.GetTable(tableName, create)
}

func (this *Server) putDb(resp http.ResponseWriter, req *http.Request) {
	fmt.Printf("PUT db: %v\n", req.URL)
	vars := mux.Vars(req)
	dbName, kerr := this.getVar(vars, "db")
	if kerr != nil {
		this.sendError(resp, kerr)
		return
	}
	var dbcfg DatabaseCfg
	err := json.NewDecoder(req.Body).Decode(&dbcfg)
	if err != nil {
		this.sendError(resp, NewError(http.StatusBadRequest, err.Error()))
		return
	}
	drv, kerr := driver.Open(dbcfg.Driver)
	if kerr != nil {
		this.sendError(resp, kerr)
		return
	}
	db, kerr := drv.Configure(dbName, dbcfg.Config)
	if kerr != nil {
		this.sendError(resp, kerr)
		return
	}
	this.mutex.Lock()
	defer this.mutex.Unlock()
	this.dbs[dbName] = db
}

func (this *Server) putRecord(resp http.ResponseWriter, req *http.Request) {
	fmt.Printf("PUT record: %v\n", req.URL)
	contentType := req.Header.Get("Content-Type")
	if contentType != "application/json" {
		kerr := NewError(http.StatusBadRequest, fmt.Sprintf("Invalid content type: %v", contentType))
		this.sendError(resp, kerr)
		return
	}
	vars := mux.Vars(req)
	table, kerr := this.getTable(vars, true)
	if kerr != nil {
		this.sendError(resp, kerr)
		return
	}
	var record Record
	err := json.NewDecoder(req.Body).Decode(&record)
	if err != nil {
		this.sendError(resp, NewError(http.StatusBadRequest, err.Error()))
		return
	}
	id, kerr := this.getVar(vars, "key")
	if kerr != nil {
		this.sendError(resp, kerr)
		return
	}
	if record.Id != id {
		this.sendError(resp, NewError(http.StatusBadRequest, "ID Mismatch"))
		return
	}
	rev, kerr := table.Put(&record)
	if kerr != nil {
		this.sendError(resp, kerr)
		return
	}
	this.sendJson(resp, rev)
}

func (this *Server) doQuery(resp http.ResponseWriter, req *http.Request) {
	fmt.Printf("GET records: %v\n", req.URL)
	args := req.URL.Query()
	vars := mux.Vars(req)
	table, kerr := this.getTable(vars, false)
	if kerr != nil {
		this.sendError(resp, kerr)
		return
	}
	lower, upper, kerr := getBounds(args)
	if kerr != nil {
		this.sendError(resp, kerr)
		return
	}
	limit, kerr := getLimit(args)
	if kerr != nil {
		this.sendError(resp, kerr)
		return
	}
	query := NewQuery(vars["index"], lower, upper, limit)
	result, kerr := this.processQuery(table, query)
	if kerr != nil {
		this.sendError(resp, kerr)
		return
	}
	this.sendJson(resp, result)
}

func (this *Server) getRecord(resp http.ResponseWriter, req *http.Request) {
	fmt.Printf("GET record: %v\n", req.URL)
	args := req.URL.Query()
	vars := mux.Vars(req)
	table, kerr := this.getTable(vars, false)
	if kerr != nil {
		this.sendError(resp, kerr)
		return
	}
	limit, kerr := getLimit(args)
	if kerr != nil {
		this.sendError(resp, kerr)
		return
	}
	query := NewQueryEQ(vars["index"], vars["key"], limit)
	result, kerr := this.processQuery(table, query)
	if kerr != nil {
		this.sendError(resp, kerr)
		return
	}
	if len(result.Records) == 0 {
		this.sendError(resp, NewError(http.StatusNotFound, "Record not found"))
		return
	}
	this.sendJson(resp, result)
}

func (this *Server) deleteRecord(resp http.ResponseWriter, req *http.Request) {
	fmt.Printf("DELETE record: %v\n", req.URL)
	vars := mux.Vars(req)
	table, kerr := this.getTable(vars, false)
	if kerr != nil {
		this.sendError(resp, kerr)
		return
	}
	key, kerr := this.getVar(vars, "key")
	if kerr != nil {
		this.sendError(resp, kerr)
		return
	}
	table.Delete(key)
}

func (this *Server) processQuery(table driver.Table, query *Query) (*ResultSet, *Error) {
	ch, kerr := table.Get(query)
	if kerr != nil {
		return nil, kerr
	}
	result := &ResultSet{
		More:    true,
		Records: []*Record{},
	}
	for record := range ch {
		if record == nil {
			result.More = false
		} else {
			result.Records = append(result.Records, record)
		}
	}
	return result, nil
}

func getLimit(args url.Values) (uint, *Error) {
	var limit uint64 = 1000
	strLimit := args.Get("limit")
	if strLimit != "" {
		var err error
		limit, err = strconv.ParseUint(strLimit, 10, 32)
		if err != nil {
			return 0, NewError(http.StatusBadRequest, err.Error())
		}
	}
	return uint(limit), nil
}

func getBounds(args url.Values) (lower, upper *Bound, err *Error) {
	for k, v := range args {
		switch k {
		case "eq":
			if lower != nil || upper != nil {
				err = NewError(http.StatusBadGateway, "Invalid query")
				return
			}
			if len(v) != 1 {
				err = NewError(http.StatusBadGateway, "Invalid query")
				return
			}
			lower = &Bound{true, v[0]}
			upper = &Bound{true, v[0]}
			break
		case "lt":
			if upper != nil {
				err = NewError(http.StatusBadGateway, "Invalid query")
				return
			}
			if len(v) != 1 {
				err = NewError(http.StatusBadGateway, "Invalid query")
				return
			}
			upper = &Bound{false, v[0]}
			break
		case "le":
			if upper != nil {
				err = NewError(http.StatusBadGateway, "Invalid query")
				return
			}
			if len(v) != 1 {
				err = NewError(http.StatusBadGateway, "Invalid query")
				return
			}
			upper = &Bound{true, v[0]}
			break
		case "gt":
			if lower != nil {
				err = NewError(http.StatusBadGateway, "Invalid query")
				return
			}
			if len(v) != 1 {
				err = NewError(http.StatusBadGateway, "Invalid query")
				return
			}
			lower = &Bound{false, v[0]}
			break
		case "ge":
			if lower != nil {
				err = NewError(http.StatusBadGateway, "Invalid query")
				return
			}
			if len(v) != 1 {
				err = NewError(http.StatusBadGateway, "Invalid query")
				return
			}
			lower = &Bound{true, v[0]}
			break
		}
	}
	return
}
