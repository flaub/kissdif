package main

import (
	"encoding/json"
	"fmt"
	. "github.com/flaub/kissdif"
	"github.com/flaub/kissdif/driver"
	_ "github.com/flaub/kissdif/driver/mem"
	_ "github.com/flaub/kissdif/driver/sql"
	"github.com/gorilla/mux"
	"net/http"
	"net/url"
	"strconv"
	"sync"
)

func main() {
	fmt.Println("KISS Data Interface")
	server := NewServer()
	server.ListenAndServe()
}

type Server struct {
	http.Server
	envs  map[string]driver.Environment
	mutex sync.RWMutex
}

func NewServer() *Server {
	router := mux.NewRouter()

	this := &Server{
		Server: http.Server{
			Addr:    ":8080",
			Handler: router,
		},
		envs: make(map[string]driver.Environment),
	}

	router.HandleFunc("/{env}", this.putEnv).
		Methods("PUT")
	router.HandleFunc("/{env}/{table}/{index}", this.doQuery).
		Methods("GET")
	router.HandleFunc("/{env}/{table}/{index}/{key:.+}", this.getRecord).
		Methods("GET")
	router.HandleFunc("/{env}/{table}/_id/{id:.+}", this.putRecord).
		Methods("PUT")
	router.HandleFunc("/{env}/{table}/_id/{id:.+}", this.deleteRecord).
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

func (this *Server) findEnv(name string) (driver.Environment, *Error) {
	this.mutex.RLock()
	defer this.mutex.RUnlock()
	env, ok := this.envs[name]
	if !ok {
		return nil, NewError(http.StatusNotFound, "Environment not found")
	}
	return env, nil
}

func (this *Server) getEnv(resp http.ResponseWriter, req *http.Request) {
}

func (this *Server) putEnv(resp http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	name := vars["env"]

	var envJson EnvJson
	err := json.NewDecoder(req.Body).Decode(&envJson)
	if err != nil {
		this.sendError(resp, NewError(http.StatusBadRequest, err.Error()))
		return
	}
	db, err2 := driver.Open(envJson.Driver)
	if err2 != nil {
		this.sendError(resp, err2)
		return
	}
	env, err2 := db.Configure(name, envJson.Config)
	if err2 != nil {
		this.sendError(resp, err2)
		return
	}
	this.mutex.Lock()
	defer this.mutex.Unlock()
	this.envs[name] = env
}

func (this *Server) putRecord(resp http.ResponseWriter, req *http.Request) {
	contentType := req.Header.Get("Content-Type")
	if contentType != "application/json" {
		err := NewError(http.StatusBadRequest, fmt.Sprintf("Invalid content type: %v", contentType))
		this.sendError(resp, err)
		return
	}
	vars := mux.Vars(req)
	env, err := this.findEnv(vars["env"])
	if err != nil {
		this.sendError(resp, err)
		return
	}
	table, err := env.GetTable(vars["table"], true)
	if err != nil {
		this.sendError(resp, err)
		return
	}
	var record Record
	err2 := json.NewDecoder(req.Body).Decode(&record)
	if err2 != nil {
		this.sendError(resp, NewError(http.StatusBadRequest, err2.Error()))
		return
	}
	id := vars["id"]
	if record.Id != id {
		this.sendError(resp, NewError(http.StatusBadRequest, "ID Mismatch"))
		return
	}
	result, err = table.Put(&record)
	if err != nil {
		this.sendError(resp, err)
		return
	}
	this.sendJson(resp, result)
}

func (this *Server) doQuery(resp http.ResponseWriter, req *http.Request) {
	args := req.URL.Query()
	vars := mux.Vars(req)
	env, err := this.findEnv(vars["env"])
	if err != nil {
		this.sendError(resp, err)
		return
	}
	table, err := env.GetTable(vars["table"], false)
	if err != nil {
		this.sendError(resp, err)
		return
	}
	lower, upper, err := getBounds(args)
	if err != nil {
		this.sendError(resp, err)
		return
	}
	limit, err := getLimit(args)
	if err != nil {
		this.sendError(resp, err)
		return
	}
	query := NewQuery(vars["index"], lower, upper, limit)
	result, err := this.processQuery(table, query)
	if err != nil {
		this.sendError(resp, err)
		return
	}
	this.sendJson(resp, result)
}

func (this *Server) getRecord(resp http.ResponseWriter, req *http.Request) {
	args := req.URL.Query()
	vars := mux.Vars(req)
	env, err := this.findEnv(vars["env"])
	if err != nil {
		this.sendError(resp, err)
		return
	}
	table, err := env.GetTable(vars["table"], false)
	if err != nil {
		this.sendError(resp, err)
		return
	}
	limit, err := getLimit(args)
	if err != nil {
		this.sendError(resp, err)
		return
	}
	query := NewQueryEQ(vars["index"], vars["key"], limit)
	result, err := this.processQuery(table, query)
	if err != nil {
		this.sendError(resp, err)
		return
	}
	if len(result.Records) == 0 {
		this.sendError(resp, NewError(http.StatusNotFound, "Record not found"))
		return
	}
	this.sendJson(resp, result)
}

func (this *Server) deleteRecord(resp http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	env, err := this.findEnv(vars["env"])
	if err != nil {
		this.sendError(resp, err)
		return
	}
	table, err := env.GetTable(vars["table"], true)
	if err != nil {
		this.sendError(resp, err)
		return
	}
	table.Delete(vars["id"])
}

func (this *Server) processQuery(table driver.Table, query *Query) (*ResultSet, *Error) {
	ch, err := table.Get(query)
	if err != nil {
		return nil, err
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
