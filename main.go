package main

import (
	"encoding/json"
	"fmt"
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

type ResultSet struct {
	IsTruncated bool
	Count       int
	Records     []*driver.Record
}

type EnvJson struct {
	Name   string
	Config map[string]string
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

	// router.HandleFunc("/{env}", this.PutEnv).
	// 	Methods("PUT")
	router.HandleFunc("/{env}/{table}/{index}/{key:.+}", this.Get).
		Methods("GET")
	router.HandleFunc("/{env}/{table}/_id/{id:.+}", this.Put).
		Methods("PUT")
	router.HandleFunc("/{env}/{table}/_id/{id:.+}", this.Delete).
		Methods("DELETE")

	return this
}

func (this *Server) decodeBody(req *http.Request) (*driver.Record, *driver.Error) {
	var msg driver.Record
	err := json.NewDecoder(req.Body).Decode(&msg)
	if err != nil {
		return nil, driver.NewError(http.StatusBadRequest, err.Error())
	}
	return &msg, nil
}

func (this *Server) sendError(resp http.ResponseWriter, err *driver.Error) {
	resp.Header().Set("Content-Type", "application/json")
	resp.WriteHeader(err.Status)
	resp.Write([]byte(err.Error()))
}

func (this *Server) sendJson(resp http.ResponseWriter, data interface{}) {
	resp.Header().Set("Content-Type", "application/json")
	json.NewEncoder(resp).Encode(data)
}

func (this *Server) getEnvironment(name string) (driver.Environment, *driver.Error) {
	this.mutex.RLock()
	defer this.mutex.RUnlock()
	env, ok := this.envs[name]
	if !ok {
		return nil, driver.NewError(http.StatusNotFound, "Environment not found")
	}
	return env, nil
}

// func (this *Server) PutEnv(resp http.ResponseWriter, req *http.Request) {
// 	vars := mux.Vars(req)
// 	env, err := this.getEnvironment(vars["env"], false)
// 	if err != nil {
// 		this.sendError(resp, err)
// 		return
// 	}
// }

func (this *Server) Put(resp http.ResponseWriter, req *http.Request) {
	contentType := req.Header.Get("Content-Type")
	if contentType != "application/json" {
		err := driver.NewError(http.StatusBadRequest, fmt.Sprintf("Invalid content type: %v", contentType))
		this.sendError(resp, err)
		return
	}
	vars := mux.Vars(req)
	env, err := this.getEnvironment(vars["env"])
	if err != nil {
		this.sendError(resp, err)
		return
	}
	table, err := env.GetTable(vars["table"], true)
	if err != nil {
		this.sendError(resp, err)
		return
	}
	reqJson, err := this.decodeBody(req)
	if err != nil {
		this.sendError(resp, err)
		return
	}
	id := vars["id"]
	if reqJson.Id != id {
		this.sendError(resp, driver.NewError(http.StatusBadRequest, "ID Mismatch"))
		return
	}
	err = table.Put(reqJson)
	if err != nil {
		this.sendError(resp, err)
		return
	}
}

func getLimit(args url.Values) (int, *driver.Error) {
	var limit int = 1000
	strLimit := args.Get("limit")
	if strLimit != "" {
		var err error
		limit, err = strconv.Atoi(strLimit)
		if err != nil {
			return 0, driver.NewError(http.StatusBadRequest, err.Error())
		}
	}
	return limit, nil
}

func getBounds(args url.Values) (lower, upper *driver.Bound, err *driver.Error) {
	for k, v := range args {
		switch k {
		case "eq":
			if lower != nil || upper != nil {
				err = driver.NewError(http.StatusBadGateway, "Invalid query")
				return
			}
			if len(v) != 1 {
				err = driver.NewError(http.StatusBadGateway, "Invalid query")
				return
			}
			lower = &driver.Bound{true, v[0]}
			upper = &driver.Bound{true, v[0]}
			break
		case "lt":
			if upper != nil {
				err = driver.NewError(http.StatusBadGateway, "Invalid query")
				return
			}
			if len(v) != 1 {
				err = driver.NewError(http.StatusBadGateway, "Invalid query")
				return
			}
			upper = &driver.Bound{false, v[0]}
			break
		case "le":
			if upper != nil {
				err = driver.NewError(http.StatusBadGateway, "Invalid query")
				return
			}
			if len(v) != 1 {
				err = driver.NewError(http.StatusBadGateway, "Invalid query")
				return
			}
			upper = &driver.Bound{true, v[0]}
			break
		case "gt":
			if lower != nil {
				err = driver.NewError(http.StatusBadGateway, "Invalid query")
				return
			}
			if len(v) != 1 {
				err = driver.NewError(http.StatusBadGateway, "Invalid query")
				return
			}
			lower = &driver.Bound{false, v[0]}
			break
		case "ge":
			if lower != nil {
				err = driver.NewError(http.StatusBadGateway, "Invalid query")
				return
			}
			if len(v) != 1 {
				err = driver.NewError(http.StatusBadGateway, "Invalid query")
				return
			}
			lower = &driver.Bound{true, v[0]}
			break
		}
	}
	return
}

func (this *Server) Get(resp http.ResponseWriter, req *http.Request) {
	args := req.URL.Query()
	vars := mux.Vars(req)
	env, err := this.getEnvironment(vars["env"])
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
	lower, upper, err := getBounds(args)
	query := &driver.Query{
		Index: vars["index"],
		Limit: limit,
		Lower: lower,
		Upper: upper,
	}
	fmt.Printf("Get: %v", query)
	ch, err := table.Get(query)
	if err != nil {
		this.sendError(resp, err)
		return
	}
	result := ResultSet{
		IsTruncated: true,
		Records:     []*driver.Record{},
	}
	for record := range ch {
		if record == nil {
			result.IsTruncated = false
		} else {
			result.Count++
			result.Records = append(result.Records, record)
		}
	}
	if result.Count == 0 {
		this.sendError(resp, driver.NewError(http.StatusNotFound, "Record not found"))
		return
	}
	this.sendJson(resp, result)
}

func (this *Server) Delete(resp http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	env, err := this.getEnvironment(vars["env"])
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
