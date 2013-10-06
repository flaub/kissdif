package main

import (
	"encoding/json"
	"fmt"
	"github.com/flaub/kissdif/driver"
	_ "github.com/flaub/kissdif/driver/mem"
	_ "github.com/flaub/kissdif/driver/sql"
	"github.com/gorilla/mux"
	"net/http"
	"sync"
)

func main() {
	fmt.Println("KISS Data Interface")
	server := NewServer()
	server.ListenAndServe()
}

type Environment struct {
	name   string
	driver driver.Driver
}

type Server struct {
	http.Server
	envs  map[string]*Environment
	mutex sync.RWMutex
}

func NewServer() *Server {
	router := mux.NewRouter()

	this := &Server{
		Server: http.Server{
			Addr:    ":8080",
			Handler: router,
		},
		envs: make(map[string]*Environment),
	}

	router.HandleFunc("/{env}/{table}/{indexName}/{indexValue:.+}", this.Get).
		Methods("GET")
	router.HandleFunc("/{env}/{table}/_id/{id:.+}", this.Put).
		Methods("PUT")
	router.HandleFunc("/{env}/{table}/_id/{id:.+}", this.Delete).
		Methods("DELETE")

	return this
}

func NewEnvironment(name string) *Environment {
	driver, err := driver.Open(name)
	if err != nil {
		panic(err)
	}
	return &Environment{
		name:   name,
		driver: driver,
	}
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

func (this *Server) sendJson(resp http.ResponseWriter, record *driver.Record) {
	resp.Header().Set("Content-Type", "application/json")
	json.NewEncoder(resp).Encode(record)
}

func (this *Server) getEnvironment(name string, create bool) (*Environment, *driver.Error) {
	if create {
		this.mutex.Lock()
		defer this.mutex.Unlock()
	} else {
		this.mutex.RLock()
		defer this.mutex.RUnlock()
	}
	env, ok := this.envs[name]
	if !ok {
		if !create {
			return nil, driver.NewError(http.StatusNotFound, "Environment not found")
		}
		fmt.Printf("Creating new environment: %v\n", name)
		env = NewEnvironment(name)
		this.envs[name] = env
	}
	return env, nil
}

func (this *Server) Put(resp http.ResponseWriter, req *http.Request) {
	contentType := req.Header.Get("Content-Type")
	if contentType != "application/json" {
		err := driver.NewError(http.StatusBadRequest, fmt.Sprintf("Invalid content type: %v", contentType))
		this.sendError(resp, err)
		return
	}
	vars := mux.Vars(req)
	env, err := this.getEnvironment(vars["env"], true)
	if err != nil {
		this.sendError(resp, err)
		return
	}
	table, err := env.getTable(vars["table"], true)
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

func (this *Server) Get(resp http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	env, err := this.getEnvironment(vars["env"], false)
	if err != nil {
		this.sendError(resp, err)
		return
	}
	table, err := env.getTable(vars["table"], false)
	if err != nil {
		this.sendError(resp, err)
		return
	}
	record, err := table.Get(vars["indexName"], vars["indexValue"])
	if err != nil {
		this.sendError(resp, err)
		return
	}
	resp.Write([]byte(record.Doc))
}

func (this *Server) Delete(resp http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	env, err := this.getEnvironment(vars["env"], true)
	if err != nil {
		this.sendError(resp, err)
		return
	}
	table, err := env.getTable(vars["table"], true)
	if err != nil {
		this.sendError(resp, err)
		return
	}
	table.Delete(vars["id"])
}

func (this *Environment) getTable(name string, create bool) (driver.Table, *driver.Error) {
	return this.driver.GetTable(name, create)
}
