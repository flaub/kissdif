package main

import (
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"net/http"
	"sync"
)

func main() {
	fmt.Println("KISS Data Interface")
	server := NewServer()
	server.ListenAndServe()
}

type Record struct {
	Id   string              `json:"id"`
	Rev  string              `json:"rev"`
	Doc  []byte              `json:"doc"`
	Keys map[string][]string `json:"keys",omitempty`
}

type Index struct {
	name  string
	index map[string]*Record
}

type Table struct {
	name  string
	keys  map[string]*Index
	mutex sync.RWMutex
}

type Environment struct {
	name   string
	tables map[string]*Table
	mutex  sync.RWMutex
}

type Server struct {
	http.Server
	envs  map[string]*Environment
	mutex sync.RWMutex
}

type Error struct {
	Status  int
	Message string
}

func NewError(status int, message string) *Error {
	return &Error{status, message}
}

func (this *Error) Error() string {
	return this.Message
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
	return &Environment{
		name:   name,
		tables: make(map[string]*Table),
	}
}

func NewTable(name string) *Table {
	this := &Table{
		name: name,
		keys: make(map[string]*Index),
	}
	this.keys["_id"] = NewIndex("_id")
	return this
}

func NewIndex(name string) *Index {
	return &Index{
		name:  name,
		index: make(map[string]*Record),
	}
}

func (this *Server) decodeBody(req *http.Request) (*Record, *Error) {
	var msg Record
	err := json.NewDecoder(req.Body).Decode(&msg)
	if err != nil {
		return nil, NewError(http.StatusBadRequest, err.Error())
	}
	return &msg, nil
}

func (this *Server) sendError(resp http.ResponseWriter, err *Error) {
	resp.Header().Set("Content-Type", "application/json")
	resp.WriteHeader(err.Status)
	resp.Write([]byte(err.Error()))
}

func (this *Server) sendJson(resp http.ResponseWriter, record *Record) {
	resp.Header().Set("Content-Type", "application/json")
	json.NewEncoder(resp).Encode(record)
}

func (this *Server) getEnvironment(name string, create bool) (*Environment, *Error) {
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
			return nil, NewError(http.StatusNotFound, "Environment not found")
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
		err := NewError(http.StatusBadRequest, fmt.Sprintf("Invalid content type: %v", contentType))
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
		this.sendError(resp, NewError(http.StatusBadRequest, "ID Mismatch"))
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
	table.mutex.RLock()
	defer table.mutex.RUnlock()
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

func (this *Environment) getTable(name string, create bool) (*Table, *Error) {
	if create {
		this.mutex.Lock()
		defer this.mutex.Unlock()
	} else {
		this.mutex.RLock()
		defer this.mutex.RUnlock()
	}
	table, ok := this.tables[name]
	if !ok {
		if !create {
			return nil, NewError(http.StatusNotFound, "Table not found")
		}
		fmt.Printf("Creating new table: %v\n", name)
		table = NewTable(name)
		this.tables[name] = table
	}
	return table, nil
}

func (this *Table) Put(newRecord *Record) *Error {
	this.mutex.Lock()
	defer this.mutex.Unlock()
	primary := this.getIndex("_id")
	record, ok := primary.index[newRecord.Id]
	if ok {
		if newRecord.Rev != "" && newRecord.Rev != record.Rev {
			return NewError(http.StatusConflict, "Document update conflict")
		}
		this.removeKeys(record)
		record.Rev = newRecord.Rev
		record.Doc = newRecord.Doc
	} else {
		record = newRecord
		primary.index[record.Id] = record
	}
	this.addKeys(record)
	return nil
}

func (this *Table) Get(indexName, indexValue string) (*Record, *Error) {
	index := this.getIndex(indexName)
	if index == nil {
		return nil, NewError(http.StatusNotFound, "Index not found")
	}
	record, ok := index.index[indexValue]
	if !ok {
		return nil, NewError(http.StatusNotFound, "Record not found")
	}
	return record, nil
}

func (this *Table) Delete(id string) {
	this.mutex.Lock()
	defer this.mutex.Unlock()
	primary := this.getIndex("_id")
	record, ok := primary.index[id]
	if !ok {
		return
	}
	this.removeKeys(record)
	delete(primary.index, id)
}

func (this *Table) getIndex(name string) *Index {
	index, ok := this.keys[name]
	if !ok {
		return nil
	}
	return index
}

func (this *Table) removeKeys(record *Record) {
	for name, values := range record.Keys {
		index := this.keys[name]
		for _, value := range values {
			delete(index.index, value)
		}
	}
}

func (this *Table) addKeys(record *Record) {
	for name, values := range record.Keys {
		index, ok := this.keys[name]
		if !ok {
			index = NewIndex(name)
			this.keys[name] = index
		}
		for _, value := range values {
			index.index[value] = record
		}
	}
}
