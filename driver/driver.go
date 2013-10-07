package driver

import (
	"fmt"
	"net/http"
)

var drivers = make(map[string]Driver)

// Register makes a kissdif driver available by the provided name.
// If Register is called twice with the same name or if driver is nil,
// it panics.
func Register(name string, driver Driver) {
	if driver == nil {
		panic("kissdif: Register driver is nil")
	}
	if _, dup := drivers[name]; dup {
		panic("kissdif: Register called twice for driver " + name)
	}
	drivers[name] = driver
}

func Open(name string) (Driver, *Error) {
	driver, ok := drivers[name]
	if !ok {
		return nil, NewError(http.StatusNotFound, fmt.Sprintf("kissdif: unknown driver %q (forgotten import?)", name))
	}
	return driver, nil
}

type Driver interface {
	GetTable(name string, create bool) (Table, *Error)
}

type Bound struct {
	Inclusive bool
	Value     string
}

type Query struct {
	Index string
	Lower *Bound
	Upper *Bound
	Limit int
}

type Table interface {
	Get(query *Query) (chan (*Record), *Error)
	Put(record *Record) *Error
	Delete(id string) *Error
}

type Record struct {
	Id   string              `json:"id"`
	Rev  string              `json:"rev"`
	Doc  []byte              `json:"doc"`
	Keys map[string][]string `json:"keys",omitempty`
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
