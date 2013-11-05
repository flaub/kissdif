package driver

import (
	"fmt"
	. "github.com/flaub/kissdif"
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
		return nil, NewError(http.StatusNotFound,
			fmt.Sprintf("kissdif: unknown driver %q (forgotten import?)", name))
	}
	return driver, nil
}

type Driver interface {
	Configure(name string, config Dictionary) (Environment, *Error)
}

type Environment interface {
	Name() string
	Driver() string
	Config() Dictionary
	GetTable(name string, create bool) (Table, *Error)
}

type Table interface {
	Get(query *Query) (chan (*Record), *Error)
	Put(record *Record) (*Record, *Error)
	Delete(id string) *Error
}
