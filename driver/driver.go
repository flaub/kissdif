package driver

import (
	"github.com/flaub/ergo"
	. "github.com/flaub/kissdif"
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

func Open(name string) (Driver, *ergo.Error) {
	driver, ok := drivers[name]
	if !ok {
		return nil, NewError(EMissingDriver, "name", name)
	}
	return driver, nil
}

type Driver interface {
	Configure(name string, config Dictionary) (Database, *ergo.Error)
}

type Database interface {
	Name() string
	Driver() string
	Config() Dictionary
	GetTable(name string, create bool) (Table, *ergo.Error)
}

type Table interface {
	Get(query *Query) (chan (*Record), *ergo.Error)
	Put(record *Record) (string, *ergo.Error)
	Delete(id string) *ergo.Error
}
