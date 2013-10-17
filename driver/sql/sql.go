package sql

import (
	"database/sql"
	"github.com/flaub/kissdif/driver"
	"net/http"
	"sync"
)

type Driver struct {
	mutex  sync.RWMutex
	stores map[string]*Store
}

type Store struct {
	name   string
	driver string
	dsn    string
}

type Table struct {
	name  string
	store *Store
}

const schema = `
CREATE TABLE {{.Table}}(
	_id INT NOT NULL,
	PRIMARY KEY(_id)
);
`

func init() {
	driver.Register("sql", NewDriver())
}

func NewDriver() *Driver {
	return &Driver{
		stores: make(map[string]*Store),
	}
}

func (this *Driver) Configure(name string, config driver.Dictionary) (driver.Store, *driver.Error) {
	store := &Store{
		name:   name,
		driver: config["driver"],
		dsn:    config["dsn"],
	}
	this.stores[name] = store
	return store, nil
}

func (this *Driver) Open(name string) (driver.Store, *driver.Error) {
	store, ok := this.stores[name]
	if !ok {
		return nil, driver.NewError(http.StatusNotFound, "Store not found")
	}
	return store, nil
}

func (this *Store) GetTable(name string, create bool) (driver.Table, *driver.Error) {
	db, err := sql.Open(this.driver, this.dsn)
	if err != nil {
		return nil, driver.NewError(http.StatusInternalServerError, err.Error())
	}
	return nil, nil
}

func (this *Table) Get(indexName, indexValue string) (*driver.Record, *driver.Error) {
	return nil, nil
}

func (this *Table) Put(record *driver.Record) *driver.Error {
	return nil
}

func (this *Table) Delete(id string) *driver.Error {
	return nil
}

func (this *Table) Query() *driver.Error {
	return nil
}
