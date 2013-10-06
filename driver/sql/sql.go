package sql

import (
	"github.com/flaub/kissdif/driver"
	"sync"
)

type Driver struct {
	mutex sync.RWMutex
}

type Table struct {
	name string
}

func init() {
	driver.Register("sql", NewDriver())
}

func NewDriver() *Driver {
	return &Driver{}
}

func (this *Driver) GetTable(name string, create bool) (driver.Table, *driver.Error) {
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
