package mem

import (
	"fmt"
	"github.com/flaub/kissdif/driver"
	"net/http"
	"sync"
)

type Driver struct {
	tables map[string]*Table
	mutex  sync.RWMutex
}

type Index struct {
	name  string
	index map[string]*driver.Record
}

type Table struct {
	name  string
	keys  map[string]*Index
	mutex sync.RWMutex
}

func init() {
	driver.Register("mem", NewDriver())
}

func NewDriver() *Driver {
	return &Driver{
		tables: make(map[string]*Table),
	}
}

func (this *Driver) GetTable(name string, create bool) (driver.Table, *driver.Error) {
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
			return nil, driver.NewError(http.StatusNotFound, "Table not found")
		}
		fmt.Printf("Creating new table: %v\n", name)
		table = NewTable(name)
		this.tables[name] = table
	}
	return table, nil
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
		index: make(map[string]*driver.Record),
	}
}

func (this *Table) Put(newRecord *driver.Record) *driver.Error {
	this.mutex.Lock()
	defer this.mutex.Unlock()
	primary := this.getIndex("_id")
	record, ok := primary.index[newRecord.Id]
	if ok {
		if newRecord.Rev != "" && newRecord.Rev != record.Rev {
			return driver.NewError(http.StatusConflict, "Document update conflict")
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

func (this *Table) Get(indexName, indexValue string) (*driver.Record, *driver.Error) {
	index := this.getIndex(indexName)
	if index == nil {
		return nil, driver.NewError(http.StatusNotFound, "Index not found")
	}
	record, ok := index.index[indexValue]
	if !ok {
		return nil, driver.NewError(http.StatusNotFound, "Record not found")
	}
	return record, nil
}

func (this *Table) Delete(id string) *driver.Error {
	this.mutex.Lock()
	defer this.mutex.Unlock()
	primary := this.getIndex("_id")
	record, ok := primary.index[id]
	if !ok {
		return nil
	}
	this.removeKeys(record)
	delete(primary.index, id)
	return nil
}

func (this *Table) Query() *driver.Error {
	return nil
}

func (this *Table) getIndex(name string) *Index {
	index, ok := this.keys[name]
	if !ok {
		return nil
	}
	return index
}

func (this *Table) removeKeys(record *driver.Record) {
	for name, values := range record.Keys {
		index := this.keys[name]
		for _, value := range values {
			delete(index.index, value)
		}
	}
}

func (this *Table) addKeys(record *driver.Record) {
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
