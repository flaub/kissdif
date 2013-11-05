package rql

import (
	"bytes"
	"encoding/json"
	"github.com/flaub/kissdif"
	"github.com/flaub/kissdif/driver"
	"net/http"
	"sync"
)

type localConn struct {
	dbs   map[string]driver.Environment
	mutex sync.RWMutex
}

func newLocalConn() *localConn {
	return &localConn{
		dbs: make(map[string]driver.Environment),
	}
}

func (this *localConn) getEnv(name string) driver.Environment {
	this.mutex.RLock()
	defer this.mutex.RUnlock()
	return this.dbs[name]
}

func (this *localConn) putEnv(name string, db driver.Environment) {
	this.mutex.Lock()
	defer this.mutex.Unlock()
	this.dbs[name] = db
}

func (this *localConn) CreateDB(name, driverName string, config kissdif.Dictionary) (IDatabase, *kissdif.Error) {
	drv, err := driver.Open(driverName)
	if err != nil {
		return nil, err
	}
	db, err := drv.Configure(name, config)
	if err != nil {
		return nil, err
	}
	this.putEnv(name, db)
	return newQuery(name), nil
}

func (this *localConn) DropDB(name string) *kissdif.Error {
	return kissdif.NewError(http.StatusNotImplemented, "Not implemented")
}

func (this *localConn) Get(query *queryImpl) (chan (*kissdif.Record), *kissdif.Error) {
	db := this.getEnv(query.db)
	if db == nil {
		return nil, kissdif.NewError(http.StatusNotFound, "DB not found")
	}
	table, err := db.GetTable(query.table, false)
	if err != nil {
		return nil, err
	}
	return table.Get(&query.query)
}

func (this *localConn) Put(query *queryImpl) (*kissdif.Record, *kissdif.Error) {
	db := this.getEnv(query.db)
	if db == nil {
		return nil, kissdif.NewError(http.StatusNotFound, "DB not found")
	}
	table, err := db.GetTable(query.table, true)
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	json.NewEncoder(&buf).Encode(query.doc)
	record := &kissdif.Record{
		Id:  query.id,
		Rev: query.rev,
		Doc: buf.String(),
	}
	return table.Put(record)
}

func (this *localConn) Delete(query *queryImpl) *kissdif.Error {
	db := this.getEnv(query.db)
	if db == nil {
		return kissdif.NewError(http.StatusNotFound, "DB not found")
	}
	table, err := db.GetTable(query.table, false)
	if err != nil {
		return err
	}
	return table.Delete(query.id)
}
