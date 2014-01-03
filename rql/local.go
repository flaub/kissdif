package rql

import (
	"github.com/flaub/ergo"
	"github.com/flaub/kissdif"
	"github.com/flaub/kissdif/driver"
	"net/http"
	"sync"
)

type localConn struct {
	dbs   map[string]driver.Database
	mutex sync.RWMutex
}

func newLocalConn() *localConn {
	return &localConn{
		dbs: make(map[string]driver.Database),
	}
}

func (this *localConn) getDb(name string) driver.Database {
	this.mutex.RLock()
	defer this.mutex.RUnlock()
	return this.dbs[name]
}

func (this *localConn) putDb(name string, db driver.Database) {
	this.mutex.Lock()
	defer this.mutex.Unlock()
	this.dbs[name] = db
}

func (this *localConn) CreateDB(name, driverName string, config kissdif.Dictionary) (Database, *ergo.Error) {
	drv, err := driver.Open(driverName)
	if err != nil {
		return nil, err
	}
	db, err := drv.Configure(name, config)
	if err != nil {
		return nil, err
	}
	this.putDb(name, db)
	return newQuery(name), nil
}

func (this *localConn) DropDB(name string) *ergo.Error {
	return kissdif.NewError(http.StatusNotImplemented, "Not implemented")
}

func (this *localConn) Get(impl QueryImpl) (*kissdif.ResultSet, *ergo.Error) {
	db := this.getDb(impl.Db_)
	if db == nil {
		return nil, kissdif.NewError(http.StatusNotFound, "DB not found")
	}
	table, err := db.GetTable(impl.Table_, false)
	if err != nil {
		return nil, err
	}
	ch, err := table.Get(&impl.Query_)
	if err != nil {
		return nil, err
	}
	result := &kissdif.ResultSet{
		More:    true,
		Records: []*kissdif.Record{},
	}
	for record := range ch {
		if record == nil {
			result.More = false
		} else {
			result.Records = append(result.Records, record)
		}
	}
	return result, nil
}

func (this *localConn) Put(impl QueryImpl) (string, *ergo.Error) {
	db := this.getDb(impl.Db_)
	if db == nil {
		return "", kissdif.NewError(http.StatusNotFound, "DB not found")
	}
	table, err := db.GetTable(impl.Table_, true)
	if err != nil {
		return "", err
	}
	return table.Put(&impl.Record_)
}

func (this *localConn) Delete(impl QueryImpl) *ergo.Error {
	db := this.getDb(impl.Db_)
	if db == nil {
		return kissdif.NewError(http.StatusNotFound, "DB not found")
	}
	table, err := db.GetTable(impl.Table_, false)
	if err != nil {
		return err
	}
	return table.Delete(impl.Record_.Id)
}
