package rql

import (
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

func (this *localConn) CreateDB(name, driverName string, config kissdif.Dictionary) (Database, *kissdif.Error) {
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

func (this *localConn) DropDB(name string) *kissdif.Error {
	return kissdif.NewError(http.StatusNotImplemented, "Not implemented")
}

func (this *localConn) get(impl *queryImpl) (*kissdif.ResultSet, *kissdif.Error) {
	db := this.getDb(impl.db)
	if db == nil {
		return nil, kissdif.NewError(http.StatusNotFound, "DB not found")
	}
	table, err := db.GetTable(impl.table, false)
	if err != nil {
		return nil, err
	}
	ch, err := table.Get(&impl.query)
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

func (this *localConn) put(impl *queryImpl) (string, *kissdif.Error) {
	db := this.getDb(impl.db)
	if db == nil {
		return "", kissdif.NewError(http.StatusNotFound, "DB not found")
	}
	table, err := db.GetTable(impl.table, true)
	if err != nil {
		return "", err
	}
	return table.Put(&impl.record)
}

func (this *localConn) delete(impl *queryImpl) *kissdif.Error {
	db := this.getDb(impl.db)
	if db == nil {
		return kissdif.NewError(http.StatusNotFound, "DB not found")
	}
	table, err := db.GetTable(impl.table, false)
	if err != nil {
		return err
	}
	return table.Delete(impl.record.Id)
}
