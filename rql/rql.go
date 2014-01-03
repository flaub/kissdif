package rql

import (
	"github.com/flaub/ergo"
	"github.com/flaub/kissdif"
	"net/http"
	_url "net/url"
)

type ResultSet interface {
	More() bool
	Count() int
	Reader() RecordReader
}

type RecordReader interface {
	Next() bool
	Record() Record
	Scan(into interface{}) (interface{}, error)
	MustScan(into interface{}) interface{}
}

type Record interface {
	Id() string
	Rev() string
	Keys() kissdif.IndexMap

	Scan(into interface{}) (interface{}, error)
	MustScan(into interface{}) interface{}

	Set(doc interface{}) error
	MustSet(doc interface{})
}

type Conn interface {
	CreateDB(name, driver string, config kissdif.Dictionary) (Database, *ergo.Error)
	DropDB(name string) *ergo.Error
	Get(impl QueryImpl) (ResultSet, *ergo.Error)
	Put(impl QueryImpl) (string, *ergo.Error)
	Delete(impl QueryImpl) *ergo.Error
}

type Database interface {
	DropTable(name string) ExecStmt
	Table(name string) Table
}

type ExecStmt interface {
	Exec(conn Conn) *ergo.Error
}

type SingleStmt interface {
	Exec(conn Conn) (Record, *ergo.Error)
}

type PutStmt interface {
	Exec(conn Conn) (string, *ergo.Error)
	By(key, value string) PutStmt
	Keys(keys kissdif.IndexMap) PutStmt
}

type MultiStmt interface {
	Exec(conn Conn) (ResultSet, *ergo.Error)
}

type Limitable interface {
	MultiStmt
	Limit(count uint) Query
}

type Query interface {
	Limitable
	Get(key string) SingleStmt
	GetAll(key string) Limitable
	Between(lower, upper string) Limitable
}

type Indexable interface {
	Query
	By(index string) Query
}

type Table interface {
	Indexable
	Insert(id string, doc interface{}) PutStmt
	Update(id, rev string, doc interface{}) PutStmt
	Delete(id, rev string) ExecStmt
	UpdateRecord(record Record) PutStmt
	DeleteRecord(record Record) ExecStmt
}

func Connect(url string) (Conn, *ergo.Error) {
	theUrl, err := _url.Parse(url)
	if err != nil {
		return nil, kissdif.NewError(http.StatusBadRequest, err.Error())
	}
	switch theUrl.Scheme {
	case "http", "https":
		return newHttpConn(url), nil
	// case "local":
	// 	return newLocalConn(), nil
	default:
		return nil, kissdif.NewError(http.StatusBadRequest, "Unrecognized connection scheme")
	}
}

func DB(name string) Database {
	return newQuery(name)
}
