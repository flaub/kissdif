package rql

import (
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
	CreateDB(name, driver string, config kissdif.Dictionary) (Database, error)
	DropDB(name string) error
	Get(impl QueryImpl) (ResultSet, error)
	Put(impl QueryImpl) (string, error)
	Delete(impl QueryImpl) error
}

type Database interface {
	DropTable(name string) ExecStmt
	Table(name string) Table
}

type ExecStmt interface {
	Exec(conn Conn) error
}

type SingleStmt interface {
	Exec(conn Conn) (Record, error)
}

type PutStmt interface {
	Exec(conn Conn) (string, error)
	By(key, value string) PutStmt
	Keys(keys kissdif.IndexMap) PutStmt
}

type MultiStmt interface {
	Exec(conn Conn) (ResultSet, error)
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

func Connect(url string) (Conn, error) {
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
