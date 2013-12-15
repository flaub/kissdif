package rql

import (
	_ "fmt"
	"github.com/flaub/kissdif"
	"net/http"
	_url "net/url"
)

type Conn interface {
	CreateDB(name, driver string, config kissdif.Dictionary) (Database, *kissdif.Error)
	DropDB(name string) *kissdif.Error
	get(query *queryImpl) (*kissdif.ResultSet, *kissdif.Error)
	put(query *queryImpl) (string, *kissdif.Error)
	delete(query *queryImpl) *kissdif.Error
}

type Database interface {
	DropTable(name string) ExecStmt
	Table(name string) Table
}

type ExecStmt interface {
	Run(conn Conn) *kissdif.Error
}

type SingleStmt interface {
	Run(conn Conn, doc interface{}) (*kissdif.Record, *kissdif.Error)
}

type PutStmt interface {
	Run(conn Conn) (string, *kissdif.Error)
	By(key, value string) PutStmt
}

type MultiStmt interface {
	Run(conn Conn) (*kissdif.ResultSet, *kissdif.Error)
}

type Bound struct {
	Open  bool
	Value string
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
}

func Connect(url string) (Conn, *kissdif.Error) {
	theUrl, err := _url.Parse(url)
	if err != nil {
		return nil, kissdif.NewError(http.StatusBadRequest, err.Error())
	}
	switch theUrl.Scheme {
	case "http", "https":
		return newHttpConn(url), nil
	case "local":
		return newLocalConn(), nil
	default:
		return nil, kissdif.NewError(http.StatusBadRequest, "Unrecognized connection scheme")
	}
}

func DB(name string) Database {
	return newQuery(name)
}
