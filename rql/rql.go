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
}

type iConn interface {
	Get(query *queryImpl) (*kissdif.ResultSet, *kissdif.Error)
	Put(query *queryImpl) (string, *kissdif.Error)
	Delete(query *queryImpl) *kissdif.Error
}

type Database interface {
	DropTable(name string) ExecStmt
	Table(name string) Table
}

type ExecStmt interface {
	Run(conn Conn) *kissdif.Error
}

type SingleStmt interface {
	Run(conn Conn) (*kissdif.Record, *kissdif.Error)
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

type putStmt struct {
	*queryImpl
}

type singleStmt struct {
	*queryImpl
}

type deleteStmt struct {
	*queryImpl
}

type queryImpl struct {
	db     string
	table  string
	record kissdif.Record
	query  kissdif.Query
	doc    interface{}
}

func Connect(url string) (Conn, *kissdif.Error) {
	theUrl, err := _url.Parse(url)
	if err != nil {
		return nil, kissdif.NewError(http.StatusBadRequest, err.Error())
	}
	switch theUrl.Scheme {
	case "http":
		fallthrough
	case "https":
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

func newQuery(db string) *queryImpl {
	return &queryImpl{
		db: db,
		record: kissdif.Record{
			Keys: make(kissdif.IndexMap),
		},
		query: kissdif.Query{
			Index: "_id",
			Limit: 1000,
		},
	}
}

func (this *queryImpl) DropTable(name string) ExecStmt {
	this.table = name
	return nil
}

func (this *queryImpl) Table(name string) Table {
	this.table = name
	return this
}

func (this *queryImpl) Limit(count uint) Query {
	this.query.Limit = count
	return this
}

func (this *queryImpl) By(index string) Query {
	this.query.Index = index
	return this
}

func (this *queryImpl) Get(key string) SingleStmt {
	bound := &kissdif.Bound{true, key}
	this.query.Limit = 1
	this.query.Lower = bound
	this.query.Upper = bound
	return &singleStmt{this}
}

func (this *queryImpl) GetAll(key string) Limitable {
	bound := &kissdif.Bound{true, key}
	this.query.Lower = bound
	this.query.Upper = bound
	return this
}

func (this *queryImpl) Between(lower, upper string) Limitable {
	this.query.Lower = &kissdif.Bound{true, lower}
	this.query.Upper = &kissdif.Bound{false, upper}
	return this
}

func (this *queryImpl) Insert(id string, doc interface{}) PutStmt {
	this.record.Id = id
	this.record.Doc = doc
	return &putStmt{this}
}

func (this *queryImpl) Update(id, rev string, doc interface{}) PutStmt {
	this.record.Id = id
	this.record.Rev = rev
	this.record.Doc = doc
	return &putStmt{this}
}

func (this *queryImpl) Delete(id, rev string) ExecStmt {
	this.record.Id = id
	this.record.Rev = rev
	return &deleteStmt{this}
}

func (this *putStmt) Run(conn Conn) (string, *kissdif.Error) {
	return conn.(iConn).Put(this.queryImpl)
}

func (this *putStmt) By(key, value string) PutStmt {
	this.record.AddKey(key, value)
	return this
}

func (this *deleteStmt) Run(conn Conn) *kissdif.Error {
	return conn.(iConn).Delete(this.queryImpl)
}

func (this *queryImpl) Run(conn Conn) (*kissdif.ResultSet, *kissdif.Error) {
	return conn.(iConn).Get(this)
}

func (this *singleStmt) Run(conn Conn) (*kissdif.Record, *kissdif.Error) {
	if conn == nil {
		return nil, kissdif.NewError(http.StatusBadRequest, "conn must not be null")
	}
	resultSet, err := conn.(iConn).Get(this.queryImpl)
	if err != nil {
		return nil, err
	}
	// fmt.Printf("RS: %v\n", resultSet)
	if resultSet.More || len(resultSet.Records) > 1 {
		return nil, kissdif.NewError(http.StatusMultipleChoices, "Multiple records found")
	}
	if len(resultSet.Records) == 0 {
		return nil, kissdif.NewError(http.StatusNotFound, "Record not found")
	}
	return resultSet.Records[0], nil
}
