package rql

import (
	"github.com/flaub/kissdif"
	"net/http"
	_url "net/url"
)

type Conn interface {
	CreateDB(name, driver string, config kissdif.Dictionary) (IDatabase, *kissdif.Error)
	DropDB(name string) *kissdif.Error
}

type iConn interface {
	Get(query *queryImpl) (chan (*kissdif.Record), *kissdif.Error)
	Put(query *queryImpl) (*kissdif.Record, *kissdif.Error)
	Delete(query *queryImpl) *kissdif.Error
}

type IDatabase interface {
	DropTable(name string) ExecStmt
	Table(name string) Table
}

type ExecStmt interface {
	Run(conn Conn) *kissdif.Error
}

type SingleStmt interface {
	Run(conn Conn) (*kissdif.Record, *kissdif.Error)
}

type MultiStmt interface {
	Run(conn Conn) (chan (*kissdif.Record), *kissdif.Error)
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
	Index(index string) Query
}

type Table interface {
	Indexable
	Insert(id string, doc interface{}) SingleStmt
	Update(id, rev string, doc interface{}) SingleStmt
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
	db    string
	table string
	query kissdif.Query
	id    string
	rev   string
	doc   interface{}
}

func Connect(url string) (Conn, *kissdif.Error) {
	if url == "local" {
		return newLocalConn(), nil
	}
	theUrl, err := _url.Parse(url)
	if err != nil {
		return nil, kissdif.NewError(http.StatusBadRequest, err.Error())
	}
	if theUrl.Scheme == "http" {
	}
	return nil, kissdif.NewError(http.StatusBadRequest, "Unrecognized connection scheme")
}

func DB(name string) IDatabase {
	return newQuery(name)
}

func newQuery(db string) *queryImpl {
	this := &queryImpl{db: db}
	this.query.Index = "_id"
	this.query.Limit = 1000
	return this
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

func (this *queryImpl) Index(index string) Query {
	this.query.Index = index
	return this
}

func (this *queryImpl) Get(key string) SingleStmt {
	bound := &kissdif.Bound{true, key}
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

func (this *queryImpl) Insert(id string, doc interface{}) SingleStmt {
	this.id = id
	this.doc = doc
	return &putStmt{this}
}

func (this *queryImpl) Update(id, rev string, doc interface{}) SingleStmt {
	this.id = id
	this.rev = rev
	this.doc = doc
	return &putStmt{this}
}

func (this *queryImpl) Delete(id, rev string) ExecStmt {
	this.id = id
	this.rev = rev
	return &deleteStmt{this}
}

func (this *putStmt) Run(conn Conn) (*kissdif.Record, *kissdif.Error) {
	return conn.(iConn).Put(this.queryImpl)
}

func (this *deleteStmt) Run(conn Conn) *kissdif.Error {
	return conn.(iConn).Delete(this.queryImpl)
}

func (this *queryImpl) Run(conn Conn) (chan *kissdif.Record, *kissdif.Error) {
	return conn.(iConn).Get(this)
}

func (this *singleStmt) Run(conn Conn) (*kissdif.Record, *kissdif.Error) {
	if conn == nil {
		return nil, kissdif.NewError(http.StatusBadRequest, "conn must not be null")
	}
	ch, err := conn.(iConn).Get(this.queryImpl)
	if err != nil {
		return nil, err
	}
	var result *kissdif.Record
	for record := range ch {
		if record != nil {
			if result != nil {
				err = kissdif.NewError(http.StatusMultipleChoices, "Multiple records found")
			} else {
				result = record
			}
		}
	}
	if err != nil {
		return nil, err
	}
	return result, nil
}
