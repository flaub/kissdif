package rql

import (
	"encoding/json"
	_ "fmt"
	"github.com/flaub/kissdif"
	"net/http"
	_url "net/url"
)

type ResultSet struct {
	More    bool
	Records []*_Record
}

type Record interface {
	Id() string
	Rev() string
	Doc(into interface{}) error
	Keys() kissdif.IndexMap
}

type _Record struct {
	_struct bool             `codec:",omitempty"` // set omitempty for every field
	Id_     string           `json:"id",omitempty`
	Rev_    string           `json:"rev",omitempty`
	Doc_    json.RawMessage  `json:"doc",omitempty`
	Keys_   kissdif.IndexMap `json:"keys",omitempty`
}

type Conn interface {
	CreateDB(name, driver string, config kissdif.Dictionary) (Database, *kissdif.Error)
	DropDB(name string) *kissdif.Error
	get(query *queryImpl) (*ResultSet, *kissdif.Error)
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
	Run(conn Conn) (Record, *kissdif.Error)
}

type PutStmt interface {
	Run(conn Conn) (string, *kissdif.Error)
	By(key, value string) PutStmt
}

type MultiStmt interface {
	Run(conn Conn) (*ResultSet, *kissdif.Error)
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
	// case "local":
	// 	return newLocalConn(), nil
	default:
		return nil, kissdif.NewError(http.StatusBadRequest, "Unrecognized connection scheme")
	}
}

func DB(name string) Database {
	return newQuery(name)
}

type RecordReader struct {
	records []*_Record
	index   int
}

func (this *_Record) Id() string {
	return this.Id_
}

func (this *_Record) Rev() string {
	return this.Rev_
}

func (this *_Record) Doc(into interface{}) error {
	return json.Unmarshal(this.Doc_, into)
}

func (this *_Record) Keys() kissdif.IndexMap {
	return this.Keys_
}

func (this *ResultSet) Reader() *RecordReader {
	return &RecordReader{records: this.Records}
}

func (this *RecordReader) Next() bool {
	if this.index == len(this.records) {
		return false
	}
	this.index++
	return true
}

func (this *RecordReader) Record(doc interface{}) error {
	record := this.records[this.index]
	return record.Doc(doc)
}
