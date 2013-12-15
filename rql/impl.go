package rql

import (
	"github.com/flaub/kissdif"
	"github.com/mitchellh/mapstructure"
	"net/http"
)

type putStmt struct {
	*queryImpl
}

type getStmt struct {
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
	return &getStmt{this}
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
	return conn.put(this.queryImpl)
}

func (this *putStmt) By(key, value string) PutStmt {
	this.record.AddKey(key, value)
	return this
}

func (this *deleteStmt) Run(conn Conn) *kissdif.Error {
	return conn.delete(this.queryImpl)
}

func (this *queryImpl) Run(conn Conn) (*kissdif.ResultSet, *kissdif.Error) {
	return conn.get(this)
}

func (this *getStmt) Run(conn Conn, into interface{}) (*kissdif.Record, *kissdif.Error) {
	if conn == nil {
		return nil, kissdif.NewError(http.StatusBadRequest, "conn must not be null")
	}
	resultSet, err := conn.get(this.queryImpl)
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
	record := resultSet.Records[0]
	if into != nil {
		err := mapstructure.Decode(record.Doc, into)
		if err != nil {
			return nil, kissdif.NewError(http.StatusBadRequest, err.Error())
		}
		record.Doc = into
	}
	return record, nil
}
