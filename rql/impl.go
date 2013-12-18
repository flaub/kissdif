package rql

import (
	"encoding/json"
	"github.com/flaub/ergo"
	"github.com/flaub/kissdif"
)

type _Record struct {
	Id_   string           `json:"id,omitempty" codec:"id,omitempty"`
	Rev_  string           `json:"rev,omitempty" codec:"rev,omitempty"`
	Doc_  json.RawMessage  `json:"doc,omitempty" codec:"doc,omitempty"`
	Keys_ kissdif.IndexMap `json:"keys,omitempty" codec:"keys,omitempty"`
}

func (this *_Record) Id() string {
	return this.Id_
}

func (this *_Record) Rev() string {
	return this.Rev_
}

func (this *_Record) Scan(into interface{}) (interface{}, error) {
	err := json.Unmarshal(this.Doc_, into)
	return into, err
}

func (this *_Record) MustScan(into interface{}) interface{} {
	result, err := this.Scan(into)
	if err != nil {
		panic(err)
	}
	return result
}

func (this *_Record) Set(doc interface{}) (err error) {
	this.Doc_, err = json.Marshal(doc)
	return err
}

func (this *_Record) MustSet(doc interface{}) {
	err := this.Set(doc)
	if err != nil {
		panic(err)
	}
}

func (this *_Record) Keys() kissdif.IndexMap {
	return this.Keys_
}

type putStmt struct {
	queryImpl
}

type getStmt struct {
	queryImpl
}

type deleteStmt struct {
	queryImpl
}

type queryImpl struct {
	db     string
	table  string
	record kissdif.Record
	query  kissdif.Query
}

func newQuery(db string) queryImpl {
	return queryImpl{
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

func (this queryImpl) DropTable(name string) ExecStmt {
	this.table = name
	return nil
}

func (this queryImpl) Table(name string) Table {
	this.table = name
	return this
}

func (this queryImpl) Limit(count uint) Query {
	this.query.Limit = count
	return this
}

func (this queryImpl) By(index string) Query {
	this.query.Index = index
	return this
}

func (this queryImpl) Get(key string) SingleStmt {
	bound := &kissdif.Bound{true, key}
	this.query.Limit = 1
	this.query.Lower = bound
	this.query.Upper = bound
	return getStmt{this}
}

func (this queryImpl) GetAll(key string) Limitable {
	bound := &kissdif.Bound{true, key}
	this.query.Lower = bound
	this.query.Upper = bound
	return this
}

func (this queryImpl) Between(lower, upper string) Limitable {
	this.query.Lower = &kissdif.Bound{true, lower}
	this.query.Upper = &kissdif.Bound{false, upper}
	return this
}

func (this queryImpl) Insert(id string, doc interface{}) PutStmt {
	this.record.Id = id
	this.record.Doc = doc
	return putStmt{this}
}

func (this queryImpl) Update(id, rev string, doc interface{}) PutStmt {
	this.record.Id = id
	this.record.Rev = rev
	this.record.Doc = doc
	return putStmt{this}
}

func (this queryImpl) Delete(id, rev string) ExecStmt {
	this.record.Id = id
	this.record.Rev = rev
	return deleteStmt{this}
}

func (this queryImpl) UpdateRecord(record Record) PutStmt {
	this.record.Id = record.Id()
	this.record.Rev = record.Rev()
	record.MustScan(&this.record.Doc)
	stmt := putStmt{this}
	stmt.Keys(record.Keys())
	return stmt
}

func (this queryImpl) DeleteRecord(record Record) ExecStmt {
	this.record.Id = record.Id()
	this.record.Rev = record.Rev()
	return deleteStmt{this}
}

func (this putStmt) Exec(conn Conn) (string, *ergo.Error) {
	result, err := conn.put(this.queryImpl)
	return result, ergo.Chain(err, kissdif.NewError(kissdif.EGeneric))
}

func (this putStmt) Keys(keys kissdif.IndexMap) PutStmt {
	this.record.Keys = make(kissdif.IndexMap)
	for k, v := range keys {
		this.record.Keys[k] = v
	}
	return this
}

func (this putStmt) By(key, value string) PutStmt {
	keys := this.record.Keys
	this.record.Keys = make(kissdif.IndexMap)
	for k, v := range keys {
		this.record.Keys[k] = v
	}
	this.record.AddKey(key, value)
	return this
}

func (this deleteStmt) Exec(conn Conn) *ergo.Error {
	err := conn.delete(this.queryImpl)
	return ergo.Chain(err, kissdif.NewError(kissdif.EGeneric))
}

func (this queryImpl) Exec(conn Conn) (*ResultSet, *ergo.Error) {
	result, err := conn.get(this)
	return result, ergo.Chain(err, kissdif.NewError(kissdif.EGeneric))
}

func (this getStmt) Exec(conn Conn) (Record, *ergo.Error) {
	if conn == nil {
		return nil, kissdif.NewError(kissdif.EBadParam, "name", "conn", "value", conn)
	}
	resultSet, err := conn.get(this.queryImpl)
	if err != nil {
		return nil, ergo.Chain(err, kissdif.NewError(kissdif.EGeneric))
	}
	// fmt.Printf("RS: %v\n", resultSet)
	if resultSet.More || len(resultSet.Records) > 1 {
		return nil, kissdif.NewError(kissdif.EMultiple)
	}
	if len(resultSet.Records) == 0 {
		return nil, kissdif.NewError(kissdif.ENotFound)
	}
	record := resultSet.Records[0]
	return record, nil
}
