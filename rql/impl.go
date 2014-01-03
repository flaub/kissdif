package rql

import (
	"encoding/json"
	"github.com/flaub/ergo"
	"github.com/flaub/kissdif"
)

type ResultSetImpl struct {
	More_    bool          `json:"more,omitempty" codec:"more,omitempty"`
	Records_ []*RecordImpl `json:"records,omitempty" codec:"records,omitempty"`
}

type RecordReaderImpl struct {
	records []*RecordImpl
	record  *RecordImpl
	index   int
}

func (this *ResultSetImpl) More() bool {
	return this.More_
}

func (this *ResultSetImpl) Count() int {
	return len(this.Records_)
}

func (this *ResultSetImpl) Reader() RecordReader {
	return &RecordReaderImpl{records: this.Records_}
}

func (this *RecordReaderImpl) Record() Record {
	return this.record
}

func (this *RecordReaderImpl) Next() bool {
	if this.index == len(this.records) {
		return false
	}
	this.record = this.records[this.index]
	this.index++
	return true
}

func (this *RecordReaderImpl) Scan(into interface{}) (interface{}, error) {
	return this.Record().Scan(into)
}

func (this *RecordReaderImpl) MustScan(into interface{}) interface{} {
	return this.Record().MustScan(into)
}

type RecordImpl struct {
	Id_   string           `json:"id,omitempty" codec:"id,omitempty"`
	Rev_  string           `json:"rev,omitempty" codec:"rev,omitempty"`
	Doc_  json.RawMessage  `json:"doc,omitempty" codec:"doc,omitempty"`
	Keys_ kissdif.IndexMap `json:"keys,omitempty" codec:"keys,omitempty"`
}

func (this *RecordImpl) Id() string {
	return this.Id_
}

func (this *RecordImpl) Rev() string {
	return this.Rev_
}

func (this *RecordImpl) Scan(into interface{}) (interface{}, error) {
	err := json.Unmarshal(this.Doc_, into)
	return into, err
}

func (this *RecordImpl) MustScan(into interface{}) interface{} {
	result, err := this.Scan(into)
	if err != nil {
		panic(err)
	}
	return result
}

func (this *RecordImpl) Set(doc interface{}) (err error) {
	this.Doc_, err = json.Marshal(doc)
	return err
}

func (this *RecordImpl) MustSet(doc interface{}) {
	err := this.Set(doc)
	if err != nil {
		panic(err)
	}
}

func (this *RecordImpl) Keys() kissdif.IndexMap {
	return this.Keys_
}

type putStmt struct {
	QueryImpl
}

type getStmt struct {
	QueryImpl
}

type deleteStmt struct {
	QueryImpl
}

type QueryImpl struct {
	Db_     string
	Table_  string
	Record_ kissdif.Record
	Query_  kissdif.Query
}

func newQuery(db string) QueryImpl {
	return QueryImpl{
		Db_: db,
		Record_: kissdif.Record{
			Keys: make(kissdif.IndexMap),
		},
		Query_: kissdif.Query{
			Index: "_id",
			Limit: 1000,
		},
	}
}

func (this QueryImpl) DropTable(name string) ExecStmt {
	this.Table_ = name
	return nil
}

func (this QueryImpl) Table(name string) Table {
	this.Table_ = name
	return this
}

func (this QueryImpl) Limit(count uint) Query {
	this.Query_.Limit = count
	return this
}

func (this QueryImpl) By(index string) Query {
	this.Query_.Index = index
	return this
}

func (this QueryImpl) Get(key string) SingleStmt {
	bound := kissdif.Bound{true, key}
	this.Query_.Limit = 1
	this.Query_.Lower = bound
	this.Query_.Upper = bound
	return getStmt{this}
}

func (this QueryImpl) GetAll(key string) Limitable {
	bound := kissdif.Bound{true, key}
	this.Query_.Lower = bound
	this.Query_.Upper = bound
	return this
}

func (this QueryImpl) Between(lower, upper string) Limitable {
	this.Query_.Lower = kissdif.Bound{true, lower}
	this.Query_.Upper = kissdif.Bound{false, upper}
	return this
}

func (this QueryImpl) Insert(id string, doc interface{}) PutStmt {
	this.Record_.Id = id
	this.Record_.Doc = doc
	return putStmt{this}
}

func (this QueryImpl) Update(id, rev string, doc interface{}) PutStmt {
	this.Record_.Id = id
	this.Record_.Rev = rev
	this.Record_.Doc = doc
	return putStmt{this}
}

func (this QueryImpl) Delete(id, rev string) ExecStmt {
	this.Record_.Id = id
	this.Record_.Rev = rev
	return deleteStmt{this}
}

func (this QueryImpl) UpdateRecord(record Record) PutStmt {
	this.Record_.Id = record.Id()
	this.Record_.Rev = record.Rev()
	record.MustScan(&this.Record_.Doc)
	stmt := putStmt{this}
	stmt.Keys(record.Keys())
	return stmt
}

func (this QueryImpl) DeleteRecord(record Record) ExecStmt {
	this.Record_.Id = record.Id()
	this.Record_.Rev = record.Rev()
	return deleteStmt{this}
}

func (this putStmt) Exec(conn Conn) (string, *ergo.Error) {
	result, err := conn.Put(this.QueryImpl)
	return result, ergo.Chain(err, kissdif.NewError(kissdif.EGeneric))
}

func (this putStmt) Keys(keys kissdif.IndexMap) PutStmt {
	this.Record_.Keys = keys.Clone()
	return this
}

func (this putStmt) By(key, value string) PutStmt {
	this.Record_.Keys = this.Record_.Keys.Clone()
	this.Record_.AddKey(key, value)
	return this
}

func (this deleteStmt) Exec(conn Conn) *ergo.Error {
	err := conn.Delete(this.QueryImpl)
	return ergo.Chain(err, kissdif.NewError(kissdif.EGeneric))
}

func (this QueryImpl) Exec(conn Conn) (ResultSet, *ergo.Error) {
	result, err := conn.Get(this)
	return result, ergo.Chain(err, kissdif.NewError(kissdif.EGeneric))
}

func (this getStmt) Exec(conn Conn) (Record, *ergo.Error) {
	if conn == nil {
		return nil, kissdif.NewError(kissdif.EBadParam, "name", "conn", "value", conn)
	}
	resultSet, err := conn.Get(this.QueryImpl)
	if err != nil {
		return nil, ergo.Chain(err, kissdif.NewError(kissdif.EGeneric))
	}
	reader := resultSet.Reader()
	if !reader.Next() {
		return nil, kissdif.NewError(kissdif.ENotFound)
	}
	if reader.Next() || resultSet.More() {
		return nil, kissdif.NewError(kissdif.EMultiple)
	}
	return reader.Record(), nil
}
