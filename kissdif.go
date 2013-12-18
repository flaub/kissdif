package kissdif

import (
	"fmt"
	"github.com/flaub/ergo"
	"log"
)

type Dictionary map[string]string

type ResultSet struct {
	More    bool
	Records []*Record
}

type DatabaseCfg struct {
	_struct bool              `codec:",omitempty"` // set omitempty for every field
	Name    string            `json:",omitempty"`
	Driver  string            `json:",omitempty"`
	Config  map[string]string `json:",omitempty"`
}

type Bound struct {
	Inclusive bool
	Value     string
}

type Query struct {
	Index string
	Lower *Bound
	Upper *Bound
	Limit uint
}

type IndexMap map[string][]string

type Record struct {
	_struct bool        `codec:",omitempty"` // set omitempty for every field
	Id      string      `json:",omitempty"`
	Rev     string      `json:",omitempty"`
	Doc     interface{} `json:",omitempty"`
	Keys    IndexMap    `json:",omitempty"`
}

func NewRecord(id, rev string, doc interface{}) *Record {
	return &Record{
		Id:   id,
		Rev:  rev,
		Doc:  doc,
		Keys: make(IndexMap),
	}
}

func (this *Record) AddKey(name, value string) *Record {
	index, ok := this.Keys[name]
	if !ok {
		index = []string{}
	}
	this.Keys[name] = append(index, value)
	return this
}

func NewQuery(index string, lower, upper *Bound, limit uint) *Query {
	return &Query{index, lower, upper, limit}
}

func NewQueryEQ(index, key string, limit uint) *Query {
	bound := &Bound{true, key}
	return &Query{index, bound, bound, limit}
}

func (this *ResultSet) String() string {
	theLen := len(this.Records)
	if theLen == 0 {
		return fmt.Sprintf("0 records")
	}
	var ret string
	if theLen == 1 {
		ret = fmt.Sprintf("1 record: [")
	} else {
		ret = fmt.Sprintf("%d records: [", theLen)
	}
	ret += fmt.Sprintf("%v", this.Records[0].Id)
	for _, record := range this.Records[1:] {
		ret += fmt.Sprintf(", %v", record.Id)
	}
	if this.More {
		ret += ", ..."
	}
	ret += "]"
	return ret
}

func (this *Query) String() string {
	str := fmt.Sprintf("[%d] ", this.Limit)
	if this.Lower != nil {
		str += this.Lower.Value
		if this.Lower.Inclusive {
			str += " <= "
		} else {
			str += " < "
		}
	}
	str += this.Index
	if this.Upper != nil {
		if this.Upper.Inclusive {
			str += " <= "
		} else {
			str += " < "
		}
		str += this.Upper.Value
	}
	return str
}

var _ = log.Printf

const (
	ENone = ergo.ErrCode(iota)
	EGeneric
	EMissingDriver
	EConflict
	EBadParam
	EBadTable
	EBadIndex
	EBadQuery
	EBadDatabase
	EBadRouteVar
	EBadRequest
	ENotFound
	EMultiple
)

var (
	domain = "kissdif"
	errors = ergo.DomainMap{
		ENone:          "No error",
		EGeneric:       "Generic error: {{.err}}",
		EMissingDriver: "Missing driver '{{.name}}' (forgotten import?)",
		EConflict:      "Document conflict",
		EBadParam:      "Invalid parameter: {{.name}} = '{{.value}}'",
		EBadTable:      "Table not found: '{{.name}}'",
		EBadIndex:      "Index not found: '{{.name}}'",
		EBadQuery:      "Invalid query",
		EBadDatabase:   "Database not found: '{{.name}}'",
		EBadRouteVar:   "Route variable not found: '{{.name}}'",
		EBadRequest:    "Invalid request",
		ENotFound:      "Record not found",
		EMultiple:      "Multiple records found",
	}
)

func init() {
	ergo.Domain(domain, errors)
}

func NewError(code ergo.ErrCode, args ...interface{}) *ergo.Error {
	return ergo.New(1, domain, code, args...)
}

func Wrap(err error) *ergo.Error {
	return ergo.New(1, domain, EGeneric, "err", err.Error())
}
