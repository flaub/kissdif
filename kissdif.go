package kissdif

import (
	"fmt"
	"log"
)

var (
	_ = log.Printf
)

type Dictionary map[string]string

type ResultSet struct {
	More    bool
	Records []*Record
}

type DatabaseCfg struct {
	_struct bool              `codec:",omitempty"` // set omitempty for every field
	Name    string            `json:"name",omitempty`
	Driver  string            `json:"driver",omitempty`
	Config  map[string]string `json:"config",omitempty`
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
	Id      string      `json:"id",omitempty`
	Rev     string      `json:"rev",omitempty`
	Doc     interface{} `json:"doc",omitempty`
	Keys    IndexMap    `json:"keys",omitempty`
}

type Error struct {
	Status  int
	Message string
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

func NewError(status int, message string) *Error {
	return &Error{status, message}
}

func (this *Error) Error() string {
	return this.Message
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
