package kissdif

import (
	"fmt"
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

func (this Bound) IsDefined() bool {
	return this.Value != ""
}

type Query struct {
	Index string
	Lower Bound
	Upper Bound
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
	// ignore duplicates
	for _, v := range index {
		if v == value {
			return this
		}
	}
	this.Keys[name] = append(index, value)
	return this
}

func (this *Record) DropKey(name, value string) *Record {
	this.Keys.drop(name, value)
	return this
}

func (this IndexMap) drop(name, value string) {
	index, ok := this[name]
	if !ok {
		return
	}
	for i, v := range index {
		if v == value {
			index[i], index = index[len(index)-1], index[:len(index)-1]
		}
	}
}

func (this IndexMap) Clone() IndexMap {
	keys := make(IndexMap)
	for k, v := range this {
		keys[k] = v
	}
	return keys
}

func NewQuery(index string, lower, upper Bound, limit uint) *Query {
	return &Query{index, lower, upper, limit}
}

func NewQueryEQ(index, key string, limit uint) *Query {
	bound := Bound{true, key}
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
	if this.Lower.IsDefined() {
		str += this.Lower.Value
		if this.Lower.Inclusive {
			str += " <= "
		} else {
			str += " < "
		}
	}
	str += this.Index
	if this.Upper.IsDefined() {
		if this.Upper.Inclusive {
			str += " <= "
		} else {
			str += " < "
		}
		str += this.Upper.Value
	}
	return str
}
