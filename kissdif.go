package kissdif

import (
	"fmt"
)

type Dictionary map[string]string

type ResultSet struct {
	IsTruncated bool
	Records     []*Record
}

type EnvJson struct {
	Name   string            `json:"_name"`
	Driver string            `json:"_driver"`
	Config map[string]string `json:"_config"`
}

type Bound struct {
	Inclusive bool
	Value     string
}

type Query struct {
	Index string
	Lower *Bound
	Upper *Bound
	Limit int
}

type IndexMap map[string][]string

type Record struct {
	Id   string   `json:"id"`
	Rev  string   `json:"rev"`
	Doc  string   `json:"doc"`
	Keys IndexMap `json:"keys",omitempty`
}

type Error struct {
	Status  int
	Message string
}

func NewQuery(index string, lower, upper *Bound, limit int) *Query {
	return &Query{index, lower, upper, limit}
}

func NewQueryEQ(index, key string, limit int) *Query {
	bound := &Bound{true, key}
	return &Query{index, bound, bound, limit}
}

func NewQueryGT(index, key string, limit int) *Query {
	bound := &Bound{false, key}
	return &Query{index, bound, nil, limit}
}

func NewQueryGTE(index, key string, limit int) *Query {
	bound := &Bound{true, key}
	return &Query{index, bound, nil, limit}
}

func NewQueryLT(index, key string, limit int) *Query {
	bound := &Bound{false, key}
	return &Query{index, nil, bound, limit}
}

func NewQueryLTE(index, key string, limit int) *Query {
	bound := &Bound{true, key}
	return &Query{index, nil, bound, limit}
}

func NewQueryRange(index, lower, upper string, limit int) *Query {
	lb := &Bound{true, lower}
	ub := &Bound{true, upper}
	return &Query{index, lb, ub, limit}
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
	ret := fmt.Sprintf("%d records: [", theLen)
	ret += fmt.Sprintf("%v", this.Records[0].Id)
	for _, record := range this.Records[1:] {
		ret += fmt.Sprintf(", %v", record.Id)
	}
	if this.IsTruncated {
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
