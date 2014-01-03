package kissdif

import (
	"github.com/flaub/ergo"
)

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

func IsError(err *ergo.Error, code ergo.ErrCode) bool {
	if err == nil {
		return false
	}
	cause := ergo.Cause(err)
	return cause.Domain == domain && cause.Code == code
}

func IsConflict(err *ergo.Error) bool {
	return IsError(err, EConflict)
}
