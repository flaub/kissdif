package server

import (
	"encoding/json"
	"fmt"
	"github.com/ant0ine/go-json-rest"
	"github.com/flaub/ergo"
	"github.com/flaub/kissdif"
	"github.com/flaub/kissdif/driver"
	"github.com/ugorji/go/codec"
	"log"
	"mime"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
)

var (
	MsgpackHandle = &codec.MsgpackHandle{}
)

type Server struct {
	http.Server
	dbs   map[string]driver.Database
	mutex sync.RWMutex
}

type Decoder interface {
	Decode(v interface{}) error
}

type Request struct {
	*rest.Request
	dec Decoder
}

func (this *Request) DecodePayload(v interface{}) error {
	return this.dec.Decode(v)
}

type Encoder interface {
	Encode(v interface{}) error
}

type ResponseWriter struct {
	*rest.ResponseWriter
	enc Encoder
}

func (this *ResponseWriter) WriteData(v interface{}) error {
	return this.enc.Encode(v)
}

func (this *ResponseWriter) Error(err *ergo.Error) {
	var code int
	switch err.Code {
	case kissdif.ENone:
		code = http.StatusOK
	case kissdif.EGeneric:
		code = http.StatusInternalServerError
	case kissdif.EMissingDriver:
		code = http.StatusNotImplemented
	case kissdif.EConflict:
		code = http.StatusConflict
	case kissdif.EBadTable:
		code = http.StatusNotFound
	case kissdif.EBadIndex:
		code = http.StatusNotFound
	case kissdif.EBadParam:
		code = http.StatusBadRequest
	case kissdif.EBadQuery:
		code = http.StatusBadRequest
	case kissdif.EBadDatabase:
		code = http.StatusNotFound
	case kissdif.EBadRouteVar:
		code = http.StatusInternalServerError
	case kissdif.EBadRequest:
		code = http.StatusBadRequest
	case kissdif.ENotFound:
		code = http.StatusNotFound
	default:
		log.Panicf("Forgot to check for error code: %d", err.Code)
	}
	this.WriteHeader(code)
	this.WriteData(err)
}

type RestHandlerFunc func(*rest.ResponseWriter, *rest.Request)
type HandlerFunc func(*ResponseWriter, *Request) interface{}

func typeWrapper(fn HandlerFunc) RestHandlerFunc {
	return func(resp *rest.ResponseWriter, req *rest.Request) {
		ctype := req.Header.Get("Content-Type")
		mediatype, params, _ := mime.ParseMediaType(ctype)
		charset, ok := params["charset"]
		if !ok {
			charset = "utf-8"
		}
		if strings.ToLower(charset) != "utf-8" {
			http.Error(resp, "Bad charset", http.StatusUnsupportedMediaType)
		}
		var enc Encoder
		var dec Decoder
		switch mediatype {
		case "application/json":
			dec = json.NewDecoder(req.Body)
			enc = json.NewEncoder(resp)
		case "application/x-msgpack":
			dec = codec.NewDecoder(req.Body, MsgpackHandle)
			enc = codec.NewEncoder(resp, MsgpackHandle)
		default:
			msg := fmt.Sprintf("Bad Content-Type: %q", mediatype)
			http.Error(resp, msg, http.StatusUnsupportedMediaType)
		}
		writer := &ResponseWriter{ResponseWriter: resp, enc: enc}
		reader := &Request{Request: req, dec: dec}
		ret := fn(writer, reader)
		writer.Header().Set("Content-Type", ctype)
		if err, ok := ret.(*ergo.Error); ok {
			writer.Error(err)
		} else if ret != nil {
			writer.WriteData(ret)
		}
	}
}

func NewServer() *Server {
	handler := &rest.ResourceHandler{
		EnableRelaxedContentType: true,
	}

	this := &Server{
		Server: http.Server{
			Addr:    ":7780",
			Handler: handler,
		},
		dbs: make(map[string]driver.Database),
	}

	handler.SetRoutes(
		rest.Route{"PUT", "/:db", typeWrapper(this.putDb)},
		rest.Route{"GET", "/:db/:table/:index", typeWrapper(this.doQuery)},
		rest.Route{"GET", "/:db/:table/:index/*key", typeWrapper(this.getRecord)},
		rest.Route{"PUT", "/:db/:table/_id/*key", typeWrapper(this.putRecord)},
		rest.Route{"DELETE", "/:db/:table/_id/*key", typeWrapper(this.deleteRecord)},
	)

	return this
}

func (this *Server) findDb(name string) (driver.Database, *ergo.Error) {
	this.mutex.RLock()
	defer this.mutex.RUnlock()
	db, ok := this.dbs[name]
	if !ok {
		return nil, kissdif.NewError(kissdif.EBadDatabase, "name", name)
	}
	return db, nil
}

func (this *Server) getVar(req *Request, name string) (string, *ergo.Error) {
	raw, ok := req.PathParams[name]
	if !ok {
		return "", kissdif.NewError(kissdif.EBadRouteVar, "name", name)
	}
	value, err := url.QueryUnescape(raw)
	if err != nil {
		return "", kissdif.Wrap(err)
	}
	return value, nil
}

func (this *Server) getTable(req *Request, create bool) (driver.Table, *ergo.Error) {
	dbName, kerr := this.getVar(req, "db")
	if kerr != nil {
		return nil, kerr
	}
	db, kerr := this.findDb(dbName)
	if kerr != nil {
		return nil, kerr
	}
	tableName, kerr := this.getVar(req, "table")
	if kerr != nil {
		return nil, kerr
	}
	return db.GetTable(tableName, create)
}

func (this *Server) putDb(resp *ResponseWriter, req *Request) interface{} {
	// log.Printf("PUT db: %v\n", req.URL)
	dbName, kerr := this.getVar(req, "db")
	if kerr != nil {
		return kerr
	}
	var dbcfg kissdif.DatabaseCfg
	err := req.DecodePayload(&dbcfg)
	if err != nil {
		return kissdif.NewError(kissdif.EBadDatabase, "err", err.Error())
	}
	drv, kerr := driver.Open(dbcfg.Driver)
	if kerr != nil {
		return kerr
	}
	db, kerr := drv.Configure(dbName, dbcfg.Config)
	if kerr != nil {
		return kerr
	}
	this.mutex.Lock()
	defer this.mutex.Unlock()
	this.dbs[dbName] = db
	return nil
}

func (this *Server) putRecord(resp *ResponseWriter, req *Request) interface{} {
	table, kerr := this.getTable(req, true)
	if kerr != nil {
		return kerr
	}
	var record kissdif.Record
	err := req.DecodePayload(&record)
	if err != nil {
		return kissdif.NewError(kissdif.EBadRequest, "err", err.Error())
	}
	// log.Printf("PUT record: %v\n%v", req.URL, record)
	id, kerr := this.getVar(req, "key")
	if kerr != nil {
		return kerr
	}
	if record.Id != id {
		return kissdif.NewError(kissdif.EBadParam, "name", "id", "value", id)
	}
	rev, kerr := table.Put(&record)
	if kerr != nil {
		return kerr
	}
	return rev
}

func (this *Server) doQuery(resp *ResponseWriter, req *Request) interface{} {
	// fmt.Printf("GET records: %v\n", req.URL)
	args := req.URL.Query()
	table, kerr := this.getTable(req, false)
	if kerr != nil {
		return kerr
	}
	lower, upper, kerr := getBounds(args)
	if kerr != nil {
		return kerr
	}
	limit, kerr := getLimit(args)
	if kerr != nil {
		return kerr
	}
	index, kerr := this.getVar(req, "index")
	if kerr != nil {
		return kerr
	}
	query := kissdif.NewQuery(index, lower, upper, limit)
	result, kerr := this.processQuery(table, query)
	if kerr != nil {
		return kerr
	}
	return result
}

func (this *Server) getRecord(resp *ResponseWriter, req *Request) interface{} {
	// log.Printf("GET record: %v\n", req.URL)
	args := req.URL.Query()
	table, kerr := this.getTable(req, false)
	if kerr != nil {
		return kerr
	}
	limit, kerr := getLimit(args)
	if kerr != nil {
		return kerr
	}
	index, kerr := this.getVar(req, "index")
	if kerr != nil {
		return kerr
	}
	key, kerr := this.getVar(req, "key")
	if kerr != nil {
		return kerr
	}
	query := kissdif.NewQueryEQ(index, key, limit)
	result, kerr := this.processQuery(table, query)
	if kerr != nil {
		return kerr
	}
	if len(result.Records) == 0 {
		return kissdif.NewError(kissdif.ENotFound)
	}
	return result
}

func (this *Server) deleteRecord(resp *ResponseWriter, req *Request) interface{} {
	// log.Printf("DELETE record: %v\n", req.URL)
	table, kerr := this.getTable(req, false)
	if kerr != nil {
		return kerr
	}
	key, kerr := this.getVar(req, "key")
	if kerr != nil {
		return kerr
	}
	table.Delete(key)
	return nil
}

func (this *Server) processQuery(table driver.Table, query *kissdif.Query) (*kissdif.ResultSet, *ergo.Error) {
	ch, kerr := table.Get(query)
	if kerr != nil {
		return nil, kerr
	}
	result := &kissdif.ResultSet{
		More:    true,
		Records: []*kissdif.Record{},
	}
	for record := range ch {
		if record == nil {
			result.More = false
		} else {
			result.Records = append(result.Records, record)
		}
	}
	return result, nil
}

func getLimit(args url.Values) (uint, *ergo.Error) {
	var limit uint64 = 1000
	strLimit := args.Get("limit")
	if strLimit != "" {
		var err error
		limit, err = strconv.ParseUint(strLimit, 10, 32)
		if err != nil {
			return 0, kissdif.NewError(kissdif.EBadParam, "name", limit, "value", strLimit, "err", err.Error())
		}
	}
	return uint(limit), nil
}

func getBounds(args url.Values) (lower, upper kissdif.Bound, err *ergo.Error) {
	for k, v := range args {
		switch k {
		case "eq":
			if lower.IsDefined() || upper.IsDefined() {
				err = kissdif.NewError(kissdif.EBadQuery)
				return
			}
			if len(v) != 1 {
				err = kissdif.NewError(kissdif.EBadQuery)
				return
			}
			lower = kissdif.Bound{true, v[0]}
			upper = kissdif.Bound{true, v[0]}
			break
		case "lt":
			if upper.IsDefined() {
				err = kissdif.NewError(kissdif.EBadQuery)
				return
			}
			if len(v) != 1 {
				err = kissdif.NewError(kissdif.EBadQuery)
				return
			}
			upper = kissdif.Bound{false, v[0]}
			break
		case "le":
			if upper.IsDefined() {
				err = kissdif.NewError(kissdif.EBadQuery)
				return
			}
			if len(v) != 1 {
				err = kissdif.NewError(kissdif.EBadQuery)
				return
			}
			upper = kissdif.Bound{true, v[0]}
			break
		case "gt":
			if lower.IsDefined() {
				err = kissdif.NewError(kissdif.EBadQuery)
				return
			}
			if len(v) != 1 {
				err = kissdif.NewError(kissdif.EBadQuery)
				return
			}
			lower = kissdif.Bound{false, v[0]}
			break
		case "ge":
			if lower.IsDefined() {
				err = kissdif.NewError(kissdif.EBadQuery)
				return
			}
			if len(v) != 1 {
				err = kissdif.NewError(kissdif.EBadQuery)
				return
			}
			lower = kissdif.Bound{true, v[0]}
			break
		}
	}
	return
}
