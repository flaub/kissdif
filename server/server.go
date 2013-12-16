package server

import (
	"encoding/json"
	"fmt"
	"github.com/ant0ine/go-json-rest"
	. "github.com/flaub/kissdif"
	"github.com/flaub/kissdif/driver"
	"github.com/ugorji/go/codec"
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

type RestHandlerFunc func(*rest.ResponseWriter, *rest.Request)
type HandlerFunc func(*ResponseWriter, *Request)

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
		w := &ResponseWriter{ResponseWriter: resp, enc: enc}
		r := &Request{Request: req, dec: dec}
		fn(w, r)
		return
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

func (this *Server) findDb(name string) (driver.Database, *Error) {
	this.mutex.RLock()
	defer this.mutex.RUnlock()
	db, ok := this.dbs[name]
	if !ok {
		return nil, NewError(http.StatusNotFound, "Database not found")
	}
	return db, nil
}

func (this *Server) getVar(req *Request, name string) (string, *Error) {
	raw, ok := req.PathParams[name]
	if !ok {
		return "", NewError(http.StatusInternalServerError, fmt.Sprintf("Missing route variable: %v", name))
	}
	value, err := url.QueryUnescape(raw)
	if err != nil {
		return "", NewError(http.StatusBadRequest, err.Error())
	}
	return value, nil
}

func (this *Server) getTable(req *Request, create bool) (driver.Table, *Error) {
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

func (this *Server) putDb(resp *ResponseWriter, req *Request) {
	// fmt.Printf("PUT db: %v\n", req.URL)
	dbName, kerr := this.getVar(req, "db")
	if kerr != nil {
		http.Error(resp, kerr.Error(), kerr.Status)
		return
	}
	var dbcfg DatabaseCfg
	err := req.DecodePayload(&dbcfg)
	if err != nil {
		http.Error(resp, err.Error(), http.StatusBadRequest)
		return
	}
	drv, kerr := driver.Open(dbcfg.Driver)
	if kerr != nil {
		http.Error(resp, kerr.Error(), kerr.Status)
		return
	}
	db, kerr := drv.Configure(dbName, dbcfg.Config)
	if kerr != nil {
		http.Error(resp, kerr.Error(), kerr.Status)
		return
	}
	this.mutex.Lock()
	defer this.mutex.Unlock()
	this.dbs[dbName] = db
}

func (this *Server) putRecord(resp *ResponseWriter, req *Request) {
	// fmt.Printf("PUT record: %v\n", req.URL)
	table, kerr := this.getTable(req, true)
	if kerr != nil {
		http.Error(resp, kerr.Error(), kerr.Status)
		return
	}
	var record Record
	err := req.DecodePayload(&record)
	if err != nil {
		http.Error(resp, err.Error(), http.StatusBadRequest)
		return
	}
	id, kerr := this.getVar(req, "key")
	if kerr != nil {
		http.Error(resp, kerr.Error(), kerr.Status)
		return
	}
	if record.Id != id {
		http.Error(resp, "ID Mismatch", http.StatusBadRequest)
		return
	}
	rev, kerr := table.Put(&record)
	if kerr != nil {
		http.Error(resp, kerr.Error(), kerr.Status)
		return
	}
	resp.WriteData(rev)
}

func (this *Server) doQuery(resp *ResponseWriter, req *Request) {
	// fmt.Printf("GET records: %v\n", req.URL)
	args := req.URL.Query()
	table, kerr := this.getTable(req, false)
	if kerr != nil {
		http.Error(resp, kerr.Error(), kerr.Status)
		return
	}
	lower, upper, kerr := getBounds(args)
	if kerr != nil {
		http.Error(resp, kerr.Error(), kerr.Status)
		return
	}
	limit, kerr := getLimit(args)
	if kerr != nil {
		http.Error(resp, kerr.Error(), kerr.Status)
		return
	}
	index, kerr := this.getVar(req, "index")
	if kerr != nil {
		http.Error(resp, kerr.Error(), kerr.Status)
		return
	}
	query := NewQuery(index, lower, upper, limit)
	result, kerr := this.processQuery(table, query)
	if kerr != nil {
		http.Error(resp, kerr.Error(), kerr.Status)
		return
	}
	resp.WriteData(result)
}

func (this *Server) getRecord(resp *ResponseWriter, req *Request) {
	// fmt.Printf("GET record: %v\n", req.URL)
	args := req.URL.Query()
	table, kerr := this.getTable(req, false)
	if kerr != nil {
		http.Error(resp, kerr.Error(), kerr.Status)
		return
	}
	limit, kerr := getLimit(args)
	if kerr != nil {
		http.Error(resp, kerr.Error(), kerr.Status)
		return
	}
	index, kerr := this.getVar(req, "index")
	if kerr != nil {
		http.Error(resp, kerr.Error(), kerr.Status)
		return
	}
	key, kerr := this.getVar(req, "key")
	if kerr != nil {
		http.Error(resp, kerr.Error(), kerr.Status)
		return
	}
	query := NewQueryEQ(index, key, limit)
	result, kerr := this.processQuery(table, query)
	if kerr != nil {
		http.Error(resp, kerr.Error(), kerr.Status)
		return
	}
	if len(result.Records) == 0 {
		http.Error(resp, "Record not found", http.StatusNotFound)
		return
	}
	resp.WriteData(result)
}

func (this *Server) deleteRecord(resp *ResponseWriter, req *Request) {
	// fmt.Printf("DELETE record: %v\n", req.URL)
	table, kerr := this.getTable(req, false)
	if kerr != nil {
		http.Error(resp, kerr.Error(), kerr.Status)
		return
	}
	key, kerr := this.getVar(req, "key")
	if kerr != nil {
		http.Error(resp, kerr.Error(), kerr.Status)
		return
	}
	table.Delete(key)
}

func (this *Server) processQuery(table driver.Table, query *Query) (*ResultSet, *Error) {
	ch, kerr := table.Get(query)
	if kerr != nil {
		return nil, kerr
	}
	result := &ResultSet{
		More:    true,
		Records: []*Record{},
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

func getLimit(args url.Values) (uint, *Error) {
	var limit uint64 = 1000
	strLimit := args.Get("limit")
	if strLimit != "" {
		var err error
		limit, err = strconv.ParseUint(strLimit, 10, 32)
		if err != nil {
			return 0, NewError(http.StatusBadRequest, err.Error())
		}
	}
	return uint(limit), nil
}

func getBounds(args url.Values) (lower, upper *Bound, err *Error) {
	for k, v := range args {
		switch k {
		case "eq":
			if lower != nil || upper != nil {
				err = NewError(http.StatusBadGateway, "Invalid query")
				return
			}
			if len(v) != 1 {
				err = NewError(http.StatusBadGateway, "Invalid query")
				return
			}
			lower = &Bound{true, v[0]}
			upper = &Bound{true, v[0]}
			break
		case "lt":
			if upper != nil {
				err = NewError(http.StatusBadGateway, "Invalid query")
				return
			}
			if len(v) != 1 {
				err = NewError(http.StatusBadGateway, "Invalid query")
				return
			}
			upper = &Bound{false, v[0]}
			break
		case "le":
			if upper != nil {
				err = NewError(http.StatusBadGateway, "Invalid query")
				return
			}
			if len(v) != 1 {
				err = NewError(http.StatusBadGateway, "Invalid query")
				return
			}
			upper = &Bound{true, v[0]}
			break
		case "gt":
			if lower != nil {
				err = NewError(http.StatusBadGateway, "Invalid query")
				return
			}
			if len(v) != 1 {
				err = NewError(http.StatusBadGateway, "Invalid query")
				return
			}
			lower = &Bound{false, v[0]}
			break
		case "ge":
			if lower != nil {
				err = NewError(http.StatusBadGateway, "Invalid query")
				return
			}
			if len(v) != 1 {
				err = NewError(http.StatusBadGateway, "Invalid query")
				return
			}
			lower = &Bound{true, v[0]}
			break
		}
	}
	return
}
