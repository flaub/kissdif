package rql

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/flaub/kissdif"
	"github.com/ugorji/go/codec"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

var (
	MsgpackHandle = &codec.MsgpackHandle{}
)

type Decoder interface {
	Decode(v interface{}) error
}

type Encoder interface {
	Encode(v interface{}) error
}

type MakeDecoder func(io.Reader) Decoder
type MakeEncoder func(io.Writer) Encoder

type httpConn struct {
	baseUrl   string
	formatter formatter
}

type formatter interface {
	ContentType() string
	Encoder(io.Writer) Encoder
	Decoder(io.Reader) Decoder
}

type msgpackFormatter struct{}

func (this *msgpackFormatter) ContentType() string {
	return "application/x-msgpack"
}

func (this *msgpackFormatter) Encoder(w io.Writer) Encoder {
	return codec.NewEncoder(w, MsgpackHandle)
}

func (this *msgpackFormatter) Decoder(r io.Reader) Decoder {
	return codec.NewDecoder(r, MsgpackHandle)
}

type jsonFormatter struct{}

func (this *jsonFormatter) ContentType() string {
	return "application/json"
}

func (this *jsonFormatter) Encoder(w io.Writer) Encoder {
	return json.NewEncoder(w)

}

func (this *jsonFormatter) Decoder(r io.Reader) Decoder {
	return json.NewDecoder(r)
}

func newHttpConn(url string) *httpConn {
	return &httpConn{
		baseUrl: url,
		// formatter: &msgpackFormatter{},
		formatter: &jsonFormatter{},
	}
}

func (this *httpConn) makeUrl(impl *queryImpl) string {
	return fmt.Sprintf("%s/%s/%s/%s",
		this.baseUrl,
		url.QueryEscape(impl.db),
		url.QueryEscape(impl.table),
		url.QueryEscape(impl.query.Index))
}

func (this *httpConn) sendRequest(method, url string, v interface{}) (*http.Response, *kissdif.Error) {
	var buf bytes.Buffer
	err := this.formatter.Encoder(&buf).Encode(v)
	if err != nil {
		msg := fmt.Sprintf("Encoder error: %v", err)
		return nil, kissdif.NewError(http.StatusBadRequest, msg)
	}
	req, err := http.NewRequest(method, url, &buf)
	if err != nil {
		msg := fmt.Sprintf("Bad request: %v", err)
		return nil, kissdif.NewError(http.StatusBadRequest, msg)
	}
	req.Header.Set("Content-Type", this.formatter.ContentType())
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		msg := fmt.Sprintf("Client error: %v", err)
		return nil, kissdif.NewError(http.StatusBadRequest, msg)
	}
	return resp, nil
}

func (this *httpConn) recvReply(resp *http.Response, v interface{}) *kissdif.Error {
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			msg := fmt.Sprintf("Body error: %v", err)
			return kissdif.NewError(http.StatusBadRequest, msg)
		}
		msg := fmt.Sprintf("Server error: %v", strings.TrimSpace(string(body)))
		return kissdif.NewError(resp.StatusCode, msg)
	}
	if v != nil {
		err := this.formatter.Decoder(resp.Body).Decode(v)
		if err != nil {
			msg := fmt.Sprintf("Decoder error: %v", err)
			return kissdif.NewError(http.StatusNotAcceptable, msg)
		}
	}
	return nil
}

func (this *httpConn) roundTrip(method, url string, in, out interface{}) *kissdif.Error {
	resp, err := this.sendRequest(method, url, in)
	if err != nil {
		return err
	}
	return this.recvReply(resp, out)
}

func (this *httpConn) CreateDB(name, driverName string, config kissdif.Dictionary) (Database, *kissdif.Error) {
	url := fmt.Sprintf("%s/%s", this.baseUrl, name)
	dbcfg := &kissdif.DatabaseCfg{
		Name:   name,
		Driver: driverName,
		Config: config,
	}
	kerr := this.roundTrip("PUT", url, dbcfg, nil)
	if kerr != nil {
		return nil, kerr
	}
	return newQuery(name), nil
}

func (this *httpConn) DropDB(name string) *kissdif.Error {
	return kissdif.NewError(http.StatusNotImplemented, "Not implemented")
}

func (this *httpConn) RegisterType(name string, doc interface{}) {
}

func (this *httpConn) get(impl *queryImpl) (*ResultSet, *kissdif.Error) {
	args := make(url.Values)
	if impl.query.Limit != 0 {
		args.Set("limit", strconv.Itoa(int(impl.query.Limit)))
	}
	if impl.query.Lower != nil && impl.query.Upper != nil &&
		impl.query.Lower.Value == impl.query.Upper.Value {
		args.Set("eq", impl.query.Lower.Value)
	} else {
		if impl.query.Lower != nil {
			if impl.query.Lower.Inclusive {
				args.Set("ge", impl.query.Lower.Value)
			} else {
				args.Set("gt", impl.query.Lower.Value)
			}
		}
		if impl.query.Upper != nil {
			if impl.query.Upper.Inclusive {
				args.Set("le", impl.query.Upper.Value)
			} else {
				args.Set("lt", impl.query.Upper.Value)
			}
		}
	}
	url := this.makeUrl(impl) + "?" + args.Encode()
	var result ResultSet
	kerr := this.roundTrip("GET", url, nil, &result)
	if kerr != nil {
		return nil, kerr
	}
	return &result, nil
}

func (this *httpConn) put(impl *queryImpl) (string, *kissdif.Error) {
	url := this.makeUrl(impl) + "/" + url.QueryEscape(impl.record.Id)
	var rev string
	kerr := this.roundTrip("PUT", url, impl.record, &rev)
	if kerr != nil {
		return "", kerr
	}
	return rev, nil
}

func (this *httpConn) delete(impl *queryImpl) *kissdif.Error {
	url := this.makeUrl(impl) + "/" + url.QueryEscape(impl.record.Id)
	return this.roundTrip("DELETE", url, nil, nil)
}
