package rql

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/flaub/ergo"
	"github.com/flaub/kissdif"
	"github.com/ugorji/go/codec"
	"io"
	_ "log"
	"net/http"
	"net/url"
	"strconv"
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

func (this *httpConn) makeUrl(impl queryImpl) string {
	return fmt.Sprintf("%s/%s/%s/%s",
		this.baseUrl,
		url.QueryEscape(impl.db),
		url.QueryEscape(impl.table),
		url.QueryEscape(impl.query.Index))
}

func (this *httpConn) sendRequest(method, url string, v interface{}) (*http.Response, *ergo.Error) {
	var buf bytes.Buffer
	err := this.formatter.Encoder(&buf).Encode(v)
	if err != nil {
		return nil, ergo.Wrap(err)
	}
	req, err := http.NewRequest(method, url, &buf)
	if err != nil {
		return nil, ergo.Wrap(err)
	}
	req.Header.Set("Content-Type", this.formatter.ContentType())
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, ergo.Wrap(err)
	}
	return resp, nil
}

func (this *httpConn) recvReply(resp *http.Response, v interface{}) *ergo.Error {
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		var erg ergo.Error
		err := this.formatter.Decoder(resp.Body).Decode(&erg)
		if err != nil {
			return ergo.Wrap(err)
		}
		if erg.Domain == "" {
			if resp.StatusCode == http.StatusNotFound {
				return kissdif.NewError(kissdif.ENotFound)
			}
			return ergo.Wrap("Blank response from server.", "code", resp.StatusCode)
		}
		return &erg
	}
	if v != nil {
		err := this.formatter.Decoder(resp.Body).Decode(v)
		if err != nil {
			return ergo.Wrap(err)
		}
	}
	return nil
}

func (this *httpConn) roundTrip(method, url string, in, out interface{}) *ergo.Error {
	resp, err := this.sendRequest(method, url, in)
	if err != nil {
		return err
	}
	return this.recvReply(resp, out)
}

func (this *httpConn) CreateDB(name, driverName string, config kissdif.Dictionary) (Database, *ergo.Error) {
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

func (this *httpConn) DropDB(name string) *ergo.Error {
	return ergo.Wrap("Not implemented")
}

func (this *httpConn) RegisterType(name string, doc interface{}) {
}

func (this *httpConn) get(impl queryImpl) (*ResultSet, *ergo.Error) {
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

func (this *httpConn) put(impl queryImpl) (string, *ergo.Error) {
	if impl.record.Id == "" {
		return "", kissdif.NewError(kissdif.EBadParam, "name", "id", "value", impl.record.Id)
	}
	url := this.makeUrl(impl) + "/" + url.QueryEscape(impl.record.Id)
	var rev string
	kerr := this.roundTrip("PUT", url, impl.record, &rev)
	if kerr != nil {
		return "", kerr
	}
	return rev, nil
}

func (this *httpConn) delete(impl queryImpl) *ergo.Error {
	url := this.makeUrl(impl) + "/" + url.QueryEscape(impl.record.Id)
	return this.roundTrip("DELETE", url, nil, nil)
}
