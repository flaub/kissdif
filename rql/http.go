package rql

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/flaub/kissdif"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
)

type httpConn struct {
	baseUrl string
}

func newHttpConn(url string) *httpConn {
	return &httpConn{url}
}

func (this *httpConn) CreateDB(name, driverName string, config kissdif.Dictionary) (IDatabase, *kissdif.Error) {
	url := fmt.Sprintf("%s/%s", this.baseUrl, name)
	envJson := &kissdif.EnvJson{
		Name:   name,
		Driver: driverName,
		Config: config,
	}
	var buf bytes.Buffer
	err := json.NewEncoder(&buf).Encode(envJson)
	if err != nil {
		return nil, kissdif.NewError(http.StatusBadRequest, err.Error())
	}
	req, err := http.NewRequest("PUT", url, &buf)
	if err != nil {
		return nil, kissdif.NewError(http.StatusBadRequest, err.Error())
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, kissdif.NewError(http.StatusBadRequest, err.Error())
	}
	defer resp.Body.Close()
	result, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, kissdif.NewError(http.StatusBadRequest, err.Error())
	}
	if resp.StatusCode != http.StatusOK {
		return nil, kissdif.NewError(resp.StatusCode, string(result))
	}
	return newQuery(name), nil
}

func (this *httpConn) DropDB(name string) *kissdif.Error {
	return kissdif.NewError(http.StatusNotImplemented, "Not implemented")
}

func (this *httpConn) Get(impl *queryImpl) (chan *kissdif.Record, *kissdif.Error) {
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
	url := fmt.Sprintf("%s/%s/%s/%s?%s", this.baseUrl, impl.db, impl.table, impl.query.Index, args.Encode())
	resp, err := http.Get(url)
	if err != nil {
		return nil, kissdif.NewError(http.StatusBadRequest, err.Error())
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, kissdif.NewError(http.StatusBadRequest, err.Error())
		}
		return nil, kissdif.NewError(resp.StatusCode, string(body))
	}
	var result kissdif.ResultSet
	err = json.NewDecoder(resp.Body).Decode(&result)
	if err != nil {
		return nil, kissdif.NewError(http.StatusNotAcceptable, err.Error())
	}
	ch := make(chan *kissdif.Record)
	go func() {
		for _, record := range result.Records {
			ch <- record
		}
		if !result.More {
			ch <- nil
		}
		close(ch)
	}()
	return ch, nil
}

func (this *httpConn) Put(impl *queryImpl) (*kissdif.Record, *kissdif.Error) {
	var bufDoc bytes.Buffer
	json.NewEncoder(&bufDoc).Encode(impl.doc)
	record := &kissdif.Record{
		Id:  impl.id,
		Rev: impl.rev,
		Doc: bufDoc.String(),
	}
	var bufRec bytes.Buffer
	err := json.NewEncoder(&bufRec).Encode(record)
	if err != nil {
		return nil, kissdif.NewError(http.StatusBadRequest, err.Error())
	}
	url := fmt.Sprintf("%s/%s/%s/_id/%s", this.baseUrl, impl.db, impl.table, record.Id)
	req, err := http.NewRequest("PUT", url, &bufRec)
	if err != nil {
		return nil, kissdif.NewError(http.StatusBadRequest, err.Error())
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, kissdif.NewError(http.StatusBadRequest, err.Error())
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		result, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, kissdif.NewError(http.StatusBadRequest, err.Error())
		}
		return nil, kissdif.NewError(resp.StatusCode, string(result))
	}
	var result kissdif.Record
	err = json.NewDecoder(resp.Body).Decode(&result)
	if err != nil {
		return nil, kissdif.NewError(http.StatusNotAcceptable, err.Error())
	}
	return &result, nil
}

func (this *httpConn) Delete(impl *queryImpl) *kissdif.Error {
	url := fmt.Sprintf("%s/%s/%s/_id/%s", this.baseUrl, impl.db, impl.table, impl.id)
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return kissdif.NewError(http.StatusBadRequest, err.Error())
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return kissdif.NewError(http.StatusBadRequest, err.Error())
	}
	defer resp.Body.Close()
	result, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return kissdif.NewError(http.StatusBadRequest, err.Error())
	}
	if resp.StatusCode != http.StatusOK {
		return kissdif.NewError(resp.StatusCode, string(result))
	}
	return nil
}
