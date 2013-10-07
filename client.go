package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/flaub/kissdif/driver"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
)

type KissClient struct {
	BaseUrl string
	Env     string
	Table   string
}

func NewKissClient(baseUrl, env, table string) *KissClient {
	return &KissClient{
		BaseUrl: baseUrl,
		Env:     env,
		Table:   table,
	}
}

func (this *KissClient) Query(query *driver.Query) (*ResultSet, error) {
	args := url.Values{}
	// args.Add("eq", key)
	if query.Limit != 0 {
		args.Set("limit", strconv.Itoa(query.Limit))
	}
	return nil, nil
}

func (this *KissClient) Get(id string) (*ResultSet, error) {
	return this.GetWithIndex("_id", id)
}

func (this *KissClient) GetWithIndex(index, key string) (*ResultSet, error) {
	url := fmt.Sprintf("%s/%s/%s/%s/%s", this.BaseUrl, this.Env, this.Table, index, key)
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("%s", body)
	}

	var result ResultSet
	json.NewDecoder(resp.Body).Decode(&result)
	return &result, nil
}

func (this *KissClient) Put(record *driver.Record) error {
	var buf bytes.Buffer
	err := json.NewEncoder(&buf).Encode(record)
	if err != nil {
		return err
	}

	url := fmt.Sprintf("%s/%s/%s/_id/%s", this.BaseUrl, this.Env, this.Table, record.Id)
	req, err := http.NewRequest("PUT", url, &buf)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	result, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s", result)
	}

	return nil
}

func (this *KissClient) Delete(id string) error {
	url := fmt.Sprintf("%s/%s/%s/_id/%s", this.BaseUrl, this.Env, this.Table, id)
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	result, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s", result)
	}

	return nil
}
