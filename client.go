package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/flaub/kissdif/driver"
	"io/ioutil"
	"net/http"
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

func (this *KissClient) Get(id string) ([]byte, error) {
	return this.GetWithIndex("_id", id)
}

func (this *KissClient) GetWithIndex(indexName string, indexValue string) ([]byte, error) {
	url := fmt.Sprintf("%s/%s/%s/%s/%s", this.BaseUrl, this.Env, this.Table, indexName, indexValue)
	res, err := http.Get(url)
	if err != nil {
		return nil, err
	}

	result, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		return nil, err
	}

	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s", result)
	}

	return result, nil
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

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}

	result, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		return err
	}

	if res.StatusCode != http.StatusOK {
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

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}

	result, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		return err
	}

	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("%s", result)
	}

	return nil
}
