package kissdif

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
)

type Client struct {
	baseUrl string
}

func NewClient(baseUrl string) *Client {
	return &Client{
		baseUrl: baseUrl,
	}
}

func (this *Client) Query(query *Query) (*ResultSet, error) {
	args := url.Values{}
	// args.Add("eq", key)
	if query.Limit != 0 {
		args.Set("limit", strconv.Itoa(query.Limit))
	}
	return nil, nil
}

func (this *Client) PutEnv(name, driver string, config Dictionary) error {
	url := fmt.Sprintf("%s/%s", this.baseUrl, name)
	envJson := &EnvJson{
		Name:   name,
		Driver: driver,
		Config: config,
	}
	var buf bytes.Buffer
	err := json.NewEncoder(&buf).Encode(envJson)
	if err != nil {
		return err
	}
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

func (this *Client) Get(env, table, id string) (*ResultSet, error) {
	return this.GetBy(env, table, "_id", id)
}

func (this *Client) GetBy(env, table, index, key string) (*ResultSet, error) {
	url := fmt.Sprintf("%s/%s/%s/%s/%s", this.baseUrl, env, table, index, key)
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

func (this *Client) Put(env, table string, record *Record) error {
	var buf bytes.Buffer
	err := json.NewEncoder(&buf).Encode(record)
	if err != nil {
		return err
	}
	url := fmt.Sprintf("%s/%s/%s/_id/%s", this.baseUrl, env, table, record.Id)
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

func (this *Client) Delete(env, table, id string) error {
	url := fmt.Sprintf("%s/%s/%s/_id/%s", this.baseUrl, env, table, id)
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
