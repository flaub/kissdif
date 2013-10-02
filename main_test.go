package main

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestServer(t *testing.T) {
	ts := httptest.NewServer(NewServer().Server.Handler)
	defer ts.Close()

	res, err := http.Get(ts.URL + "/env/table/_id/1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	result, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		t.Fatalf("Body read failed: %v", err)
	}

	if res.StatusCode != http.StatusNotFound {
		t.Fatalf("Status code is not 404: %v", res.Status)
	}

	t.Logf("Result: %s", result)
}

func TestBasic(t *testing.T) {
	ts := httptest.NewServer(NewServer().Server.Handler)
	defer ts.Close()

	record := &Record{
		Id:  "1",
		Doc: []byte("Value"),
	}

	client := NewKissClient(ts.URL, "env", "table")

	err := client.Put(record)
	if err != nil {
		t.Fatalf("PUT failed: %v", err)
	}

	result, err := client.Get(record.Id)
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}

	if string(result) != string(record.Doc) {
		t.Fatalf("Unexpected result: %q", result)
	}

	err = client.Delete(record.Id)
	if err != nil {
		t.Fatalf("DELETE failed: %v", err)
	}

	result, err = client.Get(record.Id)
	if err == nil {
		t.Fatalf("GET after DELETE should fail")
	}
	if err.Error() != "Record not found" {
		t.Fatalf("GET after DELETE unexpected err: %v", err)
	}
}

func TestIndex(t *testing.T) {
	ts := httptest.NewServer(NewServer().Server.Handler)
	defer ts.Close()

	record := &Record{
		Id:   "1",
		Doc:  []byte("Value"),
		Keys: make(map[string][]string),
	}
	record.Keys["by_name"] = []string{"Joe", "Bob"}

	client := NewKissClient(ts.URL, "env", "table")

	err := client.Put(record)
	if err != nil {
		t.Fatalf("PUT failed: %v", err)
	}

	result, err := client.Get(record.Id)
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}

	if string(result) != string(record.Doc) {
		t.Fatalf("Unexpected result: %q", result)
	}

	result, err = client.GetWithIndex("by_name", "Joe")
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}

	if string(result) != string(record.Doc) {
		t.Fatalf("Unexpected result: %q", result)
	}

	err = client.Delete(record.Id)
	if err != nil {
		t.Fatalf("DELETE failed: %v", err)
	}

	result, err = client.Get(record.Id)
	if err == nil {
		t.Fatalf("GET after DELETE should fail")
	}
	if err.Error() != "Record not found" {
		t.Fatalf("GET after DELETE unexpected err: %v", err)
	}
}
