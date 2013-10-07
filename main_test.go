package main

import (
	"github.com/flaub/kissdif/driver"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
)

func NewRecord(id, doc string) *driver.Record {
	return &driver.Record{
		Id:   id,
		Doc:  []byte(doc),
		Keys: make(map[string][]string),
	}
}

func TestServer(t *testing.T) {
	ts := httptest.NewServer(NewServer().Server.Handler)
	defer ts.Close()

	res, err := http.Get(ts.URL + "/mem/table/_id/1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	defer res.Body.Close()

	result, err := ioutil.ReadAll(res.Body)
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

	record := NewRecord("1", "Value")

	client := NewKissClient(ts.URL, "mem", "table")

	err := client.Put(record)
	if err != nil {
		t.Fatalf("PUT failed: %v", err)
	}

	result, err := client.Get(record.Id)
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}

	if result.Count != 1 {
		t.Fatalf("Unexpected result count: %d", result.Count)
	}

	if string(result.Records[0].Doc) != string(record.Doc) {
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
	if err.Error() != "No records found" {
		t.Fatalf("GET after DELETE unexpected err: %q", err)
	}
}

func TestIndex(t *testing.T) {
	ts := httptest.NewServer(NewServer().Server.Handler)
	defer ts.Close()

	record := NewRecord("1", "Value")
	record.Keys["by_name"] = []string{"Joe", "Bob"}

	client := NewKissClient(ts.URL, "mem", "table")

	err := client.Put(record)
	if err != nil {
		t.Fatalf("PUT failed: %v", err)
	}

	result, err := client.Get(record.Id)
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}

	if string(result.Records[0].Doc) != string(record.Doc) {
		t.Fatalf("Unexpected result: %q", result)
	}

	result, err = client.GetWithIndex("by_name", "Joe")
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}

	if string(result.Records[0].Doc) != string(record.Doc) {
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

func TestQuery(t *testing.T) {
	// ts := httptest.NewServer(NewServer().Server.Handler)
	// defer ts.Close()

	// record := NewRecord("1", "Value")
	// record.Keys["by_name"] = []string{"Joe", "Bob"}

	// client := NewKissClient(ts.URL, "mem", "table")

	// err := client.Put(record)
	// if err != nil {
	// 	t.Fatalf("PUT failed: %v", err)
	// }

	// result, err := client.Get(record.Id)
	// if err != nil {
	// 	t.Fatalf("GET failed: %v", err)
	// }

	// if string(result) != string(record.Doc) {
	// 	t.Fatalf("Unexpected result: %q", result)
	// }

	// result, err = client.GetWithIndex("by_name", "Joe")
	// if err != nil {
	// 	t.Fatalf("GET failed: %v", err)
	// }

	// if string(result) != string(record.Doc) {
	// 	t.Fatalf("Unexpected result: %q", result)
	// }

	// err = client.Delete(record.Id)
	// if err != nil {
	// 	t.Fatalf("DELETE failed: %v", err)
	// }

	// result, err = client.Get(record.Id)
	// if err == nil {
	// 	t.Fatalf("GET after DELETE should fail")
	// }
	// if err.Error() != "Record not found" {
	// 	t.Fatalf("GET after DELETE unexpected err: %v", err)
	// }
}
