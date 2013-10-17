package main

import (
	"github.com/flaub/kissdif/driver"
	"io/ioutil"
	. "launchpad.net/gocheck"
	"net/http"
	"net/http/httptest"
	"testing"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }

type MainSuite struct{}

func init() {
	Suite(&MainSuite{})
}

func NewRecord(id, doc string) *driver.Record {
	return &driver.Record{
		Id:   id,
		Doc:  []byte(doc),
		Keys: make(map[string][]string),
	}
}

func (this *MainSuite) TestServer(c *C) {
	ts := httptest.NewServer(NewServer().Server.Handler)
	defer ts.Close()

	res, err := http.Get(ts.URL + "/mem/table/_id/1")
	c.Assert(err, IsNil)
	defer res.Body.Close()

	result, err := ioutil.ReadAll(res.Body)
	c.Assert(err, IsNil)
	c.Assert(res.StatusCode, Equals, http.StatusNotFound)

	c.Logf("Result: %s", result)
}

func (this *MainSuite) TestBasic(c *C) {
	ts := httptest.NewServer(NewServer().Server.Handler)
	defer ts.Close()

	record := NewRecord("1", "Value")

	client := NewKissClient(ts.URL, "mem", "table")

	err := client.Put(record)
	c.Assert(err, IsNil)

	result, err := client.Get(record.Id)
	c.Assert(err, IsNil)
	c.Assert(result.Count, Equals, 1)
	c.Assert(result.Records, HasLen, 1)
	c.Assert(result.Records[0].Doc, Equals, record.Doc)

	err = client.Delete(record.Id)
	c.Assert(err, IsNil)

	result, err = client.Get(record.Id)
	c.Assert(err, ErrorMatches, "No records found")
}

func (this *MainSuite) TestIndex(c *C) {
	ts := httptest.NewServer(NewServer().Server.Handler)
	defer ts.Close()

	record := NewRecord("1", "Value")
	record.Keys["by_name"] = []string{"Joe", "Bob"}

	client := NewKissClient(ts.URL, "mem", "table")

	err := client.Put(record)
	c.Assert(err, IsNil)

	result, err := client.Get(record.Id)
	c.Assert(err, IsNil)
	c.Assert(result.Count, Equals, 1)
	c.Assert(result.Records, HasLen, 1)
	c.Assert(result.Records[0].Doc, Equals, record.Doc)

	result, err = client.GetWithIndex("by_name", "Joe")
	c.Assert(err, IsNil)
	c.Assert(result.Count, Equals, 1)
	c.Assert(result.Records, HasLen, 1)
	c.Assert(result.Records[0].Doc, Equals, record.Doc)

	err = client.Delete(record.Id)
	c.Assert(err, IsNil)

	result, err = client.Get(record.Id)
	c.Assert(err, ErrorMatches, "Record not found")
}

func (this *MainSuite) TestQuery(c *C) {
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
