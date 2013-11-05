package main

import (
	. "github.com/flaub/kissdif"
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

	record, err := NewRecord("1", "", "Value")
	c.Assert(err, IsNil)

	client := NewClient(ts.URL)
	err = client.PutEnv("mem", "mem", Dictionary{})
	c.Assert(err, IsNil)

	err = client.Put("mem", "table", record)
	c.Assert(err, IsNil)

	result, err := client.Get("mem", "table", record.Id)
	c.Assert(err, IsNil)
	c.Assert(result.Doc, Equals, record.Doc)

	err = client.Delete("mem", "table", record.Id)
	c.Assert(err, IsNil)

	result, err = client.Get("mem", "table", record.Id)
	c.Assert(err, NotNil)
}

func (this *MainSuite) TestIndex(c *C) {
	ts := httptest.NewServer(NewServer().Server.Handler)
	defer ts.Close()

	record, err := NewRecord("1", "", "Value")
	c.Assert(err, IsNil)
	record.Keys["by_name"] = []string{"Joe", "Bob"}

	client := NewClient(ts.URL)

	err = client.PutEnv("mem", "mem", Dictionary{})
	c.Assert(err, IsNil)

	err = client.Put("mem", "table", record)
	c.Assert(err, IsNil)

	result, err := client.Get("mem", "table", record.Id)
	c.Assert(err, IsNil)
	c.Assert(result.Doc, Equals, record.Doc)

	result, err = client.GetBy("mem", "table", "by_name", "Joe")
	c.Assert(err, IsNil)
	c.Assert(result.Doc, Equals, record.Doc)

	err = client.Delete("mem", "table", record.Id)
	c.Assert(err, IsNil)

	result, err = client.Get("mem", "table", record.Id)
	c.Assert(err, ErrorMatches, "Record not found")
}

func (this *MainSuite) TestQuery(c *C) {
	ts := httptest.NewServer(NewServer().Server.Handler)
	defer ts.Close()

	client := NewClient(ts.URL)

	err := client.PutEnv("mem", "mem", Dictionary{})
	c.Assert(err, IsNil)

	record, err := NewRecord("1", "", "1")
	c.Assert(err, IsNil)
	record.Keys["by_name"] = []string{"Joe", "Bob"}
	err = client.Put("mem", "table", record)
	c.Assert(err, IsNil)

	record, err = NewRecord("2", "", "2")
	c.Assert(err, IsNil)
	record.Keys["by_name"] = []string{"Alice", "Carol"}
	err = client.Put("mem", "table", record)
	c.Assert(err, IsNil)

	record, err = NewRecord("3", "", "3")
	c.Assert(err, IsNil)
	err = client.Put("mem", "table", record)
	c.Assert(err, IsNil)

	query := NewQueryEQ("_id", "2", 10)
	result, err := client.Query("mem", "table", query)
	c.Assert(err, IsNil)
	c.Assert(result.Records, HasLen, 1, Commentf("%v", result))
	c.Assert(result.Records[0].Doc, Equals, "2")

	query = NewQueryGT("_id", "2", 10)
	result, err = client.Query("mem", "table", query)
	c.Assert(err, IsNil)
	c.Assert(result.Records, HasLen, 1, Commentf("%v", result))
	c.Assert(result.Records[0].Doc, Equals, "3")

	query = NewQueryGTE("_id", "2", 10)
	result, err = client.Query("mem", "table", query)
	c.Assert(err, IsNil)
	c.Assert(result.Records, HasLen, 2, Commentf("%v", result))
	c.Assert(result.Records[0].Doc, Equals, "2")
	c.Assert(result.Records[1].Doc, Equals, "3")

	query = NewQueryLT("_id", "2", 10)
	result, err = client.Query("mem", "table", query)
	c.Assert(err, IsNil)
	c.Assert(result.Records, HasLen, 1, Commentf("%v", result))
	c.Assert(result.Records[0].Doc, Equals, "1")

	query = NewQueryLTE("_id", "2", 10)
	result, err = client.Query("mem", "table", query)
	c.Assert(err, IsNil)
	c.Assert(result.Records, HasLen, 2, Commentf("%v", result))
	c.Assert(result.Records[0].Doc, Equals, "1")
	c.Assert(result.Records[1].Doc, Equals, "2")

	lower := &Bound{true, "1"}
	upper := &Bound{true, "2"}
	query = NewQuery("_id", lower, upper, 10)
	result, err = client.Query("mem", "table", query)
	c.Assert(err, IsNil)
	c.Assert(result.Records, HasLen, 2, Commentf("%v", result))
	c.Assert(result.Records[0].Doc, Equals, "1")
	c.Assert(result.Records[1].Doc, Equals, "2")
}
