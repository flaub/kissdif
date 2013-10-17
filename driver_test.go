package main

import (
	"github.com/flaub/kissdif/driver"
	_ "github.com/flaub/kissdif/driver/mem"
	_ "github.com/flaub/kissdif/driver/sql"
	. "launchpad.net/gocheck"
	"net/http"
)

type TestSuite struct {
	name string
	env  driver.Environment
}

type MemoryDriver struct {
	TestSuite
}

type SqlDriver struct {
	TestSuite
}

func init() {
	Suite(&MemoryDriver{TestSuite: TestSuite{name: "mem"}})
	Suite(&SqlDriver{TestSuite: TestSuite{name: "sql"}})
}

func put(c *C, table driver.Table, values ...string) {
	for _, value := range values {
		record := &driver.Record{Id: value, Doc: []byte(value)}
		err := table.Put(record)
		c.Assert(err, IsNil)
	}
}

func expect(c *C, table driver.Table, query *driver.Query, expectedEof bool, expected ...string) {
	ch, err := table.Get(query)
	c.Assert(err, IsNil)
	actual := []string{}
	eof := false
	for record := range ch {
		if record == nil {
			eof = true
		} else {
			actual = append(actual, string(record.Doc))
		}
	}
	c.Assert(actual, DeepEquals, expected)
	c.Assert(eof, Equals, expectedEof)
}

func (this *TestSuite) SetUpTest(c *C) {
	db, err := driver.Open(this.name)
	config := map[string]string{}
	this.env, err = db.Configure("env", config)
	c.Assert(err, IsNil)
	c.Assert(this.env, NotNil)
}

func (this *TestSuite) TestBasic(c *C) {
	table, err := this.env.GetTable("table", true)
	c.Assert(err, IsNil)
	c.Assert(table, NotNil)

	query := &driver.Query{}
	_, err = table.Get(query)
	c.Assert(err, ErrorMatches, "Invalid index")
	c.Assert(err.Status, Equals, http.StatusBadRequest)

	query.Index = "_id"
	_, err = table.Get(query)
	c.Assert(err, ErrorMatches, "Invalid limit")
	c.Assert(err.Status, Equals, http.StatusBadRequest)

	query.Limit = 10
	query.Index = "_does_not_exist_"
	_, err = table.Get(query)
	c.Assert(err, ErrorMatches, "Index not found")
	c.Assert(err.Status, Equals, http.StatusNotFound)

	query.Index = "_id"
	_, err = table.Get(query)
	c.Assert(err, ErrorMatches, "No records found")
	c.Assert(err.Status, Equals, http.StatusNotFound)

	put(c, table, "a")
	expect(c, table, query, true, "a")
}

func (this *TestSuite) TestLowerBound(c *C) {
	table, err := this.env.GetTable("table", true)
	c.Assert(err, IsNil)

	put(c, table, "b", "c", "d")

	query := &driver.Query{
		Index: "_id",
		Limit: 10,
	}

	tt := []struct {
		key      string
		inc      bool
		expected []string
	}{
		{"a", true, []string{"b", "c", "d"}},
		{"a", false, []string{"b", "c", "d"}},
		{"b", true, []string{"b", "c", "d"}},
		{"b", false, []string{"c", "d"}},
		{"c", true, []string{"c", "d"}},
		{"c", false, []string{"d"}},
		{"d", true, []string{"d"}},
		{"d", false, []string{}},
		{"e", true, []string{}},
		{"e", false, []string{}},
	}

	for _, test := range tt {
		query.Lower = &driver.Bound{test.inc, test.key}
		expect(c, table, query, true, test.expected...)
	}
}

func (this *TestSuite) TestUpperBound(c *C) {
	table, err := this.env.GetTable("table", true)
	c.Assert(err, IsNil)

	put(c, table, "b", "c", "d")

	query := &driver.Query{
		Index: "_id",
		Limit: 10,
	}

	tt := []struct {
		key      string
		inc      bool
		expected []string
	}{
		{"a", true, []string{}},
		{"a", false, []string{}},
		{"b", true, []string{"b"}},
		{"b", false, []string{}},
		{"c", true, []string{"b", "c"}},
		{"c", false, []string{"b"}},
		{"d", true, []string{"b", "c", "d"}},
		{"d", false, []string{"b", "c"}},
		{"e", true, []string{"b", "c", "d"}},
		{"e", false, []string{"b", "c", "d"}},
	}

	for _, test := range tt {
		query.Upper = &driver.Bound{test.inc, test.key}
		expect(c, table, query, true, test.expected...)
	}
}

func (this *TestSuite) TestRange(c *C) {
	table, err := this.env.GetTable("table", true)
	c.Assert(err, IsNil)

	put(c, table, "b", "c", "d")

	query := &driver.Query{
		Index: "_id",
		Limit: 10,
	}

	tt := []struct {
		lkey     string
		linc     bool
		ukey     string
		uinc     bool
		expected []string
	}{
		{"a", true, "a", true, []string{}},
		{"b", true, "b", true, []string{"b"}},
		{"c", true, "c", true, []string{"c"}},
		{"d", true, "d", true, []string{"d"}},
		{"e", true, "e", true, []string{}},
		{"a", true, "e", true, []string{"b", "c", "d"}},
		{"a", false, "e", false, []string{"b", "c", "d"}},
		{"a", true, "b", false, []string{}},
		{"a", true, "b", true, []string{"b"}},
		{"b", true, "e", true, []string{"b", "c", "d"}},
		{"b", false, "e", true, []string{"c", "d"}},
	}

	for _, test := range tt {
		query.Lower = &driver.Bound{test.linc, test.lkey}
		query.Upper = &driver.Bound{test.uinc, test.ukey}
		expect(c, table, query, true, test.expected...)
	}
}
