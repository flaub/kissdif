package main

import (
	"github.com/flaub/kissdif/driver"
	"io/ioutil"
	. "launchpad.net/gocheck"
	"net/http"
	"os"
)

type TestSuite struct {
	name   string
	env    driver.Environment
	config driver.Dictionary
}

type TestDriverMemory struct {
	TestSuite
}

type TestDriverSql struct {
	TestSuite
}

func init() {
	Suite(&TestDriverMemory{TestSuite: TestSuite{name: "mem"}})
	Suite(&TestDriverSql{TestSuite: TestSuite{name: "sql"}})
}

func putValues(c *C, table driver.Table, values ...string) {
	for _, value := range values {
		record := &driver.Record{Id: value, Doc: value}
		err := table.Put(record)
		c.Assert(err, IsNil)
	}
}

func putRecord(c *C, table driver.Table, value string, keys driver.IndexMap) {
	record := &driver.Record{Id: value, Doc: value, Keys: keys}
	err := table.Put(record)
	c.Assert(err, IsNil)
}

func expect(c *C, table driver.Table, query *driver.Query, expectedEof bool, expected ...string) {
	c.Logf("Query: %v", query)
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
	this.env, err = db.Configure("env", this.config)
	c.Assert(err, IsNil)
	c.Assert(this.env, NotNil)
}

func getTemp(c *C) string {
	ftmp, err := ioutil.TempFile("", "")
	c.Assert(err, IsNil)
	defer ftmp.Close()
	return ftmp.Name()
}

func (this *TestDriverSql) SetUpTest(c *C) {
	this.config = make(driver.Dictionary)
	this.config["dsn"] = getTemp(c) + ".db"
	this.TestSuite.SetUpTest(c)
}

func (this *TestDriverSql) TearDownTest(c *C) {
	path := this.config["dsn"]
	c.Logf("Removing %q", path)
	os.Remove(path)
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
	// query.Index = "_does_not_exist_"
	// _, err = table.Get(query)
	// c.Assert(err, NotNil)
	// c.Assert(err, ErrorMatches, "Index not found")
	// c.Assert(err.Status, Equals, http.StatusNotFound)

	// query.Index = "_id"
	// _, err = table.Get(query)
	// c.Assert(err, NotNil)
	// c.Assert(err, ErrorMatches, "No records found")
	// c.Assert(err.Status, Equals, http.StatusNotFound)

	putValues(c, table, "a")
	expect(c, table, query, true, "a")
}

func (this *TestSuite) TestLowerBound(c *C) {
	table, err := this.env.GetTable("table", true)
	c.Assert(err, IsNil)

	putValues(c, table, "b", "c", "d")

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

	putValues(c, table, "b", "c", "d")

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

	putValues(c, table, "b", "c", "d")

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

func mb(value string, inclusive bool) *driver.Bound {
	return &driver.Bound{inclusive, value}
}

func (this *TestSuite) TestMultiIndex(c *C) {
	table, err := this.env.GetTable("table", true)
	c.Assert(err, IsNil)

	putRecord(c, table, "a", driver.IndexMap{
		"x": []string{"a_x"},
		"y": []string{"a_y"},
	})
	putRecord(c, table, "b", driver.IndexMap{
		"x": []string{"b_x"},
		"y": []string{"b_y"},
	})
	putRecord(c, table, "aa", driver.IndexMap{
		"x": []string{"a_x"},
	})
	putRecord(c, table, "c", driver.IndexMap{
		"c": []string{"ccc"},
	})
	putRecord(c, table, "d", driver.IndexMap{
		"x": []string{"d_x"},
	})

	query := &driver.Query{
		Limit: 10,
	}

	tt := []struct {
		index    string
		lower    *driver.Bound
		upper    *driver.Bound
		expected []string
	}{
		{"_id", nil, nil, []string{"a", "aa", "b", "c", "d"}},
		{"_id", mb("a", true), mb("a", true), []string{"a"}},
		{"x", nil, nil, []string{"a", "aa", "b", "d"}},
		{"y", nil, nil, []string{"a", "b"}},
		{"c", nil, nil, []string{"c"}},
		{"x", mb("a_x", true), mb("a_x", true), []string{"a", "aa"}},
		{"x", mb("a", true), mb("c", true), []string{"a", "aa", "b"}},
	}

	for _, test := range tt {
		query.Index = test.index
		query.Lower = test.lower
		query.Upper = test.upper
		expect(c, table, query, true, test.expected...)
	}
}
