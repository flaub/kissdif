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

func putRecordKey(c *C, table driver.Table, key, value string, keys driver.IndexMap) {
	record := &driver.Record{Id: key, Doc: value, Keys: keys}
	err := table.Put(record)
	c.Assert(err, IsNil)
}

// mb = make bound
func mb(value string, inclusive bool) *driver.Bound {
	return &driver.Bound{inclusive, value}
}

type expectedQuery struct {
	index    string
	lower    *driver.Bound
	upper    *driver.Bound
	expected []string
}

type expectedQuerySet []expectedQuery

func (this expectedQuery) expect(c *C, table driver.Table, expectedEof bool, limit int) {
	query := &driver.Query{
		Limit: limit,
		Index: this.index,
		Lower: this.lower,
		Upper: this.upper,
	}
	c.Logf("Query: %v", query)
	ch, err := table.Get(query)
	c.Assert(err, IsNil)
	actual := []string{}
	eof := false
	for record := range ch {
		if record == nil {
			eof = true
		} else {
			actual = append(actual, record.Doc)
		}
	}
	c.Check(actual, DeepEquals, this.expected)
	c.Check(eof, Equals, expectedEof)
}

func (this expectedQuerySet) run(c *C, table driver.Table, eof bool, limit int) {
	for _, test := range this {
		test.expect(c, table, eof, limit)
	}
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
	expectedQuerySet{
		{"_id", nil, nil, []string{"a"}},
	}.run(c, table, true, 10)
}

func (this *TestSuite) TestLowerBound(c *C) {
	table, err := this.env.GetTable("table", true)
	c.Assert(err, IsNil)

	putValues(c, table, "b", "c", "d")

	expectedQuerySet{
		{"_id", mb("a", true), nil, []string{"b", "c", "d"}},
		{"_id", mb("a", false), nil, []string{"b", "c", "d"}},
		{"_id", mb("b", true), nil, []string{"b", "c", "d"}},
		{"_id", mb("b", false), nil, []string{"c", "d"}},
		{"_id", mb("c", true), nil, []string{"c", "d"}},
		{"_id", mb("c", false), nil, []string{"d"}},
		{"_id", mb("d", true), nil, []string{"d"}},
		{"_id", mb("d", false), nil, []string{}},
		{"_id", mb("e", true), nil, []string{}},
		{"_id", mb("e", false), nil, []string{}},
	}.run(c, table, true, 10)
}

func (this *TestSuite) TestUpperBound(c *C) {
	table, err := this.env.GetTable("table", true)
	c.Assert(err, IsNil)

	putValues(c, table, "b", "c", "d")

	expectedQuerySet{
		{"_id", nil, mb("a", true), []string{}},
		{"_id", nil, mb("a", false), []string{}},
		{"_id", nil, mb("b", true), []string{"b"}},
		{"_id", nil, mb("b", false), []string{}},
		{"_id", nil, mb("c", true), []string{"b", "c"}},
		{"_id", nil, mb("c", false), []string{"b"}},
		{"_id", nil, mb("d", true), []string{"b", "c", "d"}},
		{"_id", nil, mb("d", false), []string{"b", "c"}},
		{"_id", nil, mb("e", true), []string{"b", "c", "d"}},
		{"_id", nil, mb("e", false), []string{"b", "c", "d"}},
	}.run(c, table, true, 10)
}

func (this *TestSuite) TestRange(c *C) {
	table, err := this.env.GetTable("table", true)
	c.Assert(err, IsNil)

	putValues(c, table, "b", "c", "d")

	expectedQuerySet{
		{"_id", mb("a", true), mb("a", true), []string{}},
		{"_id", mb("b", true), mb("b", true), []string{"b"}},
		{"_id", mb("c", true), mb("c", true), []string{"c"}},
		{"_id", mb("d", true), mb("d", true), []string{"d"}},
		{"_id", mb("e", true), mb("e", true), []string{}},
		{"_id", mb("a", true), mb("e", true), []string{"b", "c", "d"}},
		{"_id", mb("a", false), mb("e", false), []string{"b", "c", "d"}},
		{"_id", mb("a", true), mb("b", false), []string{}},
		{"_id", mb("a", true), mb("b", true), []string{"b"}},
		{"_id", mb("b", true), mb("e", true), []string{"b", "c", "d"}},
		{"_id", mb("b", false), mb("e", true), []string{"c", "d"}},
	}.run(c, table, true, 10)
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

	expectedQuerySet{
		{"_id", nil, nil, []string{"a", "aa", "b", "c", "d"}},
		{"_id", mb("a", true), mb("a", true), []string{"a"}},
		{"x", nil, nil, []string{"a", "aa", "b", "d"}},
		{"y", nil, nil, []string{"a", "b"}},
		{"c", nil, nil, []string{"c"}},
		{"x", mb("a_x", true), mb("a_x", true), []string{"a", "aa"}},
		{"x", mb("a", true), mb("c", true), []string{"a", "aa", "b"}},
	}.run(c, table, true, 10)
}

func (this *TestSuite) TestUpdates(c *C) {
	table, err := this.env.GetTable("table", true)
	c.Assert(err, IsNil)

	// identical update
	putRecord(c, table, "a", driver.IndexMap{
		"x": []string{"a_x"},
		"y": []string{"a_y"},
	})
	putRecord(c, table, "a", driver.IndexMap{
		"x": []string{"a_x"},
		"y": []string{"a_y"},
	})
	// modify value
	putRecordKey(c, table, "b", "b", driver.IndexMap{
		"x": []string{"b_x"},
		"y": []string{"b_y"},
	})
	putRecordKey(c, table, "b", "bb", driver.IndexMap{
		"x": []string{"b_x"},
		"y": []string{"b_y"},
	})
	// drop an alt key
	putRecord(c, table, "c", driver.IndexMap{
		"x": []string{"c_x"},
		"y": []string{"c_y"},
	})
	putRecord(c, table, "c", driver.IndexMap{
		"x": []string{"c_x"},
	})
	// drop all alt keys
	putRecord(c, table, "d", driver.IndexMap{
		"x": []string{"d_x"},
		"y": []string{"d_y"},
	})
	putRecord(c, table, "d", driver.IndexMap{})
	// add extra alt key
	putRecord(c, table, "e", driver.IndexMap{
		"x": []string{"e_x"},
	})
	putRecord(c, table, "e", driver.IndexMap{
		"x": []string{"e_x"},
		"y": []string{"e_y"},
	})
	// add another record to index
	putRecord(c, table, "f", driver.IndexMap{
		"x": []string{"a_x"},
	})
	// remove extra record from index
	putRecord(c, table, "g", driver.IndexMap{
		"x": []string{"a_x"},
	})
	putRecord(c, table, "g", driver.IndexMap{})

	expectedQuerySet{
		{"_id", nil, nil, []string{"a", "bb", "c", "d", "e", "f", "g"}},
		{"x", nil, nil, []string{"a", "f", "bb", "c", "e"}},
		{"y", nil, nil, []string{"a", "bb", "e"}},
	}.run(c, table, true, 10)
}
