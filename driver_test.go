package main

import (
	. "github.com/flaub/kissdif/driver"
	"io/ioutil"
	. "launchpad.net/gocheck"
	"net/http"
	"os"
)

type TestSuite struct {
	name   string
	env    Environment
	config Dictionary
	table  Table
	c      *C
}

type TestDriverMemory struct {
	TestSuite
}

type TestDriverSql struct {
	TestSuite
}

type expectedQuery struct {
	index    string
	lower    *Bound
	upper    *Bound
	expected []string
}

func init() {
	Suite(&TestDriverMemory{TestSuite: TestSuite{name: "mem"}})
	Suite(&TestDriverSql{TestSuite: TestSuite{name: "sql"}})
}

func (this *TestSuite) putValues(values ...string) {
	for _, value := range values {
		record := &Record{Id: value, Doc: value}
		err := this.table.Put(record)
		this.c.Assert(err, IsNil)
	}
}

func (this *TestSuite) putRecord(value string, keys IndexMap) {
	this.putRecordKey(value, value, keys)
}

func (this *TestSuite) putRecordKey(key, value string, keys IndexMap) {
	record := &Record{Id: key, Doc: value, Keys: keys}
	err := this.table.Put(record)
	this.c.Assert(err, IsNil)
}

// mb = make bound
func mb(value string, inclusive bool) *Bound {
	return &Bound{inclusive, value}
}

func (this *TestSuite) expect(test expectedQuery, expectedEof bool, limit int) {
	query := &Query{
		Limit: limit,
		Index: test.index,
		Lower: test.lower,
		Upper: test.upper,
	}
	this.c.Logf("Query: %v", query)
	ch, err := this.table.Get(query)
	this.c.Assert(err, IsNil)
	actual := []string{}
	eof := false
	for record := range ch {
		if record == nil {
			eof = true
		} else {
			actual = append(actual, record.Doc)
		}
	}
	this.c.Check(actual, DeepEquals, test.expected)
	this.c.Check(eof, Equals, expectedEof)
}

func (this *TestSuite) query(eof bool, limit int, set []expectedQuery) {
	for _, test := range set {
		this.expect(test, eof, limit)
	}
}

func getTemp(c *C) string {
	ftmp, err := ioutil.TempFile("", "")
	c.Assert(err, IsNil)
	defer ftmp.Close()
	return ftmp.Name()
}

func (this *TestSuite) SetUpTest(c *C) {
	db, err := Open(this.name)
	this.env, err = db.Configure("env", this.config)
	c.Assert(err, IsNil)
	c.Assert(this.env, NotNil)
	this.table, err = this.env.GetTable("table", true)
	c.Assert(err, IsNil)
	c.Assert(this.table, NotNil)
}

func (this *TestDriverSql) SetUpTest(c *C) {
	this.config = make(Dictionary)
	this.config["dsn"] = getTemp(c) + ".db"
	this.TestSuite.SetUpTest(c)
}

func (this *TestDriverSql) TearDownTest(c *C) {
	path := this.config["dsn"]
	c.Logf("Removing %q", path)
	os.Remove(path)
}

func (this *TestSuite) TestBasic(c *C) {
	this.c = c
	query := &Query{}
	_, err := this.table.Get(query)
	c.Assert(err, ErrorMatches, "Invalid index")
	c.Assert(err.Status, Equals, http.StatusBadRequest)

	query.Index = "_id"
	_, err = this.table.Get(query)
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

	this.putValues("a")
	this.query(true, 10, []expectedQuery{
		{"_id", nil, nil, []string{"a"}},
	})
}

func (this *TestSuite) TestLowerBound(c *C) {
	this.c = c
	this.putValues("b", "c", "d")

	this.query(true, 10, []expectedQuery{
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
	})
}

func (this *TestSuite) TestUpperBound(c *C) {
	this.c = c
	this.putValues("b", "c", "d")

	this.query(true, 10, []expectedQuery{
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
	})
}

func (this *TestSuite) TestRange(c *C) {
	this.c = c
	this.putValues("b", "c", "d")

	this.query(true, 10, []expectedQuery{
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
	})
}

func (this *TestSuite) TestMultiIndex(c *C) {
	this.c = c
	this.putRecord("a", IndexMap{
		"x": []string{"a_x"},
		"y": []string{"a_y"},
	})
	this.putRecord("b", IndexMap{
		"x": []string{"b_x"},
		"y": []string{"b_y"},
	})
	this.putRecord("aa", IndexMap{
		"x": []string{"a_x"},
	})
	this.putRecord("c", IndexMap{
		"c": []string{"ccc"},
	})
	this.putRecord("d", IndexMap{
		"x": []string{"d_x"},
	})

	this.query(true, 10, []expectedQuery{
		{"_id", nil, nil, []string{"a", "aa", "b", "c", "d"}},
		{"_id", mb("a", true), mb("a", true), []string{"a"}},
		{"x", nil, nil, []string{"a", "aa", "b", "d"}},
		{"y", nil, nil, []string{"a", "b"}},
		{"c", nil, nil, []string{"c"}},
		{"x", mb("a_x", true), mb("a_x", true), []string{"a", "aa"}},
		{"x", mb("a", true), mb("c", true), []string{"a", "aa", "b"}},
	})
}

func (this *TestSuite) TestUpdates(c *C) {
	this.c = c
	// identical update
	this.putRecord("a", IndexMap{
		"x": []string{"a_x"},
		"y": []string{"a_y"},
	})
	this.putRecord("a", IndexMap{
		"x": []string{"a_x"},
		"y": []string{"a_y"},
	})
	// modify value
	this.putRecordKey("b", "b", IndexMap{
		"x": []string{"b_x"},
		"y": []string{"b_y"},
	})
	this.putRecordKey("b", "bb", IndexMap{
		"x": []string{"b_x"},
		"y": []string{"b_y"},
	})
	// drop an alt key
	this.putRecord("c", IndexMap{
		"x": []string{"c_x"},
		"y": []string{"c_y"},
	})
	this.putRecord("c", IndexMap{
		"x": []string{"c_x"},
	})
	// drop all alt keys
	this.putRecord("d", IndexMap{
		"x": []string{"d_x"},
		"y": []string{"d_y"},
	})
	this.putRecord("d", IndexMap{})
	// add extra alt key
	this.putRecord("e", IndexMap{
		"x": []string{"e_x"},
	})
	this.putRecord("e", IndexMap{
		"x": []string{"e_x"},
		"y": []string{"e_y"},
	})
	// add another record to index
	this.putRecord("f", IndexMap{
		"x": []string{"a_x"},
	})
	// remove extra record from index
	this.putRecord("g", IndexMap{
		"x": []string{"a_x"},
	})
	this.putRecord("g", IndexMap{})

	this.query(true, 10, []expectedQuery{
		{"_id", nil, nil, []string{"a", "bb", "c", "d", "e", "f", "g"}},
		{"x", nil, nil, []string{"a", "f", "bb", "c", "e"}},
		{"y", nil, nil, []string{"a", "bb", "e"}},
	})
}

func (this *TestSuite) TestLimit(c *C) {
	this.c = c
	this.putValues("1", "2", "3")
	this.query(false, 1, []expectedQuery{
		{"_id", nil, nil, []string{"1"}},
	})
	this.query(false, 2, []expectedQuery{
		{"_id", nil, nil, []string{"1", "2"}},
	})
	this.query(true, 3, []expectedQuery{
		{"_id", nil, nil, []string{"1", "2", "3"}},
	})
}

func (this *TestSuite) TestDelete(c *C) {
	this.c = c
	this.putValues("a", "b")
	this.putRecord("c", IndexMap{
		"x": []string{"x"},
	})
	this.c.Assert(this.table.Delete("a"), IsNil)
	this.query(true, 10, []expectedQuery{
		{"_id", nil, nil, []string{"b", "c"}},
		{"x", nil, nil, []string{"c"}},
	})
	this.c.Assert(this.table.Delete("a"), IsNil)
	this.c.Assert(this.table.Delete("b"), IsNil)
	this.query(true, 10, []expectedQuery{
		{"_id", nil, nil, []string{"c"}},
		{"x", nil, nil, []string{"c"}},
	})
	this.c.Assert(this.table.Delete("c"), IsNil)
	this.query(true, 10, []expectedQuery{
		{"_id", nil, nil, []string{}},
		{"x", nil, nil, []string{}},
	})
}
