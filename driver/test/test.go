package test

import (
	. "github.com/flaub/kissdif"
	. "github.com/flaub/kissdif/driver"
	. "launchpad.net/gocheck"
)

type TestSuite struct {
	name   string
	db     Database
	Config Dictionary
	table  Table
	c      *C
}

type expectedQuery struct {
	index    string
	lower    *Bound
	upper    *Bound
	expected []string
}

func NewTestSuite(name string) *TestSuite {
	return &TestSuite{name: name}
}

func (this *TestSuite) putValues(values ...string) {
	for _, value := range values {
		this.putRecordFull(value, "", value, IndexMap{})
	}
}

func (this *TestSuite) putRecord(value string, keys IndexMap) string {
	return this.putRecordFull(value, "", value, keys)
}

func (this *TestSuite) putRecordFull(id, rev, value string, keys IndexMap) string {
	record := &Record{Id: id, Rev: rev, Doc: value, Keys: keys}
	rev, err := this.table.Put(record)
	this.c.Assert(err, IsNil, Commentf("Record: %v", record))
	this.c.Assert(rev, Not(Equals), "", Commentf("Record: %v", record))
	return rev
}

// mb = make bound
func mb(value string, inclusive bool) *Bound {
	return &Bound{inclusive, value}
}

func (this *TestSuite) expect(test expectedQuery, expectedEof bool, limit uint) {
	query := &Query{
		Limit: limit,
		Index: test.index,
		Lower: test.lower,
		Upper: test.upper,
	}
	ch, err := this.table.Get(query)
	this.c.Assert(err, IsNil)
	actual := []string{}
	eof := false
	for record := range ch {
		if record == nil {
			eof = true
		} else {
			actual = append(actual, record.Doc.(string))
		}
	}
	this.c.Check(actual, DeepEquals, test.expected, Commentf("Query: %v", query))
	this.c.Check(eof, Equals, expectedEof, Commentf("Query: %v", query))
}

func (this *TestSuite) query(eof bool, limit uint, set []expectedQuery) {
	for _, test := range set {
		this.expect(test, eof, limit)
	}
}

func (this *TestSuite) SetUpTest(c *C) {
	drv, err := Open(this.name)
	c.Assert(err, IsNil)
	this.db, err = drv.Configure("db", this.Config)
	c.Assert(err, IsNil)
	c.Assert(this.db, NotNil)
	this.table, err = this.db.GetTable("table", true)
	c.Assert(err, IsNil)
	c.Assert(this.table, NotNil)
}

func (this *TestSuite) TestBasic(c *C) {
	this.c = c
	query := &Query{}
	_, err := this.table.Get(query)
	c.Assert(err.Code, Equals, EBadIndex)

	query.Index = "_id"
	_, err = this.table.Get(query)
	c.Assert(err.Code, Equals, EBadParam)

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

func (this *TestSuite) TestAltKey(c *C) {
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
	this.putRecord("e", IndexMap{
		"x": []string{"e_x1", "e_x2"},
	})

	this.query(true, 10, []expectedQuery{
		{"_id", nil, nil, []string{"a", "aa", "b", "c", "d", "e"}},
		{"_id", mb("a", true), mb("a", true), []string{"a"}},
		{"x", nil, nil, []string{"a", "aa", "b", "d", "e", "e"}},
		{"y", nil, nil, []string{"a", "b"}},
		{"c", nil, nil, []string{"c"}},
		{"x", mb("a_x", true), mb("a_x", true), []string{"a", "aa"}},
		{"x", mb("a", true), mb("c", true), []string{"a", "aa", "b"}},
		{"x", mb("e_x1", true), mb("e_x1", true), []string{"e"}},
		{"x", mb("e", true), mb("f", true), []string{"e", "e"}},
	})
}

func (this *TestSuite) TestUpdates(c *C) {
	this.c = c
	// identical update
	rev := this.putRecord("a", IndexMap{
		"x": []string{"a_x"},
		"y": []string{"a_y"},
	})
	this.putRecordFull("a", rev, "a", IndexMap{
		"x": []string{"a_x"},
		"y": []string{"a_y"},
	})
	// modify value
	rev = this.putRecord("b", IndexMap{
		"x": []string{"b_x"},
		"y": []string{"b_y"},
	})
	this.putRecordFull("b", rev, "bb", IndexMap{
		"x": []string{"b_x"},
		"y": []string{"b_y"},
	})
	// drop an alt key
	rev = this.putRecord("c", IndexMap{
		"x": []string{"c_x"},
		"y": []string{"c_y"},
	})
	this.putRecordFull("c", rev, "c", IndexMap{
		"x": []string{"c_x"},
	})
	// drop all alt keys
	rev = this.putRecord("d", IndexMap{
		"x": []string{"d_x"},
		"y": []string{"d_y"},
	})
	this.putRecordFull("d", rev, "d", IndexMap{})
	// add extra alt key
	rev = this.putRecord("e", IndexMap{
		"x": []string{"e_x"},
	})
	this.putRecordFull("e", rev, "e", IndexMap{
		"x": []string{"e_x"},
		"y": []string{"e_y"},
	})
	// add another record to index
	rev = this.putRecord("f", IndexMap{
		"x": []string{"a_x"},
	})
	// remove extra record from index
	rev = this.putRecord("g", IndexMap{
		"x": []string{"a_x"},
	})
	this.putRecordFull("g", rev, "g", IndexMap{})

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

func (this *TestSuite) TestMVCC(c *C) {
	this.c = c

	record := &Record{Id: "a", Doc: "a"}
	prev, err := this.table.Put(record)
	this.c.Assert(err, IsNil)
	this.c.Assert(prev, Not(Equals), "")

	record = &Record{Id: "a", Doc: "a"}
	cur, err := this.table.Put(record)
	this.c.Assert(cur, Equals, "")
	this.c.Assert(err, NotNil)
	this.c.Assert(err.Code, Equals, EConflict)

	record = &Record{Id: "a", Rev: prev, Doc: "a"}
	cur, err = this.table.Put(record)
	this.c.Assert(err, IsNil)
	this.c.Assert(cur, Equals, prev)

	prev = cur
	record = &Record{Id: "a", Rev: cur, Doc: "b"}
	cur, err = this.table.Put(record)
	this.c.Assert(err, IsNil)
	this.c.Assert(cur, Not(Equals), prev)

	record = &Record{Id: "a", Rev: "xxx", Doc: "b"}
	cur, err = this.table.Put(record)
	this.c.Assert(cur, Equals, "")
	this.c.Assert(err, NotNil)
	this.c.Assert(err.Code, Equals, EConflict)
}
