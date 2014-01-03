package rql

import (
	"github.com/flaub/ergo"
	"github.com/flaub/kissdif"
	_ "github.com/flaub/kissdif/driver/mem"
	"github.com/flaub/kissdif/server"
	. "github.com/motain/gocheck"
	"net/http/httptest"
	"testing"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }

type TestSuite struct {
	conn Conn
}

type TestHttpSuite struct {
	TestSuite
	ts *httptest.Server
}

type TestLocalSuite struct {
	TestSuite
}

var (
	// _ = Suite(new(TestLocalSuite))
	_ = Suite(new(TestHttpSuite))
)

func (this *TestHttpSuite) SetUpTest(c *C) {
	this.ts = httptest.NewServer(server.NewServer().Server.Handler)
	var kerr *ergo.Error
	this.conn, kerr = Connect(this.ts.URL)
	c.Check(kerr, IsNil)
}

func (this *TestHttpSuite) TearDownTest(c *C) {
	this.ts.Close()
}

func (this *TestLocalSuite) SetUpTest(c *C) {
	var kerr *ergo.Error
	this.conn, kerr = Connect("local://")
	c.Check(kerr, IsNil)
}

type testDoc struct {
	Value string
}

func (this *TestSuite) TestBasic(c *C) {
	table := DB("db").Table("table")
	_, kerr := table.Get("1").Exec(nil)
	c.Check(kerr.Code, Equals, kissdif.EBadParam)

	db, kerr := this.conn.CreateDB("db", "mem", kissdif.Dictionary{})
	c.Check(kerr, IsNil)
	c.Check(db, NotNil)

	_, kerr = table.Exec(this.conn)
	cause := ergo.Cause(kerr)
	c.Check(cause.Code, Equals, kissdif.EBadTable)

	data := &testDoc{Value: "foo"}
	rev, kerr := table.Insert("$", data).Exec(this.conn)
	c.Check(kerr, IsNil)
	c.Check(rev, Not(Equals), "")

	result, kerr := table.Get("$").Exec(this.conn)
	c.Check(kerr, IsNil)
	c.Check(result.Id(), Equals, "$")
	doc := result.MustScan(&testDoc{}).(*testDoc)
	c.Check(doc, DeepEquals, data)

	kerr = table.Delete("$", rev).Exec(this.conn)
	c.Check(kerr, IsNil)

	result, kerr = table.Get("$").Exec(this.conn)
	c.Check(kerr.Code, Equals, kissdif.ENotFound)
}

func (this *TestSuite) TestIndex(c *C) {
	table := DB("db").Table("table")
	db, kerr := this.conn.CreateDB("db", "mem", kissdif.Dictionary{})
	c.Check(kerr, IsNil)
	c.Check(db, NotNil)

	value := "Value"
	rev, kerr := table.Insert("1", value).By("name", "Joe").By("name", "Bob").Exec(this.conn)
	c.Check(kerr, IsNil)
	c.Check(rev, Not(Equals), "")

	result, kerr := table.Get("1").Exec(this.conn)
	c.Check(kerr, IsNil)
	doc := ""
	result.MustScan(&doc)
	c.Check(doc, Equals, value)

	result, kerr = table.By("name").Get("Joe").Exec(this.conn)
	c.Check(kerr, IsNil)
	result.MustScan(&doc)
	c.Check(doc, Equals, value)

	result, kerr = table.By("name").Get("Bob").Exec(this.conn)
	c.Check(kerr, IsNil)
	result.MustScan(&doc)
	c.Check(doc, Equals, value)

	resultSet, kerr := table.By("name").Exec(this.conn)
	c.Check(kerr, IsNil)
	c.Check(resultSet.More(), Equals, false)
	c.Check(resultSet.Count(), Equals, 2)

	// drop index (Bob)
	rev, kerr = table.Update("1", rev, value).By("name", "Joe").Exec(this.conn)
	c.Check(kerr, IsNil)
	c.Check(rev, Not(Equals), "")

	result, kerr = table.By("name").Get("Joe").Exec(this.conn)
	c.Check(kerr, IsNil)
	result.MustScan(&doc)
	c.Check(doc, Equals, value)

	// Bob should now be gone
	result, kerr = table.By("name").Get("Bob").Exec(this.conn)
	c.Check(kerr, NotNil)

	// use alternate UpdateRecord API
	keys := make(kissdif.IndexMap)
	keys["name"] = []string{"Joe", "Bob"}
	rev, kerr = table.Insert("2", value).Keys(keys).Exec(this.conn)
	c.Check(kerr, IsNil)
	c.Check(rev, Not(Equals), "")

	record, kerr := table.Get("2").Exec(this.conn)
	c.Check(kerr, IsNil)
	record.MustScan(&doc)
	c.Check(doc, Equals, value)

	record.MustSet("Other")
	rev, kerr = table.UpdateRecord(record).Exec(this.conn)
	c.Check(kerr, IsNil)
	c.Check(rev, Not(Equals), "")

	record, kerr = table.Get("2").Exec(this.conn)
	c.Check(kerr, IsNil)
	record.MustScan(&doc)
	c.Check(doc, Equals, "Other")
}

func (this *TestSuite) insert(c *C, key, value string, keys kissdif.IndexMap) string {
	table := DB("db").Table("table")
	put := table.Insert(key, value)
	for index, list := range keys {
		for _, key := range list {
			put = put.By(index, key)
		}
	}
	rev, kerr := put.Exec(this.conn)
	c.Check(kerr, IsNil)
	c.Check(rev, Not(Equals), "")
	return rev
}

func (this *TestSuite) TestQuery(c *C) {
	table := DB("db").Table("table")
	db, kerr := this.conn.CreateDB("db", "mem", kissdif.Dictionary{})
	c.Check(kerr, IsNil)
	c.Check(db, NotNil)

	this.insert(c, "1", "1", nil)
	this.insert(c, "2", "2", kissdif.IndexMap{"name": []string{"Alice", "Carol"}})
	this.insert(c, "3", "3", nil)

	result, kerr := table.Get("2").Exec(this.conn)
	c.Check(kerr, IsNil)
	doc := ""
	result.MustScan(&doc)
	c.Check(doc, Equals, "2")

	rs, kerr := table.Between("3", "9").Exec(this.conn)
	c.Check(kerr, IsNil)
	c.Check(rs.More(), Equals, false)
	c.Check(rs.Count(), Equals, 1)
	reader := rs.Reader()
	c.Check(reader.Next(), Equals, true)
	reader.MustScan(&doc)
	c.Check(doc, Equals, "3")
	c.Check(reader.Next(), Equals, false)

	rs, kerr = table.Between("2", "9").Exec(this.conn)
	c.Check(kerr, IsNil)
	c.Check(rs.More(), Equals, false)
	c.Check(rs.Count(), Equals, 2)
	reader = rs.Reader()
	c.Check(reader.Next(), Equals, true)
	reader.MustScan(&doc)
	c.Check(doc, Equals, "2")
	c.Check(reader.Next(), Equals, true)
	reader.MustScan(&doc)
	c.Check(doc, Equals, "3")
	c.Check(reader.Next(), Equals, false)

	rs, kerr = table.Between("1", "3").Exec(this.conn)
	c.Check(kerr, IsNil)
	c.Check(rs.More(), Equals, false)
	c.Check(rs.Count(), Equals, 2)
	reader = rs.Reader()
	c.Check(reader.Next(), Equals, true)
	reader.MustScan(&doc)
	c.Check(doc, Equals, "1")
	c.Check(reader.Next(), Equals, true)
	reader.MustScan(&doc)
	c.Check(doc, Equals, "2")
	c.Check(reader.Next(), Equals, false)
}

func (this *TestSuite) TestPathLikeKey(c *C) {
	table := DB("db").Table("table")
	db, kerr := this.conn.CreateDB("db", "mem", kissdif.Dictionary{})
	c.Check(kerr, IsNil)
	c.Check(db, NotNil)

	data := &testDoc{Value: "foo"}
	rev, kerr := table.Insert("/", data).Exec(this.conn)
	c.Check(kerr, IsNil)
	c.Check(rev, Not(Equals), "")

	result, kerr := table.Get("/").Exec(this.conn)
	c.Check(kerr, IsNil)
	c.Check(result.Id(), Equals, "/")
	doc := result.MustScan(&testDoc{})
	c.Check(doc, DeepEquals, data)

	kerr = table.Delete("/", rev).Exec(this.conn)
	c.Check(kerr, IsNil)

	result, kerr = table.Get("/").Exec(this.conn)
	c.Check(kerr.Code, Equals, kissdif.ENotFound)
}

func (this *TestSuite) TestUpdate(c *C) {
	table := DB("db").Table("table")
	db, kerr := this.conn.CreateDB("db", "mem", kissdif.Dictionary{})
	c.Check(kerr, IsNil)
	c.Check(db, NotNil)

	data := &testDoc{Value: "foo"}
	rev, kerr := table.Insert("/", data).Exec(this.conn)
	c.Check(kerr, IsNil)
	c.Check(rev, Not(Equals), "")

	data2 := &testDoc{Value: "bar"}
	rev2, kerr := table.Update("/", rev, data2).Exec(this.conn)
	c.Check(kerr, IsNil)
	c.Check(rev2, Not(Equals), "")
	c.Check(rev2, Not(Equals), rev)

	result, kerr := table.Get("/").Exec(this.conn)
	c.Check(kerr, IsNil)
	c.Check(result.Id(), Equals, "/")
	doc := result.MustScan(&testDoc{})
	c.Check(doc, DeepEquals, data2)
}
