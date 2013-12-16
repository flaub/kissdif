package rql

import (
	"github.com/flaub/kissdif"
	_ "github.com/flaub/kissdif/driver/mem"
	"github.com/flaub/kissdif/server"
	. "launchpad.net/gocheck"
	"net/http"
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

func init() {
	Suite(&TestHttpSuite{})
	// Suite(&TestLocalSuite{})
}

func (this *TestHttpSuite) SetUpTest(c *C) {
	this.ts = httptest.NewServer(server.NewServer().Server.Handler)
	var kerr *kissdif.Error
	this.conn, kerr = Connect(this.ts.URL)
	c.Assert(kerr, IsNil)
}

func (this *TestHttpSuite) TearDownTest(c *C) {
	this.ts.Close()
}

func (this *TestLocalSuite) SetUpTest(c *C) {
	var kerr *kissdif.Error
	this.conn, kerr = Connect("local://")
	c.Assert(kerr, IsNil)
}

type testDoc struct {
	Value string
}

func (this *TestSuite) TestBasic(c *C) {
	_, kerr := DB("db").Table("table").Get("1").Run(nil)
	c.Assert(kerr.Status, Equals, http.StatusBadRequest)
	c.Assert(kerr, ErrorMatches, "conn must not be null")

	db, kerr := this.conn.CreateDB("db", "mem", kissdif.Dictionary{})
	c.Assert(kerr, IsNil)
	c.Assert(db, NotNil)

	_, kerr = DB("db").Table("table").Run(this.conn)
	c.Assert(kerr.Status, Equals, http.StatusNotFound)
	// c.Assert(kerr, ErrorMatches, "Table not found")

	data := &testDoc{"foo"}
	rev, kerr := DB("db").Table("table").Insert("$", data).Run(this.conn)
	c.Assert(kerr, IsNil)
	c.Assert(rev, Not(Equals), "")

	result, kerr := DB("db").Table("table").Get("$").Run(this.conn)
	c.Assert(kerr, IsNil)
	c.Assert(result.Id(), Equals, "$")
	doc := testDoc{}
	err := result.Doc(&doc)
	c.Assert(err, IsNil)
	c.Assert(&doc, DeepEquals, data)

	kerr = DB("db").Table("table").Delete("$", rev).Run(this.conn)
	c.Assert(kerr, IsNil)

	result, kerr = DB("db").Table("table").Get("$").Run(this.conn)
	c.Assert(kerr.Status, Equals, http.StatusNotFound)
	c.Assert(kerr, ErrorMatches, "Record not found")
}

func (this *TestSuite) TestIndex(c *C) {
	db, kerr := this.conn.CreateDB("db", "mem", kissdif.Dictionary{})
	c.Assert(kerr, IsNil)
	c.Assert(db, NotNil)

	value := "Value"
	rev, kerr := DB("db").Table("table").Insert("1", value).By("name", "Joe").By("name", "Bob").Run(this.conn)
	c.Assert(kerr, IsNil)
	c.Assert(rev, Not(Equals), "")

	result, kerr := DB("db").Table("table").Get("1").Run(this.conn)
	c.Assert(kerr, IsNil)
	doc := ""
	err := result.Doc(&doc)
	c.Assert(err, IsNil)
	c.Assert(doc, Equals, value)

	result, kerr = DB("db").Table("table").By("name").Get("Joe").Run(this.conn)
	c.Assert(kerr, IsNil)
	err = result.Doc(&doc)
	c.Assert(err, IsNil)
	c.Assert(doc, Equals, value)

	result, kerr = DB("db").Table("table").By("name").Get("Bob").Run(this.conn)
	c.Assert(kerr, IsNil)
	err = result.Doc(&doc)
	c.Assert(err, IsNil)
	c.Assert(doc, Equals, value)

	resultSet, kerr := DB("db").Table("table").By("name").Run(this.conn)
	c.Assert(kerr, IsNil)
	c.Assert(resultSet.More, Equals, false)
	c.Assert(resultSet.Records, HasLen, 2)
}

func (this *TestSuite) insert(c *C, key, value string, keys kissdif.IndexMap) string {
	put := DB("db").Table("table").Insert(key, value)
	for index, list := range keys {
		for _, key := range list {
			put = put.By(index, key)
		}
	}
	rev, kerr := put.Run(this.conn)
	c.Assert(kerr, IsNil)
	c.Assert(rev, Not(Equals), "")
	return rev
}

func (this *TestSuite) TestQuery(c *C) {
	db, kerr := this.conn.CreateDB("db", "mem", kissdif.Dictionary{})
	c.Assert(kerr, IsNil)
	c.Assert(db, NotNil)

	this.insert(c, "1", "1", nil)
	this.insert(c, "2", "2", kissdif.IndexMap{"name": []string{"Alice", "Carol"}})
	this.insert(c, "3", "3", nil)

	result, kerr := DB("db").Table("table").Get("2").Run(this.conn)
	c.Assert(kerr, IsNil)
	doc := ""
	err := result.Doc(&doc)
	c.Assert(err, IsNil)
	c.Assert(doc, Equals, "2")

	rs, kerr := DB("db").Table("table").Between("3", "9").Run(this.conn)
	c.Assert(kerr, IsNil)
	c.Assert(rs.More, Equals, false)
	c.Assert(rs.Records, HasLen, 1)
	err = rs.Records[0].Doc(&doc)
	c.Assert(err, IsNil)
	c.Assert(doc, Equals, "3")

	rs, kerr = DB("db").Table("table").Between("2", "9").Run(this.conn)
	c.Assert(kerr, IsNil)
	c.Assert(rs.More, Equals, false)
	c.Assert(rs.Records, HasLen, 2)
	err = rs.Records[0].Doc(&doc)
	c.Assert(err, IsNil)
	c.Assert(doc, Equals, "2")
	err = rs.Records[1].Doc(&doc)
	c.Assert(err, IsNil)
	c.Assert(doc, Equals, "3")

	rs, kerr = DB("db").Table("table").Between("1", "3").Run(this.conn)
	c.Assert(kerr, IsNil)
	c.Assert(rs.More, Equals, false)
	c.Assert(rs.Records, HasLen, 2)
	err = rs.Records[0].Doc(&doc)
	c.Assert(err, IsNil)
	c.Assert(doc, Equals, "1")
	err = rs.Records[1].Doc(&doc)
	c.Assert(err, IsNil)
	c.Assert(doc, Equals, "2")
}

func (this *TestSuite) TestPathLikeKey(c *C) {
	db, kerr := this.conn.CreateDB("db", "mem", kissdif.Dictionary{})
	c.Assert(kerr, IsNil)
	c.Assert(db, NotNil)

	data := &testDoc{"foo"}
	rev, kerr := DB("db").Table("table").Insert("/", data).Run(this.conn)
	c.Assert(kerr, IsNil)
	c.Assert(rev, Not(Equals), "")

	result, kerr := DB("db").Table("table").Get("/").Run(this.conn)
	c.Assert(kerr, IsNil)
	c.Assert(result.Id(), Equals, "/")
	doc := testDoc{}
	err := result.Doc(&doc)
	c.Assert(err, IsNil)
	c.Assert(&doc, DeepEquals, data)

	kerr = DB("db").Table("table").Delete("/", rev).Run(this.conn)
	c.Assert(kerr, IsNil)

	result, kerr = DB("db").Table("table").Get("/").Run(this.conn)
	c.Assert(kerr.Status, Equals, http.StatusNotFound)
	c.Assert(kerr, ErrorMatches, "Record not found")
}
