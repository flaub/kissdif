package rql

import (
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
	var err error
	this.conn, err = Connect(this.ts.URL)
	c.Check(err, IsNil)
}

func (this *TestHttpSuite) TearDownTest(c *C) {
	this.ts.Close()
}

func (this *TestLocalSuite) SetUpTest(c *C) {
	var err error
	this.conn, err = Connect("local://")
	c.Check(err, IsNil)
}

type testDoc struct {
	Value string
}

func (this *TestSuite) TestBasic(c *C) {
	table := DB("db").Table("table")
	_, err := table.Get("1").Exec(nil)
	c.Check(kissdif.IsError(err, kissdif.EBadParam), Equals, true)

	db, err := this.conn.CreateDB("db", "mem", kissdif.Dictionary{})
	c.Check(err, IsNil)
	c.Check(db, NotNil)

	_, err = table.Exec(this.conn)
	c.Check(kissdif.IsError(err, kissdif.EBadTable), Equals, true)

	data := &testDoc{Value: "foo"}
	rev, err := table.Insert("$", data).Exec(this.conn)
	c.Check(err, IsNil)
	c.Check(rev, Not(Equals), "")

	result, err := table.Get("$").Exec(this.conn)
	c.Check(err, IsNil)
	c.Check(result.Id(), Equals, "$")
	doc := result.MustScan(&testDoc{}).(*testDoc)
	c.Check(doc, DeepEquals, data)

	err = table.Delete("$", rev).Exec(this.conn)
	c.Check(err, IsNil)

	result, err = table.Get("$").Exec(this.conn)
	c.Check(kissdif.IsError(err, kissdif.ENotFound), Equals, true)
}

func (this *TestSuite) TestIndex(c *C) {
	table := DB("db").Table("table")
	db, err := this.conn.CreateDB("db", "mem", kissdif.Dictionary{})
	c.Check(err, IsNil)
	c.Check(db, NotNil)

	value := "Value"
	rev, err := table.Insert("1", value).By("name", "Joe").By("name", "Bob").Exec(this.conn)
	c.Check(err, IsNil)
	c.Check(rev, Not(Equals), "")

	result, err := table.Get("1").Exec(this.conn)
	c.Check(err, IsNil)
	doc := ""
	result.MustScan(&doc)
	c.Check(doc, Equals, value)

	result, err = table.By("name").Get("Joe").Exec(this.conn)
	c.Check(err, IsNil)
	result.MustScan(&doc)
	c.Check(doc, Equals, value)

	result, err = table.By("name").Get("Bob").Exec(this.conn)
	c.Check(err, IsNil)
	result.MustScan(&doc)
	c.Check(doc, Equals, value)

	resultSet, err := table.By("name").Exec(this.conn)
	c.Check(err, IsNil)
	c.Check(resultSet.More(), Equals, false)
	c.Check(resultSet.Count(), Equals, 2)

	// drop index (Bob)
	rev, err = table.Update("1", rev, value).By("name", "Joe").Exec(this.conn)
	c.Check(err, IsNil)
	c.Check(rev, Not(Equals), "")

	result, err = table.By("name").Get("Joe").Exec(this.conn)
	c.Check(err, IsNil)
	result.MustScan(&doc)
	c.Check(doc, Equals, value)

	// Bob should now be gone
	result, err = table.By("name").Get("Bob").Exec(this.conn)
	c.Check(err, NotNil)

	// use alternate UpdateRecord API
	keys := make(kissdif.IndexMap)
	keys["name"] = []string{"Joe", "Bob"}
	rev, err = table.Insert("2", value).Keys(keys).Exec(this.conn)
	c.Check(err, IsNil)
	c.Check(rev, Not(Equals), "")

	record, err := table.Get("2").Exec(this.conn)
	c.Check(err, IsNil)
	record.MustScan(&doc)
	c.Check(doc, Equals, value)

	record.MustSet("Other")
	rev, err = table.UpdateRecord(record).Exec(this.conn)
	c.Check(err, IsNil)
	c.Check(rev, Not(Equals), "")

	record, err = table.Get("2").Exec(this.conn)
	c.Check(err, IsNil)
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
	rev, err := put.Exec(this.conn)
	c.Check(err, IsNil)
	c.Check(rev, Not(Equals), "")
	return rev
}

func (this *TestSuite) TestQuery(c *C) {
	table := DB("db").Table("table")
	db, err := this.conn.CreateDB("db", "mem", kissdif.Dictionary{})
	c.Check(err, IsNil)
	c.Check(db, NotNil)

	this.insert(c, "1", "1", nil)
	this.insert(c, "2", "2", kissdif.IndexMap{"name": []string{"Alice", "Carol"}})
	this.insert(c, "3", "3", nil)

	result, err := table.Get("2").Exec(this.conn)
	c.Check(err, IsNil)
	doc := ""
	result.MustScan(&doc)
	c.Check(doc, Equals, "2")

	rs, err := table.Between("3", "9").Exec(this.conn)
	c.Check(err, IsNil)
	c.Check(rs.More(), Equals, false)
	c.Check(rs.Count(), Equals, 1)
	reader := rs.Reader()
	c.Check(reader.Next(), Equals, true)
	reader.MustScan(&doc)
	c.Check(doc, Equals, "3")
	c.Check(reader.Next(), Equals, false)

	rs, err = table.Between("2", "9").Exec(this.conn)
	c.Check(err, IsNil)
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

	rs, err = table.Between("1", "3").Exec(this.conn)
	c.Check(err, IsNil)
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
	db, err := this.conn.CreateDB("db", "mem", kissdif.Dictionary{})
	c.Check(err, IsNil)
	c.Check(db, NotNil)

	data := &testDoc{Value: "foo"}
	rev, err := table.Insert("/", data).Exec(this.conn)
	c.Check(err, IsNil)
	c.Check(rev, Not(Equals), "")

	result, err := table.Get("/").Exec(this.conn)
	c.Check(err, IsNil)
	c.Check(result.Id(), Equals, "/")
	doc := result.MustScan(&testDoc{})
	c.Check(doc, DeepEquals, data)

	err = table.Delete("/", rev).Exec(this.conn)
	c.Check(err, IsNil)

	result, err = table.Get("/").Exec(this.conn)
	c.Check(kissdif.IsError(err, kissdif.ENotFound), Equals, true)
}

func (this *TestSuite) TestUpdate(c *C) {
	table := DB("db").Table("table")
	db, err := this.conn.CreateDB("db", "mem", kissdif.Dictionary{})
	c.Check(err, IsNil)
	c.Check(db, NotNil)

	data := &testDoc{Value: "foo"}
	rev, err := table.Insert("/", data).Exec(this.conn)
	c.Check(err, IsNil)
	c.Check(rev, Not(Equals), "")

	data2 := &testDoc{Value: "bar"}
	rev2, err := table.Update("/", rev, data2).Exec(this.conn)
	c.Check(err, IsNil)
	c.Check(rev2, Not(Equals), "")
	c.Check(rev2, Not(Equals), rev)

	result, err := table.Get("/").Exec(this.conn)
	c.Check(err, IsNil)
	c.Check(result.Id(), Equals, "/")
	doc := result.MustScan(&testDoc{})
	c.Check(doc, DeepEquals, data2)
}

func (this *TestSuite) TestUpdateRecord(c *C) {
	table := DB("db").Table("table")
	db, err := this.conn.CreateDB("db", "mem", kissdif.Dictionary{})
	c.Check(err, IsNil)
	c.Check(db, NotNil)

	data := &testDoc{Value: "foo"}
	rev, err := table.Insert("/", data).Exec(this.conn)
	c.Check(err, IsNil)
	c.Check(rev, Not(Equals), "")

	record, err := table.Get("/").Exec(this.conn)
	c.Check(err, IsNil)
	c.Check(record, NotNil)
	var doc testDoc
	record.MustScan(&doc)
	doc.Value = "bar"
	record.MustSet(doc)
	rev2, err := table.UpdateRecord(record).Exec(this.conn)
	c.Check(err, IsNil)
	c.Check(rev2, Not(Equals), "")
	c.Check(rev2, Not(Equals), rev)

	result, err := table.Get("/").Exec(this.conn)
	c.Check(err, IsNil)
	c.Check(result.Id(), Equals, "/")
	var doc2 testDoc
	result.MustScan(&doc2)
	c.Check(doc, DeepEquals, doc2)
}

func (this *TestSuite) TestUpdateKeys(c *C) {
	const id = "548f18c364563464e6952076108c39ba"
	const key1 = "by_session"
	const key2 = "by_path"
	const kv1 = "08b678e5-1df0-4380-80f5-69d90d64753a"
	const kv2 = "/used.txt"

	table := DB("db").Table("table")
	db, err := this.conn.CreateDB("db", "mem", kissdif.Dictionary{})
	c.Check(err, IsNil)
	c.Check(db, NotNil)

	data := &testDoc{Value: "foo"}
	rev, err := table.Insert(id, data).By(key1, kv1).Exec(this.conn)
	c.Check(err, IsNil)
	c.Check(rev, Not(Equals), "")

	record, err := table.Get(id).Exec(this.conn)
	c.Check(err, IsNil)
	c.Check(record, NotNil)
	c.Check(record.Keys()[key1], DeepEquals, []string{kv1})

	// record.Keys().Drop(key1, "1")
	// record.Keys().Add(key1, "2")
	record.Keys().Add(key2, kv2)

	rev2, err := table.UpdateRecord(record).Exec(this.conn)
	c.Check(err, IsNil)
	c.Check(rev2, Not(Equals), "")
	c.Check(rev2, Not(Equals), rev)

	result, err := table.Get(id).Exec(this.conn)
	c.Check(err, IsNil)
	c.Check(result.Id(), Equals, id)
	c.Check(record.Keys()[key1], DeepEquals, []string{kv1})
	c.Check(record.Keys()[key2], DeepEquals, []string{kv2})
}
