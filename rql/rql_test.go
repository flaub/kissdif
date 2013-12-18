package rql

import (
	"github.com/flaub/ergo"
	"github.com/flaub/kissdif"
	_ "github.com/flaub/kissdif/driver/mem"
	"github.com/flaub/kissdif/server"
	"github.com/remogatto/prettytest"
	. "launchpad.net/gocheck"
	"net/http/httptest"
	"testing"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }

type TestSuite struct {
	prettytest.Suite
	conn Conn
}

type TestHttpSuite struct {
	TestSuite
	ts *httptest.Server
}

type TestLocalSuite struct {
	TestSuite
}

func TestRunner(t *testing.T) {
	prettytest.Run(t,
		new(TestHttpSuite),
	)
}

func (this *TestHttpSuite) Before() {
	this.ts = httptest.NewServer(server.NewServer().Server.Handler)
	var kerr *ergo.Error
	this.conn, kerr = Connect(this.ts.URL)
	this.Nil(kerr)
}

func (this *TestHttpSuite) After() {
	this.ts.Close()
}

func (this *TestLocalSuite) Before() {
	var kerr *ergo.Error
	this.conn, kerr = Connect("local://")
	this.Nil(kerr)
}

type testDoc struct {
	Value string
}

func (this *TestSuite) TestBasic() {
	table := DB("db").Table("table")
	_, kerr := table.Get("1").Exec(nil)
	this.Equal(kissdif.EBadParam, kerr.Code)

	db, kerr := this.conn.CreateDB("db", "mem", kissdif.Dictionary{})
	this.Nil(kerr)
	this.Check(db, NotNil)

	_, kerr = table.Exec(this.conn)
	this.Equal(kissdif.EBadTable, kerr.Code)

	data := &testDoc{Value: "foo"}
	rev, kerr := table.Insert("$", data).Exec(this.conn)
	this.Nil(kerr)
	this.Not(this.Equal("", rev))

	result, kerr := table.Get("$").Exec(this.conn)
	this.Nil(kerr)
	this.Equal("$", result.Id())
	doc := result.MustScan(&testDoc{}).(*testDoc)
	this.Check(doc, DeepEquals, data)

	kerr = table.Delete("$", rev).Exec(this.conn)
	this.Nil(kerr)

	result, kerr = table.Get("$").Exec(this.conn)
	this.Equal(kissdif.ENotFound, kerr.Code)
}

func (this *TestSuite) TestIndex() {
	table := DB("db").Table("table")
	db, kerr := this.conn.CreateDB("db", "mem", kissdif.Dictionary{})
	this.Nil(kerr)
	this.Check(db, NotNil)

	value := "Value"
	rev, kerr := table.Insert("1", value).By("name", "Joe").By("name", "Bob").Exec(this.conn)
	this.Nil(kerr)
	this.Not(this.Equal("", rev))

	result, kerr := table.Get("1").Exec(this.conn)
	this.Nil(kerr)
	doc := ""
	result.MustScan(&doc)
	this.Equal(value, doc)

	result, kerr = table.By("name").Get("Joe").Exec(this.conn)
	this.Nil(kerr)
	result.MustScan(&doc)
	this.Equal(value, doc)

	result, kerr = table.By("name").Get("Bob").Exec(this.conn)
	this.Nil(kerr)
	result.MustScan(&doc)
	this.Equal(value, doc)

	resultSet, kerr := table.By("name").Exec(this.conn)
	this.Nil(kerr)
	this.False(resultSet.More)
	this.Check(resultSet.Records, HasLen, 2)

	// drop index (Bob)
	rev, kerr = table.Update("1", rev, value).By("name", "Joe").Exec(this.conn)
	this.Nil(kerr)
	this.Not(this.Equal("", rev))

	result, kerr = table.By("name").Get("Joe").Exec(this.conn)
	this.Nil(kerr)
	result.MustScan(&doc)
	this.Equal(value, doc)

	// Bob should now be gone
	result, kerr = table.By("name").Get("Bob").Exec(this.conn)
	this.Not(this.Nil(kerr))

	// use alternate UpdateRecord API
	keys := make(kissdif.IndexMap)
	keys["name"] = []string{"Joe", "Bob"}
	rev, kerr = table.Insert("2", value).Keys(keys).Exec(this.conn)
	this.Nil(kerr)
	this.Not(this.Equal("", rev))

	record, kerr := table.Get("2").Exec(this.conn)
	this.Nil(kerr)
	record.MustScan(&doc)
	this.Equal(value, doc)

	record.MustSet("Other")
	rev, kerr = table.UpdateRecord(record).Exec(this.conn)
	this.Nil(kerr)
	this.Not(this.Equal("", rev))

	record, kerr = table.Get("2").Exec(this.conn)
	this.Nil(kerr)
	record.MustScan(&doc)
	this.Equal("Other", doc)
}

func (this *TestSuite) insert(key, value string, keys kissdif.IndexMap) string {
	table := DB("db").Table("table")
	put := table.Insert(key, value)
	for index, list := range keys {
		for _, key := range list {
			put = put.By(index, key)
		}
	}
	rev, kerr := put.Exec(this.conn)
	this.Nil(kerr)
	this.Not(this.Equal("", rev))
	return rev
}

func (this *TestSuite) TestQuery() {
	table := DB("db").Table("table")
	db, kerr := this.conn.CreateDB("db", "mem", kissdif.Dictionary{})
	this.Nil(kerr)
	this.Check(db, NotNil)

	this.insert("1", "1", nil)
	this.insert("2", "2", kissdif.IndexMap{"name": []string{"Alice", "Carol"}})
	this.insert("3", "3", nil)

	result, kerr := table.Get("2").Exec(this.conn)
	this.Nil(kerr)
	doc := ""
	result.MustScan(&doc)
	this.Equal("2", doc)

	rs, kerr := table.Between("3", "9").Exec(this.conn)
	this.Nil(kerr)
	this.False(rs.More)
	this.Check(rs.Records, HasLen, 1)
	rs.Records[0].MustScan(&doc)
	this.Equal("3", doc)

	rs, kerr = table.Between("2", "9").Exec(this.conn)
	this.Nil(kerr)
	this.False(rs.More)
	this.Check(rs.Records, HasLen, 2)
	rs.Records[0].MustScan(&doc)
	this.Equal("2", doc)
	rs.Records[1].MustScan(&doc)
	this.Equal("3", doc)

	rs, kerr = table.Between("1", "3").Exec(this.conn)
	this.Nil(kerr)
	this.False(rs.More)
	this.Check(rs.Records, HasLen, 2)
	rs.Records[0].MustScan(&doc)
	this.Equal("1", doc)
	rs.Records[1].MustScan(&doc)
	this.Equal("2", doc)
}

func (this *TestSuite) TestPathLikeKey() {
	table := DB("db").Table("table")
	db, kerr := this.conn.CreateDB("db", "mem", kissdif.Dictionary{})
	this.Nil(kerr)
	this.Check(db, NotNil)

	data := &testDoc{Value: "foo"}
	rev, kerr := table.Insert("/", data).Exec(this.conn)
	this.Nil(kerr)
	this.Not(this.Equal("", rev))

	result, kerr := table.Get("/").Exec(this.conn)
	this.Nil(kerr)
	this.Equal("/", result.Id())
	doc := result.MustScan(&testDoc{})
	this.Check(doc, DeepEquals, data)

	kerr = table.Delete("/", rev).Exec(this.conn)
	this.Nil(kerr)

	result, kerr = table.Get("/").Exec(this.conn)
	this.Equal(kissdif.ENotFound, kerr.Code)
}

func (this *TestSuite) TestUpdate() {
	table := DB("db").Table("table")
	db, kerr := this.conn.CreateDB("db", "mem", kissdif.Dictionary{})
	this.Nil(kerr)
	this.Check(db, NotNil)

	data := &testDoc{Value: "foo"}
	rev, kerr := table.Insert("/", data).Exec(this.conn)
	this.Nil(kerr)
	this.Not(this.Equal("", rev))

	data2 := &testDoc{Value: "bar"}
	rev2, kerr := table.Update("/", rev, data2).Exec(this.conn)
	this.Nil(kerr)
	this.Check(rev2, Not(Equals), "")
	this.Check(rev2, Not(Equals), rev)

	result, kerr := table.Get("/").Exec(this.conn)
	this.Nil(kerr)
	this.Check(result.Id(), Equals, "/")
	doc := result.MustScan(&testDoc{})
	this.Check(doc, DeepEquals, data2)
}
