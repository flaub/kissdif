package rql

import (
	"github.com/flaub/kissdif"
	_ "github.com/flaub/kissdif/driver/mem"
	. "launchpad.net/gocheck"
	"net/http"
	"testing"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }

type TestRqlSuite struct{}

func init() {
	Suite(&TestRqlSuite{})
}

func (this *TestRqlSuite) TestBasic(c *C) {
	_, err := DB("db").Table("table").Get("1").Run(nil)
	c.Assert(err.Status, Equals, http.StatusBadRequest)
	c.Assert(err, ErrorMatches, "conn must not be null")

	conn, err := Connect("local")
	c.Assert(err, IsNil)

	db, err := conn.CreateDB("db", "mem", kissdif.Dictionary{})
	c.Assert(err, IsNil)

	_, err = db.Table("table").Run(conn)
	c.Assert(err, ErrorMatches, "Table not found")

	data := struct{ Value string }{Value: "foo"}
	result, err := db.Table("table").Insert("/", data).Run(conn)
	c.Assert(err, IsNil)
	c.Assert(result, NotNil)
	c.Assert(result.Rev, Not(Equals), "")

	result, err = db.Table("table").Get("/").Run(conn)
	c.Assert(err, IsNil)
	c.Assert(result.Id, Equals, "/")
}
