package sql

import (
	"database/sql"
	. "github.com/flaub/kissdif"
	"github.com/flaub/kissdif/driver/test"
	"io/ioutil"
	. "launchpad.net/gocheck"
	"os"
	"testing"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }

type TestSuite struct {
	path string
}

type TestDriver struct {
	*test.TestSuite
}

func init() {
	Suite(&TestSuite{})
	Suite(&TestDriver{TestSuite: test.NewTestSuite("sql")})
}

func (this *TestDriver) SetUpTest(c *C) {
	this.Config = make(Dictionary)
	this.Config["dsn"] = getTemp(c) + ".db"
	this.TestSuite.SetUpTest(c)
}

func (this *TestDriver) TearDownTest(c *C) {
	path := this.Config["dsn"]
	c.Logf("Removing %q", path)
	os.Remove(path)
}

func getTemp(c *C) string {
	ftmp, err := ioutil.TempFile("", "")
	c.Assert(err, IsNil)
	defer ftmp.Close()
	return ftmp.Name()
}

func (this *TestSuite) explain(c *C, db *sql.DB, query string, args ...interface{}) {
	sql := "EXPLAIN QUERY PLAN " + query
	rows, err := db.Query(sql, args...)
	c.Assert(err, IsNil)
	for rows.Next() {
		var id, order, from int
		var detail string
		err := rows.Scan(&id, &order, &from, &detail)
		c.Assert(err, IsNil)
		c.Check(detail, Matches, "^SEARCH.*", Commentf("\tQuery: %v\n\tPlan: %v", query, detail))
	}
}

func (this *TestSuite) TestQueryOptimizer(c *C) {
	db, err := sql.Open("sqlite3", this.path)
	c.Assert(err, IsNil)
	defer db.Close()
	table := "table"
	_, err = db.Exec(compile(sqlSchema, table, ""))
	c.Assert(err, IsNil)

	// this.explain(c, db, compile(sqlRecordQuery, table, ""), 10)
	this.explain(c, db, compile(sqlRecordQuery, table, "\nWHERE _id = ?"), 10, "")
	this.explain(c, db, compile(sqlRecordQuery, table, "\nWHERE _id < ?"), 10, "")
	this.explain(c, db, compile(sqlRecordQuery, table, "\nWHERE _id > ?"), 10, "")
	this.explain(c, db, compile(sqlRecordQuery, table, "\nWHERE _id > ? AND _id < ?"), 10, "", "")

	// this.explain(c, db, compile(sqlIndexQuery, table, ""), 10)
	this.explain(c, db, compile(sqlIndexQuery, table, "\nWHERE i.name = ? AND i.value = ?"), 10, "", "")
	this.explain(c, db, compile(sqlIndexQuery, table, "\nWHERE i.name = ? AND i.value > ?"), 10, "", "")
	this.explain(c, db, compile(sqlIndexQuery, table, "\nWHERE i.name = ? AND i.value > ? AND i.value < ?"), 10, "", "", "")

	this.explain(c, db, compile(sqlRecordUpdate, table, ""), "", "", "", "")
	this.explain(c, db, compile(sqlRecordDelete, table, ""), "")

	this.explain(c, db, compile(sqlIndexDelete, table, ""), "")
	this.explain(c, db, compile(sqlIndexDetach, table, ""), "", "")
}

func (this *TestSuite) SetUpTest(c *C) {
	this.path = getTemp(c) + ".db"
}

func (this *TestSuite) TearDownTest(c *C) {
	os.Remove(this.path)
}
