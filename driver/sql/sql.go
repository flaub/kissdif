package sql

import (
	"database/sql"
	"github.com/flaub/kissdif/driver"
	"net/http"
	"sync"
)

const table_schema = `
CREATE TABLE TBL_{{.Table}}(
	_id INT NOT NULL,
	_rev INT NOT NULL,
	doc TEXT NOT NULL,
	PRIMARY KEY(_id)
);
`

const index_schema = `
CREATE TABLE IDX_{{.Table}}_{{.Index}}(
	_id INT NOT NULL,
	key {{.Type}} NOT NULL,
	PRIMARY KEY(_id, key)
);
`

const query_table = `
SELECT
	_id, _rev, doc
FROM
	TBL_{{.Table}}
WHERE
	_id = ?
LIMIT ?
`

const query_index = `
SELECT
	t._id, t._rev, t.doc
FROM
	TBL_{{.Table}} t,
	IDX_{{.Table}}_{{.Index}} i
WHERE
	t._id = i._id AND
	i.key ? ? AND
	i.key ? ?
LIMIT ?
`

type Driver struct {
	mutex sync.RWMutex
	envs  map[string]*Environment
}

type Environment struct {
	name   string
	driver string
	dsn    string
}

type Table struct {
	name string
	env  *Environment
}

func init() {
	driver.Register("sql", NewDriver())
}

func NewDriver() *Driver {
	return &Driver{
		envs: make(map[string]*Environment),
	}
}

func (this *Driver) Configure(name string, config driver.Dictionary) (driver.Environment, *driver.Error) {
	env := &Environment{
		name:   name,
		driver: config["driver"],
		dsn:    config["dsn"],
	}
	this.envs[name] = env
	return env, nil
}

func (this *Driver) Open(name string) (driver.Environment, *driver.Error) {
	env, ok := this.envs[name]
	if !ok {
		return nil, driver.NewError(http.StatusNotFound, "Environment not found")
	}
	return env, nil
}

func (this *Environment) GetTable(name string, create bool) (driver.Table, *driver.Error) {
	return &Table{name, this}, nil
}

func (this *Table) Get(query *driver.Query) (chan (*driver.Record), *driver.Error) {
	if query.Index == "" {
		return nil, driver.NewError(http.StatusBadRequest, "Invalid index")
	}
	if query.Limit == 0 {
		return nil, driver.NewError(http.StatusBadRequest, "Invalid limit")
	}
	db, err := sql.Open(this.env.driver, this.env.dsn)
	if err != nil {
		return nil, driver.NewError(http.StatusInternalServerError, err.Error())
	}
	var sql string
	var args []interface{}
	if query.Index == "_id" {
		sql = query_table
		args = []interface{}{
			"",
		}
	} else {
		sql = query_index
	}
	rows, err := db.Query(sql, args...)
	if err != nil {
		return nil, driver.NewError(http.StatusInternalServerError, err.Error())
	}
	ch := make(chan (*driver.Record))
	go func() {
		for rows.Next() {
			// err := rows.Scan(...)
		}
	}()
	return ch, nil
}

func (this *Table) Put(record *driver.Record) *driver.Error {
	return nil
}

func (this *Table) Delete(id string) *driver.Error {
	return nil
}

func (this *Table) Query() *driver.Error {
	return nil
}
