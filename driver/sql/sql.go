package sql

import (
	_ "code.google.com/p/go-sqlite/go1/sqlite3"
	"database/sql"
	"fmt"
	"github.com/flaub/kissdif/driver"
	"net/http"
	"strings"
	"sync"
)

const (
	sqlRecordSchema = `
CREATE TABLE IF NOT EXISTS R_%v (
	_id INT NOT NULL,
	_rev INT NOT NULL,
	doc TEXT NOT NULL,
	PRIMARY KEY(_id)
)
`
	sqlIndexSchema = `
CREATE TABLE IF NOT EXISTS I_%v (
	name TEXT NOT NULL,
	value TEXT NOT NULL,
	_id INT NOT NULL,
	PRIMARY KEY(name, value, _id)
)
`
	sqlRecordQuery = `
SELECT
	_id, _rev, doc
FROM
	R_%v%v
ORDER BY
	_id
LIMIT ?
`
	sqlIndexQuery = `
SELECT
	r._id, r._rev, r.doc
FROM
	R_%v r
JOIN
	I_%v i
	USING(_id)%v
ORDER BY
	i.value
LIMIT ?
`
	sqlRecordReplace = "REPLACE INTO R_%v (_id, _rev, doc) VALUES (?, ?, ?)"
	sqlIndexAttach   = "INSERT INTO I_%v (_id, name, value) VALUES (?, ?, ?)"
	sqlIndexDetach   = "DELETE FROM I_%v WHERE name = ? AND value = ?"
	sqlRecordDelete  = "DELETE FROM R_%v WHERE _id = ?"
	sqlIndexDelete   = "DELETE FROM I_%v WHERE _id = ?" // FIXME: this results in a table scan
)

type Driver struct {
	mutex sync.RWMutex
	envs  map[string]*Environment
}

type Environment struct {
	driver *Driver
	name   string
	config driver.Dictionary
	tables map[string]*Table
	mutex  sync.RWMutex
}

type Table struct {
	name string
	env  *Environment
}

type readyStmt struct {
	db   *sql.DB
	stmt *sql.Stmt
	args []interface{}
}

type session struct {
	db    *sql.DB
	table string
	queue []*readyStmt
}

func init() {
	driver.Register("sql", NewDriver())
}

func NewDriver() *Driver {
	this := &Driver{
		envs: make(map[string]*Environment),
	}
	return this
}

func (this *Driver) Configure(name string, config driver.Dictionary) (driver.Environment, *driver.Error) {
	// fmt.Printf("Configuring %q with %v\n", name, config)
	env := &Environment{
		name:   name,
		config: config,
		driver: this,
		tables: make(map[string]*Table),
	}
	this.mutex.Lock()
	defer this.mutex.Unlock()
	this.envs[name] = env
	return env, nil
}

func (this *Driver) Open(name string) (driver.Environment, *driver.Error) {
	this.mutex.RLock()
	defer this.mutex.RUnlock()
	env, ok := this.envs[name]
	if !ok {
		return nil, driver.NewError(http.StatusNotFound, "Environment not found")
	}
	return env, nil
}

func (this *readyStmt) close() error {
	err := this.stmt.Close()
	if err != nil {
		return err
	}
	return this.db.Close()
}

func (this *Environment) GetTable(name string, create bool) (driver.Table, *driver.Error) {
	if create {
		this.mutex.Lock()
		defer this.mutex.Unlock()
	} else {
		this.mutex.RLock()
		defer this.mutex.RUnlock()
	}
	table, ok := this.tables[name]
	if !ok {
		if !create {
			return nil, driver.NewError(http.StatusNotFound, "Table not found")
		}
		// fmt.Printf("Creating new table: %v\n", name)
		var err *driver.Error
		table, err = this.NewTable(name)
		if err != nil {
			return nil, err
		}
		this.tables[name] = table
	}
	return table, nil
}

func (this *Environment) NewTable(name string) (*Table, *driver.Error) {
	session, err := this.newSession(name)
	if err != nil {
		return nil, err
	}
	defer session.close()
	err = session.add(sqlRecordSchema)
	if err != nil {
		return nil, err
	}
	err = session.add(sqlIndexSchema)
	if err != nil {
		return nil, err
	}
	err = session.exec()
	if err != nil {
		return nil, err
	}
	return &Table{name, this}, nil
}

func (this *Environment) newSession(table string) (*session, *driver.Error) {
	db, err := sql.Open("sqlite3", this.config["dsn"])
	if err != nil {
		return nil, driver.NewError(http.StatusInternalServerError, err.Error())
	}
	return &session{db: db, table: table}, nil
}

func (this *session) close() {
	for _, stmt := range this.queue {
		stmt.close()
	}
	this.queue = []*readyStmt{}
	this.db.Close()
}

func (this *session) add(format string, args ...interface{}) *driver.Error {
	sql := fmt.Sprintf(format, this.table)
	// fmt.Printf("Preparing sql: %v\n", sql)
	stmt, err := this.db.Prepare(sql)
	if err != nil {
		return driver.NewError(http.StatusInternalServerError, err.Error())
	}
	ready := &readyStmt{this.db, stmt, args}
	this.queue = append(this.queue, ready)
	return nil
}

func (this *session) exec() *driver.Error {
	tx, err := this.db.Begin()
	if err != nil {
		return driver.NewError(http.StatusInternalServerError, err.Error())
	}
	for _, stmt := range this.queue {
		defer stmt.close()
		if err == nil {
			// fmt.Printf("Executing sql: %v\n", stmt.stmt)
			tx.Stmt(stmt.stmt).Exec(stmt.args...)
		}
	}
	if err != nil {
		tx.Rollback()
		return driver.NewError(http.StatusInternalServerError, err.Error())
	}
	tx.Commit()
	this.queue = []*readyStmt{}
	return nil
}

func (this *session) prepare(format string, args ...interface{}) (*readyStmt, *driver.Error) {
	sql := fmt.Sprintf(format, args...)
	// fmt.Printf("Preparing sql: %s\n", sql)
	stmt, err := this.db.Prepare(sql)
	if err != nil {
		return nil, driver.NewError(http.StatusInternalServerError, err.Error())
	}
	return &readyStmt{this.db, stmt, nil}, nil
}

func (this *Table) where(query *driver.Query) (string, []interface{}) {
	// fmt.Printf("Where: (%v, %v)\n", query.Lower, query.Upper)
	exprs := []string{}
	var args []interface{}
	var selector string
	if query.Index == "_id" {
		selector = query.Index
	} else {
		exprs = append(exprs, "i.name = ?")
		args = append(args, query.Index)
		selector = "i.value"
	}
	if query.Lower != nil && query.Upper != nil &&
		query.Lower.Value == query.Upper.Value {
		exprs = append(exprs, selector+" = ?")
		args = append(args, query.Lower.Value)
	} else {
		if query.Lower != nil {
			if query.Lower.Inclusive {
				exprs = append(exprs, selector+" >= ?")
			} else {
				exprs = append(exprs, selector+" > ?")
			}
			args = append(args, query.Lower.Value)
		}
		if query.Upper != nil {
			if query.Upper.Inclusive {
				exprs = append(exprs, selector+" <= ?")
			} else {
				exprs = append(exprs, selector+" < ?")
			}
			args = append(args, query.Upper.Value)
		}
	}
	if len(exprs) == 0 {
		return "", args
	}
	return "\nWHERE " + strings.Join(exprs, " AND "), args
}

func (this *Table) prepareQuery(query *driver.Query) (*readyStmt, *driver.Error) {
	session, err := this.env.newSession(this.name)
	if err != nil {
		return nil, err
	}

	where, args := this.where(query)
	args = append(args, query.Limit+1)

	var format string
	var fmtArgs []interface{}
	if query.Index == "_id" {
		format = sqlRecordQuery
		fmtArgs = []interface{}{this.name, where}
	} else {
		format = sqlIndexQuery
		fmtArgs = []interface{}{this.name, this.name, where}
	}
	stmt, err := session.prepare(format, fmtArgs...)
	if err != nil {
		return nil, err
	}
	stmt.args = args
	return stmt, nil
}

func (this *Table) Get(query *driver.Query) (chan (*driver.Record), *driver.Error) {
	if query.Index == "" {
		return nil, driver.NewError(http.StatusBadRequest, "Invalid index")
	}
	if query.Limit == 0 {
		return nil, driver.NewError(http.StatusBadRequest, "Invalid limit")
	}
	ready, err := this.prepareQuery(query)
	if err != nil {
		return nil, err
	}
	rows, err2 := ready.stmt.Query(ready.args...)
	if err2 != nil {
		return nil, driver.NewError(http.StatusInternalServerError, err2.Error())
	}
	ch := make(chan (*driver.Record))
	go func() {
		defer ready.close()
		defer rows.Close()
		defer close(ch)
		count := 0
		for rows.Next() {
			var record driver.Record
			err := rows.Scan(&record.Id, &record.Rev, &record.Doc)
			if err != nil {
				fmt.Printf("Scan failed: %v\n", err)
				return
			}
			if count == query.Limit {
				return
			}
			ch <- &record
			count++
		}
		ch <- nil
	}()
	return ch, nil
}

func (this *Table) Put(record *driver.Record) *driver.Error {
	session, err := this.env.newSession(this.name)
	if err != nil {
		return err
	}
	defer session.close()
	// FIXME: this results in a table scan
	err = session.add(sqlIndexDelete, record.Id)
	if err != nil {
		return err
	}
	err = session.add(sqlRecordReplace, record.Id, record.Rev, record.Doc)
	if err != nil {
		return err
	}
	for name, keys := range record.Keys {
		for _, key := range keys {
			err = session.add(sqlIndexAttach, record.Id, name, key)
			if err != nil {
				return err
			}
		}
	}
	err = session.exec()
	if err != nil {
		return err
	}
	return nil
}

func (this *Table) Delete(id string) *driver.Error {
	return driver.NewError(http.StatusNotImplemented, "Not implemented")
}

func (this *Table) Query() *driver.Error {
	return driver.NewError(http.StatusNotImplemented, "Not implemented")
}
