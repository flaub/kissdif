package sql

import (
	"bytes"
	_ "code.google.com/p/go-sqlite/go1/sqlite3"
	"crypto/sha1"
	"database/sql"
	"encoding/json"
	"fmt"
	. "github.com/flaub/kissdif"
	"github.com/flaub/kissdif/driver"
	"io"
	"net/http"
	"strings"
	"sync"
	"text/template"
)

const (
	sqlSchema = `
CREATE TABLE IF NOT EXISTS T_Main_{{.T}} (
	_id INT NOT NULL,
	_rev INT NOT NULL,
	doc TEXT NOT NULL,
	PRIMARY KEY(_id)
);

CREATE TABLE IF NOT EXISTS T_Alt_{{.T}} (
	name TEXT NOT NULL,
	value TEXT NOT NULL,
	_id INT NOT NULL,
	PRIMARY KEY(name, value, _id)
);

CREATE INDEX IF NOT EXISTS I_Alt_{{.T}}_value ON T_Alt_{{.T}} (value);
CREATE INDEX IF NOT EXISTS I_Alt_{{.T}}_id ON T_Alt_{{.T}} (_id);
`
	sqlRecordQuery = `
SELECT
	_id, _rev, doc
FROM
	T_Main_{{.T}}{{.W}}
ORDER BY
	_id
LIMIT ?
`
	sqlIndexQuery = `
SELECT
	r._id, r._rev, r.doc
FROM
	T_Main_{{.T}} r
JOIN
	T_Alt_{{.T}} i
	USING(_id){{.W}}
ORDER BY
	i.value
LIMIT ?
`
	sqlRecordInsert = "INSERT INTO T_Main_{{.T}} (_id, _rev, doc) VALUES (?, ?, ?)"
	sqlRecordUpdate = `
UPDATE T_Main_{{.T}} 
SET _rev = ?, doc = ?
WHERE _id = ? AND _rev = ?
`
	sqlIndexAttach  = "INSERT INTO T_Alt_{{.T}} (_id, name, value) VALUES (?, ?, ?)"
	sqlIndexDetach  = "DELETE FROM T_Alt_{{.T}} WHERE name = ? AND value = ?"
	sqlRecordDelete = "DELETE FROM T_Main_{{.T}} WHERE _id = ?"
	sqlIndexDelete  = "DELETE FROM T_Alt_{{.T}} WHERE _id = ?"
)

type Driver struct {
}

type Database struct {
	name   string
	config Dictionary
	tables map[string]*Table
	mutex  sync.RWMutex
}

type Table struct {
	name string
	db   *Database
}

func init() {
	driver.Register("sql", NewDriver())
}

func NewDriver() *Driver {
	return new(Driver)
}

func (this *Driver) Configure(name string, config Dictionary) (driver.Database, *Error) {
	db := &Database{
		name:   name,
		config: config,
		tables: make(map[string]*Table),
	}
	return db, nil
}

func (this *Database) Name() string {
	return this.name
}

func (this *Database) Driver() string {
	return "sql"
}

func (this *Database) Config() Dictionary {
	return this.config
}

func (this *Database) GetTable(name string, create bool) (driver.Table, *Error) {
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
			return nil, NewError(http.StatusNotFound, "Table not found")
		}
		// fmt.Printf("Creating new table: %v\n", name)
		var err *Error
		table, err = this.NewTable(name)
		if err != nil {
			return nil, err
		}
		this.tables[name] = table
	}
	return table, nil
}

func (this *Database) NewTable(name string) (*Table, *Error) {
	db, err := sql.Open("sqlite3", this.config["dsn"])
	if err != nil {
		return nil, NewError(http.StatusInternalServerError, err.Error())
	}
	defer db.Close()
	_, err = db.Exec(compile(sqlSchema, name, ""))
	if err != nil {
		return nil, NewError(http.StatusInternalServerError, err.Error())
	}
	return &Table{name, this}, nil
}

func compile(text, table, where string) string {
	var buf bytes.Buffer
	tmpl := template.Must(template.New("").Parse(text))
	err := tmpl.Execute(&buf, struct{ T, W string }{table, where})
	if err != nil {
		panic(err)
	}
	return buf.String()
}

func (this *Table) where(query *Query) (string, []interface{}) {
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

func (this *Table) prepareQuery(query *Query) (string, []interface{}) {
	where, args := this.where(query)
	args = append(args, query.Limit+1)
	var text string
	if query.Index == "_id" {
		text = sqlRecordQuery
	} else {
		text = sqlIndexQuery
	}
	return compile(text, this.name, where), args
}

func (this *Table) Get(query *Query) (chan (*Record), *Error) {
	if query.Index == "" {
		return nil, NewError(http.StatusBadRequest, "Invalid index")
	}
	if query.Limit == 0 {
		return nil, NewError(http.StatusBadRequest, "Invalid limit")
	}
	db, err := sql.Open("sqlite3", this.db.config["dsn"])
	if err != nil {
		return nil, NewError(http.StatusInternalServerError, err.Error())
	}
	stmt, args := this.prepareQuery(query)
	rows, err := db.Query(stmt, args...)
	if err != nil {
		db.Close()
		return nil, NewError(http.StatusInternalServerError, err.Error())
	}
	ch := make(chan (*Record))
	go func() {
		defer db.Close()
		defer rows.Close()
		defer close(ch)
		var count uint
		for rows.Next() {
			var record Record
			var doc string
			err := rows.Scan(&record.Id, &record.Rev, &doc)
			if err != nil {
				fmt.Printf("Scan failed: %v\n", err)
				return
			}
			buf := bytes.NewBufferString(doc)
			err = json.NewDecoder(buf).Decode(&record.Doc)
			if err != nil {
				fmt.Printf("JSON decode failed: %v\n", err)
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

type referee struct {
	ok bool
	tx *sql.Tx
}

func (this *referee) Close() {
	if this.ok {
		this.tx.Commit()
	} else {
		this.tx.Rollback()
	}
}

func (this *Table) Put(record *Record) (string, *Error) {
	var buf bytes.Buffer
	err := json.NewEncoder(&buf).Encode(record.Doc)
	if err != nil {
		return "", NewError(http.StatusInternalServerError, err.Error())
	}
	doc := buf.String()
	hasher := sha1.New()
	io.WriteString(hasher, doc)
	rev := fmt.Sprintf("%x", hasher.Sum(nil))
	db, err := sql.Open("sqlite3", this.db.config["dsn"])
	if err != nil {
		return "", NewError(http.StatusInternalServerError, err.Error())
	}
	defer db.Close()
	tx, err := db.Begin()
	if err != nil {
		return "", NewError(http.StatusInternalServerError, err.Error())
	}
	ref := referee{tx: tx}
	defer ref.Close()
	if record.Rev == "" {
		_, err = tx.Exec(compile(sqlRecordInsert, this.name, ""), record.Id, rev, doc)
		if err != nil {
			return "", NewError(http.StatusConflict, err.Error())
		}
	} else {
		result, err := tx.Exec(compile(sqlRecordUpdate, this.name, ""),
			rev, doc, record.Id, record.Rev)
		rows, err := result.RowsAffected()
		if err != nil || rows != 1 {
			return "", NewError(http.StatusConflict, "Document update conflict")
		}
	}
	_, err = tx.Exec(compile(sqlIndexDelete, this.name, ""), record.Id)
	if err != nil {
		return "", NewError(http.StatusInternalServerError, err.Error())
	}
	for name, keys := range record.Keys {
		for _, key := range keys {
			_, err = tx.Exec(compile(sqlIndexAttach, this.name, ""), record.Id, name, key)
			if err != nil {
				return "", NewError(http.StatusInternalServerError, err.Error())
			}
		}
	}
	ref.ok = true
	record.Rev = rev
	return rev, nil
}

func (this *Table) Delete(id string) *Error {
	db, err := sql.Open("sqlite3", this.db.config["dsn"])
	if err != nil {
		return NewError(http.StatusInternalServerError, err.Error())
	}
	defer db.Close()
	tx, err := db.Begin()
	if err != nil {
		return NewError(http.StatusInternalServerError, err.Error())
	}
	ref := referee{tx: tx}
	defer ref.Close()
	_, err = tx.Exec(compile(sqlIndexDelete, this.name, ""), id)
	if err != nil {
		return NewError(http.StatusInternalServerError, err.Error())
	}
	_, err = tx.Exec(compile(sqlRecordDelete, this.name, ""), id)
	if err != nil {
		return NewError(http.StatusInternalServerError, err.Error())
	}
	ref.ok = true
	return nil
}
