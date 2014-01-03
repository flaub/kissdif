package mem

import (
	"bytes"
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"github.com/cznic/b"
	"github.com/flaub/ergo"
	. "github.com/flaub/kissdif"
	"github.com/flaub/kissdif/driver"
	"io"
	"sync"
)

type Driver struct {
}

type Database struct {
	name   string
	config Dictionary
	tables map[string]*Table
	mutex  sync.RWMutex
}

type Index struct {
	name string
	tree *b.Tree
}

type Table struct {
	name  string
	keys  map[string]*Index
	mutex sync.RWMutex
}

type recordById struct {
	records map[string]*Record
}

type sentinel struct {
	key string
}

func init() {
	driver.Register("mem", NewDriver())
}

func NewDriver() *Driver {
	return new(Driver)
}

func (this *Driver) Configure(name string, config Dictionary) (driver.Database, *ergo.Error) {
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
	return "mem"
}

func (this *Database) Config() Dictionary {
	return this.config
}

func (this *Database) GetTable(name string, create bool) (driver.Table, *ergo.Error) {
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
			return nil, NewError(EBadTable, "name", name)
		}
		// fmt.Printf("Creating new table: %v\n", name)
		table = NewTable(name)
		this.tables[name] = table
	}
	return table, nil
}

func NewTable(name string) *Table {
	this := &Table{
		name: name,
		keys: make(map[string]*Index),
	}
	this.keys["_id"] = newIndex("_id")
	return this
}

func cmp(a, b interface{}) int {
	sa := a.(string)
	sb := b.(string)
	if sa < sb {
		return -1
	} else if sa > sb {
		return 1
	}
	return 0
}

func newIndex(name string) *Index {
	return &Index{
		name: name,
		tree: b.TreeNew(cmp),
	}
}

func (this *Table) Put(newRecord *Record) (string, *ergo.Error) {
	var buf bytes.Buffer
	err := json.NewEncoder(&buf).Encode(newRecord.Doc)
	if err != nil {
		return "", Wrap(err)
	}
	doc := buf.String()
	hasher := sha1.New()
	io.WriteString(hasher, doc)
	rev := fmt.Sprintf("%x", hasher.Sum(nil))

	this.mutex.Lock()
	defer this.mutex.Unlock()
	primary := this.getIndex("_id")
	var record *Record
	value, ok := primary.tree.Get(newRecord.Id)
	if ok {
		record = value.(*Record)
		if newRecord.Rev != record.Rev {
			return "", NewError(EConflict)
		}
		this.removeKeys(record)
		record.Doc = doc
		record.Keys = newRecord.Keys
	} else {
		record = newRecord
		record.Doc = doc
		primary.tree.Set(record.Id, record)
	}
	record.Rev = rev
	this.addKeys(record)
	return rev, nil
}

func (this *Table) Delete(id string) *ergo.Error {
	this.mutex.Lock()
	defer this.mutex.Unlock()
	primary := this.getIndex("_id")
	raw, ok := primary.tree.Get(id)
	if !ok {
		return nil
	}
	record := raw.(*Record)
	this.removeKeys(record)
	primary.tree.Delete(id)
	return nil
}

func emit(query *Query, value interface{}, ch chan<- (*Record)) {
	if query.Index == "_id" {
		emit2(ch, value.(*Record))
	} else {
		node, ok := value.(*recordById)
		if !ok {
			panic("Downcast to recordById failed")
		}
		for _, v := range node.records {
			// fmt.Printf("emit: %v\n", v)
			emit2(ch, v)
		}
	}
}

func emit2(ch chan<- (*Record), record *Record) {
	result := &Record{Id: record.Id, Rev: record.Rev, Keys: record.Keys}
	buf := bytes.NewBufferString(record.Doc.(string))
	err := json.NewDecoder(buf).Decode(&result.Doc)
	if err != nil {
		fmt.Printf("JSON decode failed: %v\n", err)
	}
	ch <- result
}

func (this *Table) Get(query *Query) (chan (*Record), *ergo.Error) {
	if query.Index == "" {
		return nil, NewError(EBadIndex, "name", query.Index)
	}
	if query.Limit == 0 {
		return nil, NewError(EBadParam, "name", "limit", "value", query.Limit)
	}
	this.mutex.RLock()
	index := this.getIndex(query.Index)
	if index == nil {
		this.mutex.RUnlock()
		return nil, NewError(EBadIndex, "name", query.Index)
	}
	var cur *b.Enumerator
	var hit bool
	if query.Lower.IsDefined() {
		cur, hit = index.tree.Seek(query.Lower.Value)
	} else {
		cur, _ = index.tree.SeekFirst()
	}
	end := index.findEnd(query.Upper)
	ch := make(chan (*Record))
	go func() {
		// fmt.Printf("Query: (%v, %v)\n", query.Lower, query.Upper)
		defer this.mutex.RUnlock()
		defer close(ch)
		if cur == nil {
			ch <- nil
			return
		}
		var count uint = 0
		for {
			key, value, err := cur.Next()
			// fmt.Printf("Enumerating: [%d] %v %v\n", count, key, err)
			if err == io.EOF || (end != nil && key == end.key) {
				// fmt.Printf("EOF\n")
				ch <- nil
				return
			}
			if hit && key == query.Lower.Value && !query.Lower.Inclusive {
				continue
			}
			if count == query.Limit {
				// fmt.Printf("Reached limit\n")
				return
			}
			emit(query, value, ch)
			count++
		}
	}()
	return ch, nil
}

func (this *Table) getIndex(name string) *Index {
	index, ok := this.keys[name]
	if !ok {
		return nil
	}
	return index
}

func (this *Index) findEnd(upper Bound) *sentinel {
	if !upper.IsDefined() {
		return nil
	}
	cursor, hit := this.tree.Seek(upper.Value)
	for {
		key, _, err := cursor.Next()
		if err == io.EOF {
			return nil
		}
		if !hit || !upper.Inclusive {
			return &sentinel{key.(string)}
		}
		if key != upper.Value {
			return &sentinel{key.(string)}
		}
	}
}

func (this *Index) add(key string, record *Record) {
	// fmt.Printf("addRecord: (%v, %v)\n", tree, key)
	var node *recordById
	raw, ok := this.tree.Get(key)
	if ok {
		node, ok = raw.(*recordById)
		if !ok {
			panic("Downcast to recordById failed")
		}
	} else {
		node = &recordById{make(map[string]*Record)}
		this.tree.Set(key, node)
	}
	node.records[record.Id] = record
}

func (this *Index) remove(key string, record *Record) {
	var node *recordById
	raw, ok := this.tree.Get(key)
	if !ok {
		return
	}
	node, ok = raw.(*recordById)
	if !ok {
		panic("Downcast to recordById failed")
	}
	delete(node.records, record.Id)
	// fmt.Printf("removeRecord: %v, %v, %v\n", key, record.Id, node)
	if len(node.records) == 0 {
		// fmt.Printf("removeRecord: Delete key: %v\n", key)
		this.tree.Delete(key)
	}
}

func (this *Table) removeKeys(record *Record) {
	for name, keys := range record.Keys {
		// fmt.Printf("removeKeys: %v, %v\n", name, keys)
		index := this.keys[name]
		for _, key := range keys {
			index.remove(key, record)
		}
	}
}

func (this *Table) addKeys(record *Record) {
	for name, keys := range record.Keys {
		// fmt.Printf("addKeys: (%v, %v)\n", name, keys)
		index, ok := this.keys[name]
		if !ok {
			index = newIndex(name)
			this.keys[name] = index
		}
		for _, key := range keys {
			index.add(key, record)
		}
	}
}
