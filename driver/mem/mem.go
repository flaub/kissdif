package mem

import (
	"code.google.com/p/go.text/collate"
	"code.google.com/p/go.text/language"
	"crypto/sha1"
	"fmt"
	"github.com/cznic/b"
	. "github.com/flaub/kissdif"
	"github.com/flaub/kissdif/driver"
	"io"
	"net/http"
	"sync"
)

type Driver struct {
}

type Environment struct {
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

var (
	collator *collate.Collator
)

func init() {
	driver.Register("mem", NewDriver())
	collator = collate.New(language.En_US)
}

func NewDriver() *Driver {
	return new(Driver)
}

func (this *Driver) Configure(name string, config Dictionary) (driver.Environment, *Error) {
	env := &Environment{
		name:   name,
		config: config,
		tables: make(map[string]*Table),
	}
	return env, nil
}

func (this *Environment) Name() string {
	return this.name
}

func (this *Environment) Driver() string {
	return "mem"
}

func (this *Environment) Config() Dictionary {
	return this.config
}

func (this *Environment) GetTable(name string, create bool) (driver.Table, *Error) {
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
	return collator.CompareString(a.(string), b.(string))
}

func newIndex(name string) *Index {
	return &Index{
		name: name,
		tree: b.TreeNew(cmp),
	}
}

func (this *Table) Put(newRecord *Record) (*Record, *Error) {
	this.mutex.Lock()
	defer this.mutex.Unlock()
	hasher := sha1.New()
	io.WriteString(hasher, newRecord.Doc)
	rev := fmt.Sprintf("%x", hasher.Sum(nil))
	primary := this.getIndex("_id")
	var record *Record
	value, ok := primary.tree.Get(newRecord.Id)
	if ok {
		record = value.(*Record)
		if newRecord.Rev != record.Rev {
			return nil, NewError(http.StatusConflict, "Document update conflict")
		}
		this.removeKeys(record)
		record.Doc = newRecord.Doc
		record.Keys = newRecord.Keys
	} else {
		record = newRecord
		primary.tree.Set(record.Id, record)
	}
	record.Rev = rev
	this.addKeys(record)
	return record, nil
}

func (this *Table) Delete(id string) *Error {
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
		ch <- value.(*Record)
	} else {
		node, ok := value.(*recordById)
		if !ok {
			panic("Downcast to recordById failed")
		}
		for _, v := range node.records {
			// fmt.Printf("emit: %v\n", v)
			ch <- v
		}
	}
}

func (this *Table) Get(query *Query) (chan (*Record), *Error) {
	if query.Index == "" {
		return nil, NewError(http.StatusBadRequest, "Invalid index")
	}
	if query.Limit == 0 {
		return nil, NewError(http.StatusBadRequest, "Invalid limit")
	}
	this.mutex.RLock()
	index := this.getIndex(query.Index)
	if index == nil {
		this.mutex.RUnlock()
		return nil, NewError(http.StatusNotFound, "Index not found")
	}
	var cur *b.Enumerator
	var hit bool
	if query.Lower != nil {
		cur, hit = index.tree.Seek(query.Lower.Value)
	} else {
		cur, _ = index.tree.SeekFirst()
	}
	// if cur == nil {
	// 	this.mutex.RUnlock()
	// 	return nil, NewError(http.StatusNotFound, "No records found")
	// }
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
			// fmt.Printf("Enumerating: [%d] %v %v\n", i, key, err)
			if err == io.EOF || (end != nil && key == end.key) {
				ch <- nil
				return
			}
			if hit && key == query.Lower.Value && !query.Lower.Inclusive {
				continue
			}
			emit(query, value, ch)
			count++
			if count == query.Limit {
				_, _, err := cur.Next()
				if err == io.EOF {
					ch <- nil
				}
				return
			}
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

func (this *Index) findEnd(upper *Bound) *sentinel {
	if upper == nil {
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
