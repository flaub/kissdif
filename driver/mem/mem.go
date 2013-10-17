package mem

import (
	"code.google.com/p/go.text/collate"
	"code.google.com/p/go.text/language"
	"fmt"
	"github.com/cznic/b"
	"github.com/flaub/kissdif/driver"
	"io"
	"net/http"
	"sync"
)

type Driver struct {
	stores map[string]*Store
	mutex  sync.RWMutex
}

type Store struct {
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

type altNode struct {
	records map[string]*driver.Record
}

var (
	collator *collate.Collator
)

func init() {
	driver.Register("mem", NewDriver())
	collator = collate.New(language.En_US)
}

func NewDriver() *Driver {
	return &Driver{
		stores: make(map[string]*Store),
	}
}

func (this *Driver) Configure(name string, config driver.Dictionary) (driver.Store, *driver.Error) {
	store := &Store{
		tables: make(map[string]*Table),
	}
	this.stores[name] = store
	return store, nil
}

func (this *Driver) Open(name string) (driver.Store, *driver.Error) {
	store, ok := this.stores[name]
	if !ok {
		return nil, driver.NewError(http.StatusNotFound, "Store not found")
	}
	return store, nil
}

func (this *Store) GetTable(name string, create bool) (driver.Table, *driver.Error) {
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
		fmt.Printf("Creating new table: %v\n", name)
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
	this.keys["_id"] = NewIndex("_id")
	return this
}

func cmp(a, b interface{}) int {
	return collator.CompareString(a.(string), b.(string))
}

func NewIndex(name string) *Index {
	return &Index{
		name: name,
		tree: b.TreeNew(cmp),
	}
}

func (this *Table) Put(newRecord *driver.Record) *driver.Error {
	this.mutex.Lock()
	defer this.mutex.Unlock()
	primary := this.getIndex("_id")
	var record *driver.Record
	value, ok := primary.tree.Get(newRecord.Id)
	if ok {
		record = value.(*driver.Record)
		if newRecord.Rev != "" && newRecord.Rev != record.Rev {
			return driver.NewError(http.StatusConflict, "Document update conflict")
		}
		this.removeKeys(record)
		record.Rev = newRecord.Rev
		record.Doc = newRecord.Doc
	} else {
		record = newRecord
		primary.tree.Set(record.Id, record)
	}
	this.addKeys(record)
	return nil
}

func (this *Table) Delete(id string) *driver.Error {
	this.mutex.Lock()
	defer this.mutex.Unlock()
	primary := this.getIndex("_id")
	raw, ok := primary.tree.Get(id)
	if !ok {
		return nil
	}
	record := raw.(*driver.Record)
	this.removeKeys(record)
	primary.tree.Delete(id)
	return nil
}

type sentinel struct {
	key string
}

func findEnd(tree *b.Tree, upper *driver.Bound) *sentinel {
	if upper == nil {
		return nil
	}
	cursor, hit := tree.Seek(upper.Value)
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

func (this *Table) Get(query *driver.Query) (chan (*driver.Record), *driver.Error) {
	if query.Index == "" {
		return nil, driver.NewError(http.StatusBadRequest, "Invalid index")
	}
	if query.Limit == 0 {
		return nil, driver.NewError(http.StatusBadRequest, "Invalid limit")
	}
	this.mutex.RLock()
	index := this.getIndex(query.Index)
	if index == nil {
		this.mutex.RUnlock()
		return nil, driver.NewError(http.StatusNotFound, "Index not found")
	}
	var cur *b.Enumerator
	var hit bool
	if query.Lower != nil {
		cur, hit = index.tree.Seek(query.Lower.Value)
	} else {
		cur, _ = index.tree.SeekFirst()
	}
	if cur == nil {
		this.mutex.RUnlock()
		return nil, driver.NewError(http.StatusNotFound, "No records found")
	}
	end := findEnd(index.tree, query.Upper)
	ch := make(chan (*driver.Record))
	go func() {
		count := 0
		emit := func(value interface{}) bool {
			if query.Index == "_id" {
				ch <- value.(*driver.Record)
			} else {
				node := value.(*altNode)
				for _, v := range node.records {
					ch <- v
				}
			}
			count++
			return count < query.Limit
		}
		// fmt.Printf("Query: (%v, %v)\n", query.Lower, query.Upper)
		defer this.mutex.RUnlock()
		defer close(ch)
		if cur == nil {
			ch <- nil
			return
		}
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
			if !emit(value) {
				_, _, err := cur.Next()
				if err == io.EOF {
					ch <- nil
					return
				}
				break
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

func newAltNode() *altNode {
	return &altNode{make(map[string]*driver.Record)}
}

func addRecord(tree *b.Tree, key string, record *driver.Record) {
	var node *altNode
	raw, ok := tree.Get(key)
	if ok {
		node = raw.(*altNode)
	} else {
		node = newAltNode()
	}
	node.records[record.Id] = record
}

func removeRecord(tree *b.Tree, key string, record *driver.Record) {
	var node *altNode
	raw, ok := tree.Get(key)
	if !ok {
		return
	}
	node = raw.(*altNode)
	delete(node.records, record.Id)
	if len(node.records) == 0 {
		tree.Delete(key)
	}
}

func (this *Table) removeKeys(record *driver.Record) {
	for name, keys := range record.Keys {
		index := this.keys[name]
		for _, key := range keys {
			removeRecord(index.tree, key, record)
		}
	}
}

func (this *Table) addKeys(record *driver.Record) {
	for name, keys := range record.Keys {
		index, ok := this.keys[name]
		if !ok {
			index = NewIndex(name)
			this.keys[name] = index
		}
		for _, key := range keys {
			addRecord(index.tree, key, record)
		}
	}
}
