package mem

import (
	"github.com/flaub/kissdif/driver"
	"net/http"
	"testing"
)

func eq(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}

func expect(t *testing.T, table driver.Table, query *driver.Query, expectedEof bool, expected ...string) {
	ch, err := table.Get(query)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	actual := []string{}
	eof := false
	for record := range ch {
		if record == nil {
			eof = true
		} else {
			actual = append(actual, string(record.Doc))
		}
	}
	if !eq(actual, expected) {
		t.Fatalf("Mismatch. query: (%v, %v) expected: %v actual: %v",
			query.Lower, query.Upper, expected, actual)
	}
	if eof != expectedEof {
		t.Fatalf("EOF Mismatch. query: (%v, %v) expected: %v, actual: %v",
			query.Lower, query.Upper, expectedEof, eof)
	}
}

func put(t *testing.T, table driver.Table, values ...string) {
	for _, value := range values {
		record := &driver.Record{Id: value, Doc: []byte(value)}
		err := table.Put(record)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}
}

func TestBasic(t *testing.T) {
	mem := NewDriver()
	table, err := mem.GetTable("table", true)
	if err != nil {
		t.Fatalf("GetTable failed: %v", err)
	}

	query := &driver.Query{}
	_, err = table.Get(query)
	if err == nil || err.Status != http.StatusBadRequest {
		t.Fatalf("Get should fail without index specified: %v", err)
	}

	query.Index = "_id"
	_, err = table.Get(query)
	if err == nil || err.Status != http.StatusBadRequest || err.Message != "Invalid limit" {
		t.Fatalf("Get should fail with invalid limit: %v", err)
	}

	query.Limit = 10
	query.Index = "_does_not_exist_"
	_, err = table.Get(query)
	if err == nil || err.Status != http.StatusNotFound || err.Message != "Index not found" {
		t.Fatalf("Get should fail with index not found: %v", err)
	}

	query.Index = "_id"
	_, err = table.Get(query)
	if err == nil || err.Status != http.StatusNotFound {
		t.Fatalf("Get should fail with records not found: %v", err)
	}

	put(t, table, "a")
	expect(t, table, query, true, "a")
}

func TestLowerBound(t *testing.T) {
	mem := NewDriver()
	table, err := mem.GetTable("table", true)
	if err != nil {
		t.Fatalf("GetTable failed: %v", err)
	}

	put(t, table, "b", "c", "d")

	query := &driver.Query{
		Index: "_id",
		Limit: 10,
	}

	tt := []struct {
		key      string
		inc      bool
		expected []string
	}{
		{"a", true, []string{"b", "c", "d"}},
		{"a", false, []string{"b", "c", "d"}},
		{"b", true, []string{"b", "c", "d"}},
		{"b", false, []string{"c", "d"}},
		{"c", true, []string{"c", "d"}},
		{"c", false, []string{"d"}},
		{"d", true, []string{"d"}},
		{"d", false, []string{}},
		{"e", true, []string{}},
		{"e", false, []string{}},
	}

	for _, test := range tt {
		query.Lower = &driver.Bound{test.inc, test.key}
		expect(t, table, query, true, test.expected...)
	}
}

func TestUpperBound(t *testing.T) {
	mem := NewDriver()
	table, err := mem.GetTable("table", true)
	if err != nil {
		t.Fatalf("GetTable failed: %v", err)
	}

	put(t, table, "b", "c", "d")

	query := &driver.Query{
		Index: "_id",
		Limit: 10,
	}

	tt := []struct {
		key      string
		inc      bool
		expected []string
	}{
		{"a", true, []string{}},
		{"a", false, []string{}},
		{"b", true, []string{"b"}},
		{"b", false, []string{}},
		{"c", true, []string{"b", "c"}},
		{"c", false, []string{"b"}},
		{"d", true, []string{"b", "c", "d"}},
		{"d", false, []string{"b", "c"}},
		{"e", true, []string{"b", "c", "d"}},
		{"e", false, []string{"b", "c", "d"}},
	}

	for _, test := range tt {
		query.Upper = &driver.Bound{test.inc, test.key}
		expect(t, table, query, true, test.expected...)
	}
}

func TestRange(t *testing.T) {
	mem := NewDriver()
	table, err := mem.GetTable("table", true)
	if err != nil {
		t.Fatalf("GetTable failed: %v", err)
	}

	put(t, table, "b", "c", "d")

	query := &driver.Query{
		Index: "_id",
		Limit: 10,
	}

	tt := []struct {
		lkey     string
		linc     bool
		ukey     string
		uinc     bool
		expected []string
	}{
		{"a", true, "a", true, []string{}},
		{"b", true, "b", true, []string{"b"}},
		{"c", true, "c", true, []string{"c"}},
		{"d", true, "d", true, []string{"d"}},
		{"e", true, "e", true, []string{}},
		{"a", true, "e", true, []string{"b", "c", "d"}},
		{"a", false, "e", false, []string{"b", "c", "d"}},
		{"a", true, "b", false, []string{}},
		{"a", true, "b", true, []string{"b"}},
		{"b", true, "e", true, []string{"b", "c", "d"}},
		{"b", false, "e", true, []string{"c", "d"}},
	}

	for _, test := range tt {
		query.Lower = &driver.Bound{test.linc, test.lkey}
		query.Upper = &driver.Bound{test.uinc, test.ukey}
		expect(t, table, query, true, test.expected...)
	}
}
