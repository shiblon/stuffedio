package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/shiblon/stuffedio/orderedio"
	"github.com/shiblon/stuffedio/recordio"
)

// seedOrdered writes an ordered log at fname with the given entries starting at index 1.
func seedOrdered(t *testing.T, fname string, entries []string) {
	t.Helper()
	f, err := os.OpenFile(fname, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatalf("seedOrdered open: %v", err)
	}
	enc := orderedio.NewStreamEncoder(f, orderedio.WithFirstIndex(1))
	defer enc.Close()
	for _, s := range entries {
		if _, err := enc.Encode(enc.NextIndex(), []byte(s)); err != nil {
			t.Fatalf("seedOrdered encode: %v", err)
		}
	}
}

func TestStuffAndCat_plain(t *testing.T) {
	fname := filepath.Join(t.TempDir(), "plain.log")

	lines := []string{"one", "two", "three"}
	if err := stuffFile(fname, strings.NewReader(strings.Join(lines, "\n")+"\n"), false); err != nil {
		t.Fatalf("stuffFile: %v", err)
	}

	var out bytes.Buffer
	if err := catFile(fname, &out, false); err != nil {
		t.Fatalf("catFile: %v", err)
	}

	got := out.String()
	for _, line := range lines {
		if !strings.Contains(got, line) {
			t.Errorf("output missing %q; got:\n%s", line, got)
		}
	}
}

func TestStuffAndCat_ordered(t *testing.T) {
	fname := filepath.Join(t.TempDir(), "ordered.log")

	seedOrdered(t, fname, []string{"alpha", "beta"})

	// stuffFile should detect the ordered format and continue from index 3.
	if err := stuffFile(fname, strings.NewReader("gamma\ndelta\n"), false); err != nil {
		t.Fatalf("stuffFile ordered append: %v", err)
	}

	var out bytes.Buffer
	if err := catFile(fname, &out, false); err != nil {
		t.Fatalf("catFile ordered: %v", err)
	}

	got := out.String()
	for _, want := range []string{"alpha", "beta", "gamma", "delta"} {
		if !strings.Contains(got, want) {
			t.Errorf("output missing %q; got:\n%s", want, got)
		}
	}
}

func TestStuffAndCat_json(t *testing.T) {
	fname := filepath.Join(t.TempDir(), "json.log")

	records := []string{`{"a":1}`, `{"b":2}`}
	if err := stuffFile(fname, strings.NewReader(strings.Join(records, "\n")+"\n"), true); err != nil {
		t.Fatalf("stuffFile json: %v", err)
	}

	var out bytes.Buffer
	if err := catFile(fname, &out, true); err != nil {
		t.Fatalf("catFile json: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	want := []string{`{"data":{"a":1}}`, `{"data":{"b":2}}`}
	if len(lines) != len(want) {
		t.Fatalf("want %d lines, got %d:\n%s", len(want), len(lines), out.String())
	}
	for i, w := range want {
		if lines[i] != w {
			t.Errorf("line %d: want %q, got %q", i, w, lines[i])
		}
	}
}

func TestStuff_jsonValidation(t *testing.T) {
	fname := filepath.Join(t.TempDir(), "bad.log")
	if err := stuffFile(fname, strings.NewReader("not json\n"), true); err == nil {
		t.Fatal("expected error for invalid JSON input, got nil")
	}
}

func Example_catRecords() {
	var buf bytes.Buffer
	enc := recordio.NewEncoder(&buf)
	enc.Encode([]byte("hello"))
	enc.Encode([]byte("world"))
	enc.Close()

	catRecords(&buf, os.Stdout, false)
	// Output:
	// "hello"
	// "world"
}

func Example_catOrdered() {
	var buf bytes.Buffer
	enc := orderedio.NewStreamEncoder(&buf)
	enc.Encode(1, []byte(`{"msg":"first"}`))
	enc.Encode(2, []byte(`{"msg":"second"}`))
	enc.Close()

	catOrdered(&buf, os.Stdout, true)
	// Output:
	// {"index":1,"data":{"msg":"first"}}
	// {"index":2,"data":{"msg":"second"}}
}
