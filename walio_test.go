package stuffedio

import (
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func ExampleReadWrite() {
	buf := new(bytes.Buffer)
	w := NewStuffer(buf).WAL()

	// Write messages.
	msgs := []string{
		"This is a message",
		"This is another message",
		"And here's a third",
	}

	for i, msg := range msgs {
		if err := w.Append(uint64(i)+1, []byte(msg)); err != nil {
			log.Fatalf("Append error: %v", err)
		}
	}

	// Now read them back.
	r := NewUnstuffer(buf).WAL()
	for !r.Done() {
		idx, val, err := r.Next()
		if err != nil {
			log.Fatalf("Read error: %v", err)
		}
		fmt.Printf("%d: %q\n", idx, string(val))
	}

	// Output:
	// 1: "This is a message"
	// 2: "This is another message"
	// 3: "And here's a third"
}

func TestWAL(t *testing.T) {
	type entry struct {
		i   uint64
		val string
	}
	cases := []struct {
		name             string
		entries          []entry
		randomlyCorrupt  int
		writeFirstIndex  uint64
		readInitialIndex uint64
		writeError       bool
		readError        bool
	}{
		{
			name: "all-is-well",
			entries: []entry{
				{1, "One message"},
				{2, "Two message"},
				{3, "Three message"},
			},
		},
		{
			name: "start-late-default-initial",
			entries: []entry{
				{3, "One message"},
				{4, "Two message"},
				{5, "Three message"},
			},
			writeError:      true,
			writeFirstIndex: 1,
		},
		{
			name: "start-late-set-write-index",
			entries: []entry{
				{3, "One message"},
				{4, "Two message"},
				{5, "Three message"},
			},
			writeFirstIndex: 3,
		},
		{
			name: "start-late-set-write-read-index",
			entries: []entry{
				{3, "One message"},
				{4, "Two message"},
				{5, "Three message"},
			},
			writeFirstIndex:  3,
			readInitialIndex: 3,
		},
		{
			name: "start-late-set-write-bad-read-index",
			entries: []entry{
				{3, "One message"},
				{4, "Two message"},
				{5, "Three message"},
			},
			writeFirstIndex:  3,
			readInitialIndex: 5,
			readError:        true,
		},
		{
			name: "corrupt-random-byte",
			entries: []entry{
				{1, "One message"},
				{2, "Two message"},
				{3, "Three"},
			},
			randomlyCorrupt: 1,
			readError:       true,
		},
		{
			name: "corrupt-random-bytes",
			entries: []entry{
				{1, "One message"},
				{2, "Two message"},
				{3, "Three"},
			},
			randomlyCorrupt: 3,
			readError:       true,
		},
	}

	for _, test := range cases {
		buf := new(bytes.Buffer)

		// Test writes.
		w := NewWALStuffer(NewStuffer(buf), WithFirstIndex(test.writeFirstIndex))

		var writeErr error
		for _, entry := range test.entries {
			if err := w.Append(entry.i, []byte(entry.val)); err != nil {
				writeErr = err
			}
		}
		if writeErr != nil && !test.writeError {
			t.Fatalf("WAL %q: unexpected error: %v", test.name, writeErr)
		} else if writeErr == nil && test.writeError {
			t.Fatalf("WAL %q: expected error, got none", test.name)
		}

		// No point in reading - we expected an error.
		if test.writeError {
			continue
		}

		// If we are supposed to corrupt some bytes, do that now.
		for i := 0; i < test.randomlyCorrupt; i++ {
			idx := rand.Intn(buf.Len())
			val := buf.Bytes()[idx]
			for {
				newVal := byte(rand.Intn(256))
				if newVal != val {
					buf.Bytes()[idx] = newVal
					break
				}
			}
		}

		// Test reads.
		r := NewWALUnstuffer(NewUnstuffer(buf), ExpectFirstIndex(test.readInitialIndex))

		entryIdx := 0
		var readErr error
		for !r.Done() {
			index, val, err := r.Next()
			if err != nil {
				readErr = err
				continue
			}
			expected := test.entries[entryIdx]
			if index != expected.i {
				t.Fatalf("WAL %q: read index expected %d, got %d", test.name, expected.i, index)
			}
			if diff := cmp.Diff(expected.val, string(val)); diff != "" {
				t.Fatalf("WAL %q: unexpected diff in read val (-want +got):\n%v", test.name, diff)
			}
			entryIdx++
		}

		if readErr != nil && !test.readError {
			t.Fatalf("WAL %q: unexpected read error: %v", test.name, readErr)
		} else if readErr == nil && test.readError {
			t.Fatalf("WAL %q: expected error, got none", test.name)
		}
	}
}
