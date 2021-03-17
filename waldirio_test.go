package stuffedio

import (
	"bytes"
	"fmt"
	"log"
	"testing/fstest"
)

func fakeJournalData(start, end uint64) (string, []byte) {
	buf := new(bytes.Buffer)
	w := NewWALWriter(NewWriter(buf), WithFirstIndex(start))
	defer w.Close()

	for i := start; i < end; i++ {
		if err := w.Append(i, []byte(fmt.Sprintf("Record with number %d", i))); err != nil {
			log.Fatalf("Error appending: %v", err)
		}
	}

	return IndexName("journal", start), buf.Bytes()
}

func ExampleWALDirReader() {
	// Create a fake file system with some data in it.
	fakeFS := make(fstest.MapFS)
	ends := []uint64{3, 5, 7}
	start := uint64(1)
	for _, end := range ends {
		name, val := fakeJournalData(start, end)
		fakeFS[name] = &fstest.MapFile{
			Data: val,
		}
		start = end
	}

	// Set up a journal reader for that file system.
	r, err := NewWALDirReader(fakeFS)
	if err != nil {
		log.Fatalf("Error opening journal fs: %v", err)
	}
	defer r.Close()

	// Read entries in order.
	for !r.Done() {
		idx, val, err := r.Next()
		if err != nil {
			log.Fatalf("Error reading next value: %v", err)
		}
		fmt.Printf("%d: %q\n", idx, val)
	}

	// Output:
	// 1: "Record with number 1"
	// 2: "Record with number 2"
	// 3: "Record with number 3"
	// 4: "Record with number 4"
	// 5: "Record with number 5"
	// 6: "Record with number 6"
}
