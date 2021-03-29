package recordio_test

import (
	"bytes"
	"fmt"
	"log"
	"testing/fstest"

	"entrogo.com/stuffedio/recordio"
)

const prefix = "journal"

func fakeJournalData(start, end uint64) (string, []byte) {
	buf := new(bytes.Buffer)
	enc := recordio.NewEncoder(buf)
	defer enc.Close()

	for i := start; i < end; i++ {
		if _, err := enc.Encode([]byte(fmt.Sprintf("Record with number %d", i))); err != nil {
			log.Fatalf("Error appending: %v", err)
		}
	}

	return fmt.Sprintf("%s-%016x", prefix, start), buf.Bytes()
}

func ExampleMultiDecoder() {
	// Create a fake file system with some data in it.
	fakeFS := make(fstest.MapFS)

	var names []string

	ends := []uint64{3, 5, 7}
	start := uint64(1)
	for _, end := range ends {
		name, val := fakeJournalData(start, end)
		names = append(names, name)
		fakeFS[name] = &fstest.MapFile{Data: val}
		start = end
	}

	// Create a MultiDecoder WAL that knows about these files, using a FilesDecoderIterator.
	dec := recordio.NewMultiDecoderIter(recordio.NewFilesDecoderIterator(fakeFS, names))
	defer dec.Close()

	// Read entries in order.
	for !dec.Done() {
		val, err := dec.Next()
		if err != nil {
			log.Fatalf("Error reading next value: %v", err)
		}
		fmt.Printf("%q\n", val)
	}

	// Output:
	// "Record with number 1"
	// "Record with number 2"
	// "Record with number 3"
	// "Record with number 4"
	// "Record with number 5"
	// "Record with number 6"
}
