package stuffedio

import (
	"bytes"
	"fmt"
	"log"
)

func Example_WAL() {
	buf := new(bytes.Buffer)
	w := NewWALWriter(NewWriter(buf))

	// Write messages.
	msgs := []string{
		"This is a message",
		"This is another message",
	}

	for i, msg := range msgs {
		if err := w.Append(uint64(i)+1, []byte(msg)); err != nil {
			log.Fatalf("Append error: %v", err)
		}
	}

	// Now read them back.
	r := NewWALReader(NewReader(buf))
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
}
