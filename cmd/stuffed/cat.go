package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/shiblon/stuffedio/orderedio"
	"github.com/shiblon/stuffedio/recordio"
)

// jsonData returns b as a json.RawMessage if it is valid JSON, otherwise as a
// JSON-encoded string.
func jsonData(b []byte) json.RawMessage {
	if json.Valid(b) {
		return b
	}
	s, _ := json.Marshal(string(b))
	return s
}

func catRecords(r io.Reader, w io.Writer, asJSON bool) error {
	d := recordio.NewDecoder(r)

	for !d.Done() {
		b, err := d.Next()
		if err != nil {
			return fmt.Errorf("record next: %w", err)
		}
		if asJSON {
			fmt.Fprintf(w, `{"data":%s}`+"\n", jsonData(b))
		} else {
			fmt.Fprintf(w, "%q\n", b)
		}
	}
	return nil
}

func catOrdered(r io.Reader, w io.Writer, asJSON bool) error {
	d := orderedio.NewStreamDecoder(r)

	for !d.Done() {
		idx, b, err := d.Next()
		if err != nil {
			return fmt.Errorf("ordered next: %w", err)
		}
		if asJSON {
			fmt.Fprintf(w, `{"index":%d,"data":%s}`+"\n", idx, jsonData(b))
		} else {
			fmt.Fprintf(w, "[%05d, %s]\n", idx, b)
		}
	}
	return nil
}

func catFile(fname string, w io.Writer, asJSON bool) error {
	// Try ordered first, then unordered if that doesn't work out.
	f, err := os.Open(fname)
	if err != nil {
		return fmt.Errorf("open file: %w", err)
	}
	if err := catOrdered(f, w, asJSON); err != nil {
		if !errors.Is(err, recordio.CorruptRecord) {
			return fmt.Errorf("ordered IO read failure: %w", err)
		}
		f.Seek(0, io.SeekStart)
		if err := catRecords(f, w, asJSON); err != nil {
			return fmt.Errorf("record IO read failure: %w", err)
		}
	}
	return nil
}

func cmdCat(args []string) {
	fs := flag.NewFlagSet("cat", flag.ExitOnError)
	asJSON := fs.Bool("j", false, "Output records as JSON objects.")
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: stuffed cat [-j] <file> [file...]\n\nPrint records from stuffed log files.\n")
		fs.PrintDefaults()
	}
	fs.Parse(args)

	if fs.NArg() == 0 {
		fs.Usage()
		os.Exit(2)
	}

	for _, fname := range fs.Args() {
		fmt.Printf(`{"FILE": %q}`+"\n", fname)
		if err := catFile(fname, os.Stdout, *asJSON); err != nil {
			log.Printf("Error: %v", err)
		}
	}
}
