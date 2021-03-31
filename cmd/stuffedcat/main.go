// Command stuffedcat allows you to cat stuffedio logs, either record or ordered IO.
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	"entrogo.com/stuffedio/orderedio"
	"entrogo.com/stuffedio/recordio"
)

func catRecords(r io.Reader) error {
	d := recordio.NewDecoder(r)

	for !d.Done() {
		b, err := d.Next()
		if err != nil {
			return fmt.Errorf("record next: %w", err)
		}
		fmt.Printf("%q\n", b)
	}
	return nil
}

func catOrdered(r io.Reader) error {
	d := orderedio.NewStreamDecoder(r)

	for !d.Done() {
		idx, b, err := d.Next()
		if err != nil {
			return fmt.Errorf("ordered next: %w", err)
		}
		fmt.Printf("[%05d, %s]\n", idx, b)
	}
	return nil
}

func catFile(fname string) error {
	// Try ordered first, then unordered if that doesn't work out.
	f, err := os.Open(fname)
	if err != nil {
		return fmt.Errorf("open file: %w", err)
	}
	if err := catOrdered(f); err != nil {
		if !errors.Is(err, recordio.CorruptRecord) {
			return fmt.Errorf("ordered IO read failure: %w", err)
		}
		f.Seek(0, io.SeekStart)
		if err := catRecords(f); err != nil {
			return fmt.Errorf("record IO read failure: %w", err)
		}
	}
	return nil
}

func main() {
	flag.Parse()
	if flag.NArg() == 0 {
		log.Fatalf("At least one filename needed.")
	}

	for _, fname := range flag.Args() {
		fmt.Printf("%q\n", fname)
		if err := catFile(fname); err != nil {
			log.Printf("Error: %v", err)
		}
	}
}
