package main

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"hash/crc32"
	"io"
	"os"

	"github.com/shiblon/stuffedio/orderedio"
	"github.com/shiblon/stuffedio/recordio"
)

// lastOrderedIndex peeks at the last raw record in fname and checks whether it
// looks like a valid orderedio entry. If so, it returns (true, lastIndex).
// Returns (false, 0) if the file is empty, absent, or not in ordered format.
func lastOrderedIndex(fname string) (uint64, bool) {
	f, err := os.Open(fname)
	if err != nil {
		return 0, false
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil || stat.Size() == 0 {
		return 0, false
	}

	rd := recordio.NewReverseDecoder(f, stat.Size())
	if rd.Done() {
		return 0, false
	}

	raw, err := rd.Next()
	if err != nil || len(raw) < 12 {
		return 0, false
	}

	idx := binary.LittleEndian.Uint64(raw[0:8])
	storedCRC := binary.LittleEndian.Uint32(raw[8:12])
	computedCRC := crc32.Checksum(raw[12:], orderedio.CRCTable)

	if idx == 0 || storedCRC != computedCRC {
		return 0, false
	}

	return idx, true
}

func stuffFile(fname string, r io.Reader, asJSON bool) error {
	lastIdx, isOrdered := lastOrderedIndex(fname)

	f, err := os.OpenFile(fname, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("open for append: %w", err)
	}

	sc := bufio.NewScanner(r)

	encode := func(data []byte) error { return nil } // assigned below

	if isOrdered {
		enc := orderedio.NewStreamEncoder(f, orderedio.WithFirstIndex(lastIdx+1))
		defer enc.Close()
		encode = func(data []byte) error {
			_, err := enc.Encode(enc.NextIndex(), data)
			return err
		}
	} else {
		enc := recordio.NewEncoder(f)
		defer enc.Close()
		encode = func(data []byte) error {
			_, err := enc.Encode(data)
			return err
		}
	}

	for sc.Scan() {
		line := []byte(sc.Text())
		if asJSON && !json.Valid(line) {
			return fmt.Errorf("invalid JSON: %q", line)
		}
		if err := encode(line); err != nil {
			return fmt.Errorf("encode: %w", err)
		}
	}
	return sc.Err()
}

func cmdStuff(args []string) {
	fs := flag.NewFlagSet("stuff", flag.ExitOnError)
	asJSON := fs.Bool("j", false, "Validate each input line as JSON before writing.")
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: stuffed stuff [-j] <file>\n\nAppend stdin lines as records to a stuffed log file.\nDetects ordered vs. plain format automatically from an existing file.\n")
		fs.PrintDefaults()
	}
	fs.Parse(args)

	if fs.NArg() != 1 {
		fs.Usage()
		os.Exit(2)
	}

	if err := stuffFile(fs.Arg(0), os.Stdin, *asJSON); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
