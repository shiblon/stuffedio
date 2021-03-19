package wal

import (
	"fmt"
	"os"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestWAL_JournalOnly(t *testing.T) {
	msgs := []string{
		"Message 1",
		"Message 2",
		"Message 3",
		"Message 4",
		"Message 5",
	}

	dir, err := os.MkdirTemp("", "waltest-")
	if err != nil {
		t.Fatalf("Create temp dir: %v", err)
	}

	// Create a new journal inside a function (so we can defer closing it).
	if err := func() error {
		// Create a WAL (journals only, in this case) in the brand new dir.
		// We don't specify a loader or adder because we just created an empty
		// directory. See below for how to load things.
		//
		// We also force frequent rotation by severely limiting max counts.
		w, err := Open(dir, WithMaxJournalIndices(2))
		if err != nil {
			return fmt.Errorf("create empty WAL: %w", err)
		}
		defer w.Close()

		for _, msg := range msgs {
			if err := w.Append([]byte(msg)); err != nil {
				return fmt.Errorf("create, append initial: %w", err)
			}
		}
		return nil
	}(); err != nil {
		t.Fatalf("Error creating WAL: %v", err)
	}

	// Check that we have expected names.
	expectNames := []string{
		"j-0000000000000001",
		"j-0000000000000003",
		"j-0000000000000005",
	}
	var gotNames []string
	ds, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("Read dir: %v", err)
	}
	for _, de := range ds {
		gotNames = append(gotNames, de.Name())
	}
	if diff := cmp.Diff(expectNames, gotNames); diff != "" {
		t.Fatalf("Unexpected diff in dir names (-want +got):\n%v", diff)
	}

	additionalMsgs := []string{
		"Message 6",
		"Message 7",
	}

	// Open the WAL and read it, write a couple more records.
	var readMsgs []string
	if err := func() error {
		w, err := Open(dir,
			WithMaxJournalIndices(2),
			WithJournalPlayer(func(b []byte) error {
				readMsgs = append(readMsgs, string(b))
				return nil
			}),
		)
		if err != nil {
			return fmt.Errorf("read WAL: %w", err)
		}
		defer w.Close()

		for _, msg := range additionalMsgs {
			if err := w.Append([]byte(msg)); err != nil {
				return fmt.Errorf("append to WAL: %w", err)
			}
		}
		return nil
	}(); err != nil {
		t.Fatalf("Error reading/appending WAL: %v", err)
	}

	// Check that we read back the right things.
	if diff := cmp.Diff(msgs, readMsgs); diff != "" {
		t.Fatalf("Unexpected diff in written messages and read messages (-want +got):\n%v", diff)
	}

	// Check that a new file was added due to rotation.
	allNames := append(expectNames, "j-0000000000000007")
	gotNames = nil
	ds, err = os.ReadDir(dir)
	for _, de := range ds {
		gotNames = append(gotNames, de.Name())
	}
	if diff := cmp.Diff(allNames, gotNames); diff != "" {
		t.Fatalf("Unexpected diff in dir names after append (-want +got):\n%v", diff)
	}

	// Try reading everything back, check that it has every entry.
	finalExpected := append(msgs, additionalMsgs...)
	var finalMsgs []string
	if err := func() error {
		w, err := Open(dir,
			WithJournalPlayer(func(b []byte) error {
				finalMsgs = append(finalMsgs, string(b))
				return nil
			}),
		)
		if err != nil {
			return fmt.Errorf("read final WAL: %w", err)
		}
		return w.Close()
	}(); err != nil {
		t.Fatalf("Error reading final journal: %v", err)
	}

	// Check that we got all messages back.
	if diff := cmp.Diff(finalExpected, finalMsgs); diff != "" {
		t.Fatalf("Unexpected diff in final messages (-want +got):\n%v", diff)
	}
}
