package wal_test

import (
	"fmt"
	"io/fs"
	"log"
	"os"

	"entrogo.com/stuffedio/wal"
)

func appendToWAL(dir string, values []string) error {
	w, err := wal.Open(dir,
		wal.WithAllowWrite(true),
		wal.WithEmptySnapshotLoader(true),
		wal.WithEmptyJournalPlayer(true),
	)
	if err != nil {
		return fmt.Errorf("append to WAL: %w", err)
	}
	defer w.Close()

	for _, v := range values {
		if err := w.Append([]byte(v)); err != nil {
			return fmt.Errorf("append to WAL: %w", err)
		}
	}
	return nil
}

func readWAL(dir string) (w *wal.WAL, snapshot, journal []string, err error) {
	w, err = wal.Open(dir,
		wal.WithSnapshotLoader(func(b []byte) error {
			snapshot = append(snapshot, string(b))
			return nil
		}),
		wal.WithJournalPlayer(func(b []byte) error {
			journal = append(journal, string(b))
			return nil
		}),
	)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("read WAL: %v", err)
	}

	return w, snapshot, journal, nil
}

func makeSnapshot(w *wal.WAL, values []string) (err error) {
	snap, err := w.CreateSnapshot()
	if err != nil {
		return fmt.Errorf("Error createing snapshot: %w", err)
	}

	defer func() {
		// Only close if there is no error - otherwise leave it partial; it didn't work.
		if err == nil {
			snap.Close()
		}
	}()

	for i, v := range values {
		if _, err := snap.Append(uint64(i+1), []byte(v)); err != nil {
			return fmt.Errorf("Create snapshot append: %w", err)
		}
	}
	return nil
}

func mustLogFiles(dir string) {
	ds, err := fs.ReadDir(os.DirFS(dir), ".")
	if err != nil {
		log.Fatalf("Error reading dir %q: %v", dir, err)
	}
	for _, de := range ds {
		log.Print(de.Name())
	}
}

func Example() {
	// This example just makes something in tmp. Real use would use a more
	// durable location (and not delete afterward).
	dir, err := os.MkdirTemp("", "walex-")
	if err != nil {
		log.Fatalf("Error making temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	// Add some things to the WAL.
	if err := appendToWAL(dir, []string{
		"Message 1",
		"Message 2",
		"Message 3",
		"Message 4",
	}); err != nil {
		log.Fatalf("Error appending to empty WAL: %v", err)
	}

	// Read them back, also prepare to make a snapshot.
	w, snapshotVals, journalVals, err := readWAL(dir)
	if err != nil {
		log.Fatalf("Error reading initial WAL: %v", err)
	}
	defer w.Close()

	fmt.Println("Read Initial:")
	for _, val := range snapshotVals {
		fmt.Println("- Snapshot: " + val)
	}
	for _, val := range journalVals {
		fmt.Println("- Journal: " + val)
	}

	// Now create a snapshot and dump values into it, based on where we ended up when in read-only mode.
	if err := makeSnapshot(w, journalVals); err != nil {
		log.Fatalf("Error creating snapshot: %v", err)
	}

	// We can now open again in write mode, and dump more things in the journal:
	if err := appendToWAL(dir, []string{
		"Message 5",
		"Message 6",
	}); err != nil {
		log.Fatalf("Error appending to WAL after snapshot: %v", err)
	}

	// Read the whole WAL back, which will include the snapshot and latest journal.
	w, finalSnapVals, finalJVals, err := readWAL(dir)
	if err != nil {
		log.Fatalf("Error reading final WAL: %v", err)
	}
	defer w.Close()

	fmt.Println("Read Final:")
	for _, v := range finalSnapVals {
		fmt.Println("- Snapshot: " + v)
	}
	for _, v := range finalJVals {
		fmt.Println("- Journal: " + v)
	}

	// Output:
	// Read Initial:
	// - Journal: Message 1
	// - Journal: Message 2
	// - Journal: Message 3
	// - Journal: Message 4
	// Read Final:
	// - Snapshot: Message 1
	// - Snapshot: Message 2
	// - Snapshot: Message 3
	// - Snapshot: Message 4
	// - Journal: Message 5
	// - Journal: Message 6
}
