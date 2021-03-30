package wal_test

import (
	"context"
	"fmt"
	"io/fs"
	"log"
	"os"

	"entrogo.com/stuffedio/wal"
)

func appendToWAL(ctx context.Context, dir string, values []string, finalize bool) error {
	w, err := wal.Open(ctx, dir,
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
	if finalize {
		if err := w.Finalize(); err != nil {
			return fmt.Errorf("append to WAL: %w", err)
		}
	}
	return nil
}

func readWAL(ctx context.Context, dir string) (snapshot, journal []string, err error) {
	w, err := wal.Open(ctx, dir,
		wal.WithSnapshotLoaderFunc(func(_ context.Context, b []byte) error {
			snapshot = append(snapshot, string(b))
			return nil
		}),
		wal.WithJournalPlayerFunc(func(_ context.Context, b []byte) error {
			journal = append(journal, string(b))
			return nil
		}),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("read WAL: %v", err)
	}
	defer w.Close()

	return snapshot, journal, nil
}

func makeSnapshot(ctx context.Context, dir string) (string, error) {
	var values []string
	w, err := wal.Open(ctx, dir,
		wal.WithExcludeLiveJournal(true),
		wal.WithJournalPlayerFunc(func(_ context.Context, b []byte) error {
			values = append(values, string(b))
			return nil
		}),
		wal.WithSnapshotLoaderFunc(func(_ context.Context, b []byte) error {
			values = append(values, string(b))
			return nil
		}),
	)
	if err != nil {
		return "", fmt.Errorf("make snapshot: %w", err)
	}

	if err := w.CheckCanSnapshot(); err != nil {
		return "", fmt.Errorf("make snapshot: %w", err)
	}

	fname, err := w.CreateSnapshot(func(a wal.ValueAdder) error {
		for _, val := range values {
			if err := a.AddValue([]byte(val)); err != nil {
				return fmt.Errorf("snapshotter: %w", err)
			}
		}
		return nil
	})
	if err != nil {
		return "", fmt.Errorf("make snapshot: %w", err)
	}
	return fname, nil
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
	ctx := context.Background()
	// This example just makes something in tmp. Real use would use a more
	// durable location (and not delete afterward).
	dir, err := os.MkdirTemp("", "walex-")
	if err != nil {
		log.Fatalf("Error making temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	// Add some things to the WAL.
	if err := appendToWAL(ctx, dir, []string{
		"Message 1",
		"Message 2",
		"Message 3",
		"Message 4",
	}, true); err != nil {
		log.Fatalf("Error appending to empty WAL: %v", err)
	}

	// Read them back, also prepare to make a snapshot.
	snapshotVals, journalVals, err := readWAL(ctx, dir)
	if err != nil {
		log.Fatalf("Error reading initial WAL: %v", err)
	}

	fmt.Println("Read Initial:")
	for _, val := range snapshotVals {
		fmt.Println("- Snapshot: " + val)
	}
	for _, val := range journalVals {
		fmt.Println("- Journal: " + val)
	}

	// Now create a snapshot and dump values into it, based on where we ended
	// up when in read-only mode.
	if _, err := makeSnapshot(ctx, dir); err != nil {
		log.Fatalf("Error creating snapshot: %v", err)
	}

	// We can now open again in write mode, and dump more things in the
	// journal. We'll leave it "live" this time:
	if err := appendToWAL(ctx, dir, []string{
		"Message 5",
		"Message 6",
	}, false); err != nil {
		log.Fatalf("Error appending to WAL after snapshot: %v", err)
	}

	// Read the whole WAL back, which will include the snapshot and latest journal.
	finalSnapVals, finalJVals, err := readWAL(ctx, dir)
	if err != nil {
		log.Fatalf("Error reading final WAL: %v", err)
	}

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
