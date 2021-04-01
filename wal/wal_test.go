package wal

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func mustTempDir(t *testing.T) (dname string, cleanup func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "waltest-")
	if err != nil {
		t.Fatalf("Create temp dir: %v", err)
	}
	return dir, func() {
		os.RemoveAll(dir)
	}
}

func TestWAL_Snapshots(t *testing.T) {
	dir, cleanup := mustTempDir(t)
	defer cleanup()

	initialValues := []string{
		"Hello 1",
		"Hello 2",
		"Hello 3",
	}

	ctx := context.Background()

	// Create a snapshot.
	func() {
		// Get started.
		w, err := Open(ctx, dir, WithRequireEmpty(true))
		// Try a snapshot right away, seed with initial values.
		snapName, err := w.CreateSnapshot(func(a ValueAdder) error {
			for _, v := range initialValues {
				if err := a.AddValue([]byte(v)); err != nil {
					return fmt.Errorf("snapshotter: %w", err)
				}
			}
			return nil
		})
		if err != nil {
			t.Fatalf("WAL snapshot adder: %v", err)
		}
		// All zeros on initial snapshots with no previous journals. Last index
		// seen is not a valid journal index, it's *right before* a valid
		// journal index.
		if want, got := "0000000000000000-snapshot-final", filepath.Base(snapName); got != want {
			t.Fatalf("WAL snapshot: expected name %q, got %q", want, got)
		}
	}()

	journalValues := []string{
		"Hello 4",
		"Hello 5",
	}

	// Try reading it with our WAL reader.
	func() {
		var snapFound []string
		w, err := Open(ctx, dir,
			WithAllowWrite(true),
			WithSnapshotLoaderFunc(func(_ context.Context, b []byte) error {
				snapFound = append(snapFound, string(b))
				return nil
			}),
		)
		if err != nil {
			t.Fatalf("WAL snapshot open: %v", err)
		}
		if diff := cmp.Diff(initialValues, snapFound); diff != "" {
			t.Fatalf("WAL snapshot re-read unexpected diff (-want +got):\n%v", diff)
		}

		// Try writing to the WAL to get a journal file in there after the snapshot.
		for _, v := range journalValues {
			if err := w.Append([]byte(v)); err != nil {
				t.Fatalf("WAL snapshot write to journal: %v", err)
			}
		}
	}()

	expectNames := []string{
		"0000000000000000-snapshot-final",
		"0000000000000001-journal",
	}
	ds, err := fs.ReadDir(os.DirFS(dir), ".")
	if err != nil {
		t.Fatalf("WAL snapshot read dir: %v", err)
	}
	var gotNames []string
	for _, de := range ds {
		gotNames = append(gotNames, de.Name())
	}
	sort.Strings(gotNames)
	if diff := cmp.Diff(expectNames, gotNames); diff != "" {
		t.Fatalf("WAL snapshot with journal: unexpected filename diff (-want +got):\n%v", diff)
	}

	// Finally, try reading it all back, snapshot and journal, ensure that we
	// have what we expect.
	func() {
		var snapFound []string
		var journalFound []string
		if _, err := Open(ctx, dir,
			WithJournalPlayerFunc(func(_ context.Context, b []byte) error {
				journalFound = append(journalFound, string(b))
				return nil
			}),
			WithSnapshotLoaderFunc(func(_ context.Context, b []byte) error {
				snapFound = append(snapFound, string(b))
				return nil
			}),
		); err != nil {
			t.Fatalf("WAL snapshot read with journal: %v", err)
		}

		if diff := cmp.Diff(initialValues, snapFound); diff != "" {
			t.Fatalf("WAL snapshot read with journal: unexpected diff in snapshot (-want +got):\n%v", diff)
		}
		if diff := cmp.Diff(journalValues, journalFound); diff != "" {
			t.Fatalf("WAL snapshot read with journal: unexpected diff in journal (-want +got):\n%v", diff)
		}
	}()
}

func TestWAL_ReadOnly(t *testing.T) {
	ctx := context.Background()

	dir, cleanup := mustTempDir(t)
	defer cleanup()

	w, err := Open(ctx, dir)
	if err != nil {
		t.Fatalf("WAL read only: empty open: %v", err)
	}

	if err := w.Append([]byte("hello")); err == nil {
		t.Errorf("wal read only: empty append: expected error on append")
	}

	w, err = Open(ctx, dir, WithAllowWrite(true))
	if err != nil {
		t.Fatalf("WAL read only: writeable open: %v", err)
	}
	if err := w.Append([]byte("hello")); err != nil {
		t.Errorf("wal read only: writeable append: %v", err)
	}
	w.Close()

	var found []string
	w, err = Open(ctx, dir,
		WithAllowWrite(false),
		WithJournalPlayerFunc(func(_ context.Context, b []byte) error {
			found = append(found, string(b))
			return nil
		}),
	)
	if err != nil {
		t.Fatalf("WAL read only: non-empty open: %v", err)
	}
	want := []string{"hello"}
	if diff := cmp.Diff(want, found); diff != "" {
		t.Fatalf("WAL read only: non-empty read unexpected diff (-want +got):\n%v", diff)
	}
}

func TestWAL_JournalOnly(t *testing.T) {
	ctx := context.Background()

	msgs := []string{
		"Message 1",
		"Message 2",
		"Message 3",
		"Message 4",
		"Message 5",
	}

	dir, cleanup := mustTempDir(t)
	defer cleanup()

	// Create a new journal inside a function (so we can defer closing it).
	if err := func() error {
		// Create a WAL (journals only, in this case) in the brand new dir.
		// We don't specify a loader or adder because we just created an empty
		// directory. See below for how to load things.
		//
		// We also force frequent rotation by severely limiting max counts.
		w, err := Open(ctx, dir, WithMaxJournalIndices(2), WithAllowWrite(true))
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
		"0000000000000001-journal-final",
		"0000000000000003-journal-final",
		"0000000000000005-journal",
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
		w, err := Open(ctx, dir,
			WithMaxJournalIndices(2),
			WithAllowWrite(true),
			WithJournalPlayerFunc(func(_ context.Context, b []byte) error {
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
	allNames := []string{
		"0000000000000001-journal-final",
		"0000000000000003-journal-final",
		"0000000000000005-journal-final",
		"0000000000000007-journal",
	}
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
		w, err := Open(ctx, dir,
			WithJournalPlayerFunc(func(_ context.Context, b []byte) error {
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
