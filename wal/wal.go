// Package wal implements a full write-ahead log over a directory of files,
// including snapshot management, reply iterators, and rotating log writers.
package wal

import (
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"

	"entrogo.com/stuffedio"
)

const (
	DefaultJournalBase  = "journal"
	DefaultSnapshotBase = "snapshot"
	DefaultMaxIndices   = 10 * 1 << 10 // 10Ki
	DefaultMaxBytes     = 10 * 1 << 20 // 10Mi
)

var (
	indexPattern = regexp.MustCompile(`^([a-fA-F0-9]+)-([^.]+)$`)
)

// Loader is a function type called by snapshot item loads and journal entry replay.
type Loader func([]byte) error

// Option describes a WAL creation option.
type Option func(*WAL)

// WithJournalBase sets the journal base, otherwise uses DefaultJournalBase.
func WithJournalBase(p string) Option {
	return func(w *WAL) {
		w.journalBase = p
	}
}

// WithSnaphotBase sets the snapshot base, otherwise uses DefaultSnapshotBase.
func WithSnapshotBase(p string) Option {
	return func(w *WAL) {
		w.snapshotBase = p
	}
}

// WithSnapshotAdder sets the record adder for all snapshot records. Clients
// provide one of these to allow snapshots to be loaded.
func WithSnapshotAdder(a Loader) Option {
	return func(w *WAL) {
		w.snapshotAdder = a
	}
}

// WithJournalPlayer sets the journal player for all journal records after the
// latest snapshot. Clients provide this to allow journals to be replayed.
func WithJournalPlayer(p Loader) Option {
	return func(w *WAL) {
		w.journalPlayer = p
	}
}

// WithMaxJournalBytes sets the maximum number of bytes before a journal is rotated.
// Default is DefaultMaxBytes.
func WithMaxJournalBytes(m int64) Option {
	return func(w *WAL) {
		w.maxJournalBytes = m
	}
}

// WithMaxJournalIndices sets the maximum number of indices in a journal file
// before it must be rotated.
func WithMaxJournalIndices(m int) Option {
	return func(w *WAL) {
		w.maxJournalIndices = m
	}
}

// WithAllowWrite lets the WAL go into "write" mode after it has been parsed.
// The default is to be read-only, and to create errors when attempting to do
// write operations on the file system. This makes it easy to not make
// mistakes, for example, when trying to collect journals to make a new
// snapshot. The WAL can be opened in read-only mode, a snapshot can be
// created, then it can be reopened in write mode and it will know to load that
// snapshot instead of replaying the entire set of journals. An empty snapshot
// loader can be given in that case to speed the loading process.
func WithAllowWrite(a bool) Option {
	return func(w *WAL) {
		w.allowWrite = a
	}
}

// WithAllowEmptySnapshotAdder indicates that not loading a snapshot is
// actually desired. This is to prevent mistakes: usually a snapshot adder is
// wanted, if there is a snapshot to be added.
func WithAllowEmptySnapshotAdder(a bool) Option {
	return func(w *WAL) {
		w.allowEmptyAdder = a
	}
}

// WithAllowEmptyJournalPlayer indicates that journals are to be scanned, not processed.
// This is a safety measure to avoid default behavior being unwanted: usually
// you want to process journal entries, but sometimes there is good reason to
// simply scan for the proper index and start appending.
func WithAllowEmptyJournalPlayer(a bool) Option {
	return func(w *WAL) {
		w.allowEmptyPlayer = a
	}
}

// WAL implements a write-ahead logger capable of replaying snapshots and
// journals, setting up a writer for appending to journals and rotating them
// when full, etc.
type WAL struct {
	dir               string
	journalBase       string
	snapshotBase      string
	maxJournalBytes   int64
	maxJournalIndices int

	allowEmptyAdder  bool
	allowEmptyPlayer bool
	allowWrite       bool

	snapshotAdder Loader
	journalPlayer Loader

	snapshotWasLast bool // If the snapshot was the last thing read (no later journals).

	currSize    int64                 // Current size of current journal file.
	currCount   int                   // Current number of indices in current journal file.
	nextIndex   uint64                // Keep track of what the next record's index should be.
	currStuffer *stuffedio.WALStuffer // The current stuffer, can be rotated on write.
}

// Open opens a directory and loads the WAL found in it, then provides a WAL
// that can be appended to over time.
func Open(dir string, opts ...Option) (*WAL, error) {
	w := &WAL{
		dir: dir,

		journalBase:       DefaultJournalBase,
		maxJournalBytes:   DefaultMaxBytes,
		maxJournalIndices: DefaultMaxIndices,
		snapshotBase:      DefaultSnapshotBase,
	}

	for _, opt := range opts {
		opt(w)
	}

	fsys := os.DirFS(dir)

	type entry struct {
		i    uint64
		name string
	}

	var (
		snapshotEntries []entry
		journalEntries  []entry
	)

	ds, err := fs.ReadDir(fsys, ".")
	if err != nil {
		return nil, fmt.Errorf("open wal list files: %w", err)
	}
	for _, de := range ds {
		base, idx, err := ParseIndexName(de.Name())
		if err != nil {
			log.Printf("Unrecognized file pattern in %q, skipping", de.Name())
			continue
		}
		switch base {
		case w.journalBase:
			journalEntries = append(journalEntries, entry{idx, de.Name()})
		case w.snapshotBase:
			snapshotEntries = append(snapshotEntries, entry{idx, de.Name()})
		default:
			log.Printf("Unrecognized file base in %q, skipping", de.Name())
		}
	}
	sort.Slice(snapshotEntries, func(i, j int) bool {
		return snapshotEntries[i].i < snapshotEntries[j].i
	})
	sort.Slice(journalEntries, func(i, j int) bool {
		return journalEntries[i].i < journalEntries[j].i
	})

	snapshotEntry := entry{}
	if len(snapshotEntries) != 0 {
		snapshotEntry = snapshotEntries[len(snapshotEntries)-1]

		if w.allowWrite {
			// Move early snapshots to non-matching names. Can be collected later.
			earlier := snapshotEntries[:len(snapshotEntries)-1]
			for _, e := range earlier {
				name := e.name
				if err := os.Rename(filepath.Join(dir, name), filepath.Join(dir, "_old__"+name)); err != nil {
					return nil, fmt.Errorf("open wal move early snapshots: %w", err)
				}
			}
		}
	}

	// Default to no journals.
	jStart := len(journalEntries)
	for i, entry := range journalEntries {
		if entry.i > snapshotEntry.i {
			jStart = i
			break
		}
	}

	// Move early journals to non-matching names. Can be collected later.
	if w.allowWrite {
		for _, e := range journalEntries[:jStart] {
			if err := os.Rename(filepath.Join(dir, e.name), filepath.Join(dir, "_old__"+e.name)); err != nil {
				return nil, fmt.Errorf("open wal move early journals: %w", err)
			}
		}
	}

	if snapshotEntry.name != "" {
		// Only allow snapshot load to be skipped if explicitly asked.
		if !w.allowEmptyAdder && w.snapshotAdder == nil {
			return nil, fmt.Errorf("open wal: snapshot found but no snapshot adder option given")
		}
		// Skip reading snapshot if requested.
		if w.snapshotAdder != nil {
			f, err := fsys.Open(snapshotEntry.name)
			if err != nil {
				return nil, fmt.Errorf("open wal open snapshot: %w", err)
			}
			u := stuffedio.NewUnstuffer(f).WAL()
			for !u.Done() {
				idx, b, err := u.Next()
				if err != nil {
					return nil, fmt.Errorf("open wal snapshot next (%d): %w", idx, err)
				}
				if err := w.snapshotAdder(b); err != nil {
					return nil, fmt.Errorf("open wal snapshot add (%d): %w", idx, err)
				}
			}
		}
	}

	var jNames []string
	for _, e := range journalEntries[jStart:] {
		jNames = append(jNames, e.name)
	}

	// If we have a snapshot, then it is expected that the next journal file in
	// line has a name that is exactly 1 more than the index in the snapshot
	// name. Thus, the snapshot is named after the most recent journal's final
	// good index.
	w.nextIndex = 0
	if snapshotEntry.name != "" {
		_, idx, err := ParseIndexName(snapshotEntry.name)
		if err != nil {
			return nil, fmt.Errorf("open wal snapshot parse: %w", err)
		}
		w.nextIndex = idx + 1
	}

	// Note that we don't use the files iterator and multi unstuffer because
	// we need filenames all the way along the process, to check that indices match.
	// So we implement some of the loops here by hand instead.
	for _, name := range jNames {
		w.currCount = 0
		if !w.allowEmptyPlayer && w.journalPlayer == nil {
			return nil, fmt.Errorf("open wal: journal files found but no journal player option given")
		}
		f, err := fsys.Open(name)
		if err != nil {
			return nil, fmt.Errorf("open wal file: %w", err)
		}
		fi, err := f.Stat()
		if err != nil {
			return nil, fmt.Errorf("open wal stat: %w", err)
		}
		w.currSize = fi.Size()

		u := stuffedio.NewUnstuffer(f).WAL()
		defer u.Close()

		checked := false
		for !u.Done() {
			idx, b, err := u.Next()
			if err != nil {
				return nil, fmt.Errorf("open wal next: %w", err)
			}
			if w.nextIndex == 0 {
				w.nextIndex = idx
			}
			if w.nextIndex != idx {
				return nil, fmt.Errorf("open wal next: want index %d, got %d", w.nextIndex, idx)
			}
			w.nextIndex++
			w.currCount++
			if !checked {
				checked = true
				if err := CheckIndexName(name, w.journalBase, idx); err != nil {
					return nil, fmt.Errorf("open wal check: %w", err)
				}
			}
			if w.journalPlayer != nil {
				if err := w.journalPlayer(b); err != nil {
					return nil, fmt.Errorf("open wal play: %w", err)
				}
			}
		}
	}

	// If nothing was read, set up for a correct first index.
	if w.nextIndex == 0 {
		w.nextIndex = 1
	}

	if !w.allowWrite {
		// Read-only - don't open the last file for writing.
		return w, nil
	}

	if len(jNames) == 0 || w.timeToRotate() {
		if err := w.rotate(); err != nil {
			return nil, fmt.Errorf("open wal new journal: %w", err)
		}
		return w, nil
	}

	// Room left in last file, allow append there.
	f, err := os.OpenFile(filepath.Join(w.dir, jNames[len(jNames)-1]), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("open wal for append: %w", err)
	}
	w.currStuffer = stuffedio.NewStuffer(f).WAL(stuffedio.WithFirstIndex(w.nextIndex))

	return w, nil
}

// Append sends another record to the journal, and can trigger rotation of underlying files.
func (w *WAL) Append(b []byte) error {
	if !w.allowWrite {
		return fmt.Errorf("wal append: not opened for appending, read-only")
	}
	if w.currStuffer == nil {
		return fmt.Errorf("wal append: no current journal stuffer")
	}
	if w.timeToRotate() {
		if err := w.rotate(); err != nil {
			return fmt.Errorf("append rotate if ready: %v", err)
		}
	}
	n, err := w.currStuffer.Append(w.nextIndex, b)
	if err != nil {
		return fmt.Errorf("wal append: %w", err)
	}
	w.currSize += int64(n)
	w.currCount++
	w.nextIndex++
	return nil
}

// CurrIndex returns index number for the most recently read (or written) journal entry.
func (w *WAL) CurrIndex() uint64 {
	if w.nextIndex == 0 {
		return 0
	}
	return w.nextIndex - 1
}

// Close cleans up any open resources.
func (w *WAL) Close() error {
	if w.currStuffer != nil {
		err := w.currStuffer.Close()
		w.currStuffer = nil
		return err
	}
	return nil
}

// timeToRotate returns whether we are due for a rotation.
func (w *WAL) timeToRotate() (yes bool) {
	if w.currSize >= w.maxJournalBytes {
		return true
	}
	if w.currCount >= w.maxJournalIndices {
		return true
	}
	return false
}

// rotate performs a file rotation, closing the current stuffer and opening a
// new one over a new file, if possible.
func (w *WAL) rotate() error {
	if !w.allowWrite {
		return fmt.Errorf("wal rotate: not opened for append, read-only")
	}
	defer func() {
		w.currCount = 0
		w.currSize = 0
	}()
	// Close current.
	if w.currStuffer != nil {
		err := w.currStuffer.Close()
		w.currStuffer = nil
		if err != nil {
			return fmt.Errorf("rotate: %w", err)
		}
	}
	// Open new.
	f, err := os.Create(filepath.Join(w.dir, IndexName(w.journalBase, w.nextIndex)))
	if err != nil {
		return fmt.Errorf("rotate: %w", err)
	}
	w.currStuffer = stuffedio.NewStuffer(f).WAL(
		stuffedio.WithFirstIndex(w.nextIndex),
	)
	return nil
}

// IndexName returns a string for the given index value.
func IndexName(base string, idx uint64) string {
	return fmt.Sprintf("%016x-%s", idx, base)
}

// CheckIndexName checkes that the given file name contains the right base and index.
func CheckIndexName(name, base string, index uint64) error {
	p, i, err := ParseIndexName(name)
	if err != nil {
		return fmt.Errorf("check index name %q: %w", name, err)
	}
	if i != index {
		return fmt.Errorf("check index name %q: data index is %d, but filename index is %d", name, index, i)
	}
	if p != base {
		return fmt.Errorf("check index name %q: desired base %q, but filename base is %q", name, base, p)
	}
	return nil
}

// ParseIndexName pulls the index from a file name. Should not have path components.
func ParseIndexName(name string) (base string, index uint64, err error) {
	groups := indexPattern.FindStringSubmatch(name)
	if len(groups) == 0 {
		return "", 0, fmt.Errorf("parse name: no match for %q", name)
	}
	idxStr, base := groups[1], groups[2]
	idx, err := strconv.ParseUint(idxStr, 16, 64)
	if err != nil {
		return "", 0, fmt.Errorf("parse name: %w", err)
	}
	return base, idx, nil
}

// CreateSnapshot creates a snapshot stuffer for writing.
func CreateSnapshot(dir, base string, idx uint64) (*stuffedio.WALStuffer, error) {
	baseName := IndexName(base, idx)
	partialName := "partial." + baseName
	name := filepath.Join(dir, partialName)
	f, err := os.OpenFile(name, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("create snapshot: %w", err)
	}
	w := stuffedio.NewStuffer(f).WAL()
	w.RegisterClose(func() error {
		if err := os.Rename(name, filepath.Join(dir, baseName)); err != nil {
			return fmt.Errorf("create snapshot rename: %w", err)
		}
		return nil
	})
	return w, nil
}
