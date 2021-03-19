// Package wal implements a full write-ahead log over a directory of files,
// including snapshot management, reply iterators, and rotating log writers.
package wal

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"

	"entrogo.com/stuffedio"
)

const (
	DefaultJournalPrefix  = "j"
	DefaultSnapshotPrefix = "s"
	DefaultMaxIndices     = 10 * 1 << 10 // 10Ki
	DefaultMaxBytes       = 10 * 1 << 20 // 10Mi
)

var (
	indexPattern = regexp.MustCompile(`^([^.]+)-([a-fA-F0-9]+)$`)
)

// Loader is a function type called by snapshot item loads and journal entry replay.
type Loader func([]byte) error

// Option describes a WAL creation option.
type Option func(*WAL)

// WithJournalPrefix sets the journal prefix, otherwise uses DefaultJournalPrefix.
func WithJournalPrefix(p string) Option {
	return func(w *WAL) {
		w.journalPrefix = p
	}
}

// WithSnaphotPrefix sets the snapshot prefix, otherwise uses DefaultSnapshotPrefix.
func WithSnapshotPrefix(p string) Option {
	return func(w *WAL) {
		w.snapshotPrefix = p
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

// WAL implements a write-ahead logger capable of replaying snapshots and
// journals, setting up a writer for appending to journals and rotating them
// when full, etc.
type WAL struct {
	dir               string
	journalPrefix     string
	snapshotPrefix    string
	maxJournalBytes   int64
	maxJournalIndices int

	snapshotAdder Loader
	journalPlayer Loader

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

		journalPrefix:     DefaultJournalPrefix,
		maxJournalBytes:   DefaultMaxBytes,
		maxJournalIndices: DefaultMaxIndices,
		snapshotPrefix:    DefaultSnapshotPrefix,
	}

	for _, opt := range opts {
		opt(w)
	}

	fsys := os.DirFS(dir)

	sNames, err := fs.Glob(fsys, w.snapshotPrefix+"-*")
	if err != nil {
		return nil, fmt.Errorf("open wal: %w", err)
	}
	sort.Strings(sNames)

	jNames, err := fs.Glob(fsys, w.journalPrefix+"-*")
	if err != nil {
		return nil, fmt.Errorf("open wal: %w", err)
	}
	sort.Strings(jNames)

	var latestSnapshot string
	if len(sNames) != 0 {
		latestSnapshot = sNames[len(sNames)-1]
	}

	// TODO: clean up any non-latest snapshots? Move to a "clean up later" location?

	jStart := 0
	for i, name := range jNames {
		if name > latestSnapshot {
			jStart = i
			break
		}
	}

	// TODO: clean up any journal files before jStart? Move them to a "please collect me" location?

	if latestSnapshot != "" {
		if w.snapshotAdder == nil {
			return nil, fmt.Errorf("open wal: snapshot found but no snapshot adder option given")
		}
		f, err := fsys.Open(latestSnapshot)
		if err != nil {
			return nil, fmt.Errorf("open wal open snapshot: %w", err)
		}
		sReader := stuffedio.NewUnstuffer(f).WAL()
		for !sReader.Done() {
			idx, b, err := sReader.Next()
			if err != nil {
				return nil, fmt.Errorf("open wal snapshot next (%d): %w", idx, err)
			}
			if err := w.snapshotAdder(b); err != nil {
				return nil, fmt.Errorf("open wal snapshot add (%d): %w", idx, err)
			}
		}
	}

	jNames = jNames[jStart:]

	// Note that we don't use the files iterator and multi unstuffer because
	// we need filenames all the way along the process, to check that indices match.
	// So we implement some of the loops here by hand instead.
	w.nextIndex = 0
	for _, name := range jNames {
		w.currCount = 0
		if w.journalPlayer == nil {
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
				if err := CheckIndexName(name, w.journalPrefix, idx); err != nil {
					return nil, fmt.Errorf("open wal check: %w", err)
				}
			}
			if err := w.journalPlayer(b); err != nil {
				return nil, fmt.Errorf("open wal play: %w", err)
			}
		}
	}

	// If nothing was read, set up for a correct first index.
	if w.nextIndex == 0 {
		w.nextIndex = 1
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
	f, err := os.Create(filepath.Join(w.dir, IndexName(w.journalPrefix, w.nextIndex)))
	if err != nil {
		return fmt.Errorf("rotate: %w", err)
	}
	w.currStuffer = stuffedio.NewStuffer(f).WAL(
		stuffedio.WithFirstIndex(w.nextIndex),
	)
	return nil
}

// IndexName returns a string for the given index value.
func IndexName(prefix string, idx uint64) string {
	return fmt.Sprintf("%s-%016x", prefix, idx)
}

// CheckIndexName checkes that the given file name contains the right prefix and index.
func CheckIndexName(name, prefix string, index uint64) error {
	p, i, err := ParseIndexName(name)
	if err != nil {
		return fmt.Errorf("check index name %q: %w", name, err)
	}
	if i != index {
		return fmt.Errorf("check index name %q: data index is %d, but filename index is %d", name, index, i)
	}
	if p != prefix {
		return fmt.Errorf("check index name %q: desired prefix %q, but filename prefix is %q", name, prefix, p)
	}
	return nil
}

// ParseIndexName pulls the index from a file name. Should not have path components.
func ParseIndexName(name string) (prefix string, index uint64, err error) {
	groups := indexPattern.FindStringSubmatch(name)
	if len(groups) == 0 {
		return "", 0, fmt.Errorf("parse name: no match for %q", name)
	}
	prefix, idxStr := groups[1], groups[2]
	idx, err := strconv.ParseUint(idxStr, 16, 64)
	if err != nil {
		return "", 0, fmt.Errorf("parse name: %w", err)
	}
	return prefix, idx, nil
}
