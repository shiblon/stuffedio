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
func WithSnapshotAdder(a Adder) Option {
	return func(w *WAL) {
		w.snapshotAdder = a
	}
}

// WithJournalPlayer sets the journal player for all journal records after the
// latest snapshot. Clients provide this to allow journals to be replayed.
func WithJournalPlayer(p Player) Option {
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

	snapshotAdder Adder
	journalPlayer Player

	currSize    int64                 // Current size of current journal file.
	currCount   int                   // Current number of indices in current journal file.
	nextIndex   uint64                // Keep track of what the next record's index should be.
	currStuffer *stuffedio.WALStuffer // The current stuffer, can be rotated on write.
}

// Open opens a directory and loads the WAL found in it, then provides a WAL
// that can be appended to over time.
func Open(dir string, loader Adder, replayer Player, opts ...Option) (*WAL, error) {
	w := &WAL{
		dir:           dir,
		snapshotAdder: loader,
		journalPlayer: replayer,

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
			if err := w.snapshotAdder.Add(b); err != nil {
				return nil, fmt.Errorf("open wal snapshot add (%d): %w", idx, err)
			}
		}
	}

	jNames = jNames[jStart:]

	var (
		finalSize int64  // last file's size
		lastStart uint64 // last start index
		lastFinal uint64 // last final index
		finalName string // last file's name
	)
	if len(jNames) != 0 {
		if w.journalPlayer == nil {
			return nil, fmt.Errorf("open wal: journal files found but no journal player option given")
		}
		var (
			prevConsumed int64
			newFileName  string
		)
		filesIter := stuffedio.NewFilesUnstufferIterator(fsys, jNames)
		jUnstuffer := stuffedio.NewMultiUnstufferIter(filesIter)
		jWALReader := stuffedio.NewWALUnstuffer(jUnstuffer)

		defer jWALReader.Close()

		// Keep track of how much was consumed when the previous file
		// finished, so we can calculate the final file's size.
		// Also track the expected index when a new file starts (which will be
		// the previous one-past-end index).
		filesIter.RegisterOnStart(func(name string, f fs.File) error {
			prevConsumed = jUnstuffer.Consumed()
			newFileName = name // save for quick index check
			finalName = name   // save for much later
			return nil
		})

		for !jWALReader.Done() {
			idx, b, err := jWALReader.Next()
			if err != nil {
				return nil, fmt.Errorf("open wal journal next (%d): %w", idx, err)
			}
			if newFileName != "" {
				if err := CheckIndexName(newFileName, w.journalPrefix, idx); err != nil {
					return nil, fmt.Errorf("open wal check journal name: %w", err)
				}
				lastStart = idx  // remember the first index for this file.
				newFileName = "" // don't remember it next time around.
			}
			lastFinal = idx
			if err := w.journalPlayer.Play(b); err != nil {
				return nil, fmt.Errorf("open wal journal play (%d): %w", idx, err)
			}
		}

		// It's an error if we never got a final file name and there are names present.
		if finalName == "" {
			return nil, fmt.Errorf("open wal unexpected condition: no final journal name set when iterating over journal names %q", jNames)
		}

		// Compute the file size and number of records in the last journal.
		finalSize = jUnstuffer.Consumed() - prevConsumed
	}

	// Stats for the current journal file.
	w.currSize = finalSize
	if lastStart != 0 && lastFinal >= lastStart {
		w.currCount = int(lastFinal - lastStart + 1)
	}
	w.nextIndex = lastFinal + 1

	if finalName == "" || w.timeToRotate() {
		if err := w.rotate(); err != nil {
			return nil, fmt.Errorf("open wal new journal: %w", err)
		}
	} else {
		// Can append to last file found.
		f, err := os.OpenFile(filepath.Join(w.dir, finalName), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return nil, fmt.Errorf("open wal for append: %w", err)
		}
		w.currStuffer = stuffedio.NewStuffer(f).WAL(stuffedio.WithFirstIndex(w.nextIndex))
	}

	return w, nil
}

// Append sends another record to the journal, and can trigger rotation of underlying files.
func (w *WAL) Append(b []byte) error {
	if w.currStuffer == nil {
		return fmt.Errorf("append: no current journal stuffer")
	}
	if w.timeToRotate() {
		if err := w.rotate(); err != nil {
			return fmt.Errorf("append rotate if ready: %v", err)
		}
	}
	n, err := w.currStuffer.Append(w.nextIndex, b)
	if err != nil {
		return fmt.Errorf("append: %w", err)
	}
	w.currSize += int64(n)
	w.nextIndex++
	return nil
}

// timeToRotate returns whether we are due for a rotation.
func (w *WAL) timeToRotate() bool {
	if w.currSize > w.maxJournalBytes {
		return true
	}
	if w.currCount > w.maxJournalIndices {
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
		s := w.currStuffer
		w.currStuffer = nil
		if err := s.Close(); err != nil {
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
		return fmt.Errorf("check index name %q: want index %d, got %d", name, index, i)
	}
	if p != prefix {
		return fmt.Errorf("check index name %q: want prefix %q, got %q", name, prefix, p)
	}
	return nil
}

// ParseIndexName pulls the index from a file name. Should not have path components.
func ParseIndexName(name string) (prefix string, index uint64, err error) {
	groups := indexPattern.FindStringSubmatch(name)
	if len(groups) == 0 {
		return "", 0, fmt.Errorf("parse name: no match for %q", name)
	}
	prefix, idxStr := groups[0], groups[1]
	idx, err := strconv.ParseUint(idxStr, 16, 64)
	if err != nil {
		return "", 0, fmt.Errorf("parse name: %w", err)
	}
	return prefix, idx, nil
}

// Adder should be passed in for snapshotting. It receives one Add call
// for every record in the snapshot. If Add returns an error, the snapshot
// process will halt and the WAL will stop trying to load.
type Adder interface {
	Add([]byte) error
}

// Player is passed into a new WAL to receive play events as journal
// entries are processed. This allows clients to implement the actual journal
// replay logic given valid bytes from the journal. Errors cause loading to
// stop and fail.
type Player interface {
	Play([]byte) error
}
