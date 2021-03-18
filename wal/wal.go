// Package wal implements a full write-ahead log over a directory of files,
// including snapshot management, reply iterators, and rotating log writers.
package wal

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"regexp"
	"sort"
	"strconv"
)

const (
	DefaultJournalPrefix  = "j"
	DefaultSnapshotPrefix = "s"
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

// WAL implements a write-ahead logger capable of replaying snapshots and
// journals, setting up a writer for appending to journals and rotating them
// when full, etc.
type WAL struct {
	dir            string
	journalPrefix  string
	snapshotPrefix string

	snapshotAdder Adder
	journalPlayer Player
}

// Open opens a directory and loads the WAL found in it, then provides a WAL
// that can be appended to over time.
func Open(dir string, loader Adder, replayer Player, opts ...Option) (*WAL, error) {
	w := &WAL{
		dir:           dir,
		snapshotAdder: loader,
		journalPlayer: replayer,

		journalPrefix:  DefaultJournalPrefix,
		snapshotPrefix: DefaultSnapshotPrefix,
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

	// TODO: clean up any non-latest snapshots?

	jStart := 0
	for i, name := range jNames {
		if name > latestSnapshot {
			jStart = i
			break
		}
	}

	// TODO: clean up any journal files before jStart? Move them to a "please collect me" location?

	if latestSnapshot != "" {
		sReader := stuffedio.NewUnstuffer(fsys.Open(latestSnapshot)).WAL()
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
		finalSize  int64
		finalFirst uint64
		finalLast  uint64
	)
	if len(jNames) != 0 {
		f, err := fsys.Open(jNames[len(jNames)-1])
		if err != nil {
			return nil, fmt.Errorf("open wal final open: %w", err)
		}
		finalSize, finalFirst, finalLast, err = WALStats(f)
		if err != nil {
			return nil, fmt.Errorf("open wal final stats: %w", err)
		}

		jReader := stuffedio.NewMultiUnstufferIter(stuffedio.NewFileUnstufferIterator(fsys, jNames)).WAL()
		for !jReader.Done() {
			idx, b, err := jReader.Next()
			if err != nil {
				return nil, fmt.Errorf("open wal journal next (%d): %w", idx, err)
			}
			if err := w.journalPlayer.Play(b); err != nil {
				return nil, fmt.Errorf("open wal journal play (%d): %w", idx, err)
			}
		}
	}

	// TODO:
	// - use counts and size to determine whether we should create a new journal or not.
	// - get ready to write.

	return w, nil
}

// WALStats returns the size and extreme record indices for non-corrupt records
// in a stuffed WAL file. Fails if the underlying file is not an io.ReaderAt.
func WALStats(f fs.File) (size int64, first, last uint64, err error) {
	fi, err := f.Stat()
	if err != nil {
		return 0, 0, 0, fmt.Errorf("wal stats: %w", err)
	}

	r, ok := f.(io.ReaderAt)
	if !ok {
		return 0, 0, 0, fmt.Errorf("wal stats, underlying type %T is not a ReaderAt", f)
	}

	uBackward := stuffedio.NewReverseUnstuffer(r, fi.Size()).WAL()
	last, _, err = uBackward.Next()
	if err != nil {
		return 0, 0, 0, fmt.Errorf("wal stats, last: %w", err)
	}

	uForward := stuffedio.NewUnstuffer(f).WAL()
	first, _, err = uForward.Next()
	if err != nil {
		return 0, 0, 0, fmt.Errorf("wal stats, first: %w", err)
	}

	return fi.Size(), first, last, nil
}

// IndexName returns a string for the given index value.
func IndexName(prefix string, idx uint64) string {
	return fmt.Sprintf("%s-%016x", prefix, idx)
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
