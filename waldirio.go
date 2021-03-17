package stuffedio

import (
	"fmt"
	"io/fs"
	"regexp"
	"sort"
	"strconv"
)

const (
	journalPrefix = "journal"
)

var (
	indexPattern = regexp.MustCompile(`^(.+)-([a-fA-F0-9]+)$`)
)

// WALDirReader is a write-ahead log reader that works over specifically-named files in a directory.
type WALDirReader struct {
	fsys       fs.FS
	readerIter *FilesReaderIterator
	readerImpl *WALReader
}

// NewWALDirReader creates a new write-ahead log accessor, given a particular
// directory on a file system. Note that if no directory is specified, one is
// created in the temp file system, and the location can be obtained from the
// Dir method.
//
// Implements the RecordIterator and RecordAppender interfaces.
func NewWALDirReader(fsys fs.FS) (*WALDirReader, error) {
	names, err := FilesWithPrefix(fsys, journalPrefix)
	if err != nil {
		return nil, fmt.Errorf("open wal dir: %w", err)
	}
	if len(names) == 0 {
		return nil, fmt.Errorf("open wal dir: no journal files found")
	}

	rIter := NewFilesReaderIterator(fsys, names)

	return &WALDirReader{
		fsys:       fsys,
		readerIter: rIter,
		readerImpl: NewMultiReaderIter(rIter).WAL(),
	}, nil
}

// Next reads the next record from the write-ahead log system of files. It can
// be used for files that are actively being written, but that's not a great
// idea in general, because you can get corrupt record errors as you read up to
// the end (there is no way of knowing that a record is just incomplete).
func (w *WALDirReader) Next() (uint64, []byte, error) {
	return w.readerImpl.Next()
}

// Done indicates whether the reader is done.
func (w *WALDirReader) Done() bool {
	return w.readerImpl.Done()
}

// Close closes any open files and invalidates this, causing to always return EOF.
func (w *WALDirReader) Close() error {
	return w.readerIter.Close()
}

// FilesWithPrefix finds files in the given file system
func FilesWithPrefix(fsys fs.FS, prefix string) ([]string, error) {
	matches, err := fs.Glob(fsys, prefix+"*")
	if err != nil {
		return nil, fmt.Errorf("find prefix %q: %w", prefix, err)
	}
	sort.Slice(matches, func(i, j int) bool {
		return matches[i] < matches[j]
	})
	return matches, nil
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
