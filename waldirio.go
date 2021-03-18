package stuffedio

import (
	"fmt"
	"io/fs"
	"regexp"
	"sort"
	"strconv"
)

const (
	defaultPrefix = "wal"
)

var (
	indexPattern = regexp.MustCompile(`^(.+)-([a-fA-F0-9]+)$`)
)

// WALDirReaderOption defines an option for creating a dir-based write-ahead log.
type WALDirReaderOption func(*WALDirReader)

// WithPrefix sets the filename prefix for a WALDirReader. Otherwise it has a
// sensible default. It is invalid (and silently ignored) to set the prefix to
// an empty string.
func WithPrefix(p string) WALDirReaderOption {
	return func(w *WALDirReader) {
		if p == "" {
			return
		}
		w.prefix = p
	}
}

// WALDirReader is a write-ahead log reader that works over specifically-named files in a directory.
type WALDirReader struct {
	fsys       fs.FS
	prefix     string
	readerImpl *WALReader
}

// NewWALDirReader creates a new write-ahead log accessor, given a particular
// directory on a file system. Note that if no directory is specified, one is
// created in the temp file system, and the location can be obtained from the
// Dir method.
//
// Implements the RecordIterator and RecordAppender interfaces.
func NewWALDirReader(fsys fs.FS, opts ...WALDirReaderOption) (*WALDirReader, error) {
	w := &WALDirReader{
		fsys:   fsys,
		prefix: defaultPrefix,
	}

	for _, opt := range opts {
		opt(w)
	}

	names, err := FilesWithPattern(fsys, w.Pattern())
	if err != nil {
		return nil, fmt.Errorf("open wal dir: %w", err)
	}
	if len(names) == 0 {
		return nil, fmt.Errorf("open wal dir: no journal files found")
	}

	w.readerImpl = NewMultiReaderIter(NewFilesReaderIterator(fsys, names)).WAL()
	return w, nil
}

// Pattern returns a glob pattern that identifies files in this file system
// that are relevant.
func (w *WALDirReader) Pattern() string {
	return w.prefix + "-*"
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
	return w.readerImpl.Close()
}

// FilesWithPattern finds files in the given file system
func FilesWithPattern(fsys fs.FS, pattern string) ([]string, error) {
	matches, err := fs.Glob(fsys, pattern)
	if err != nil {
		return nil, fmt.Errorf("find pattern %q: %w", pattern, err)
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
