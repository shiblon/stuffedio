package stuffedio

import (
	"fmt"
	"io"
	"io/fs"
	"regexp"
	"strconv"
	"strings"
)

var (
	indexPattern = regexp.MustCompile(`^(.+)-([a-fA-F0-9]+)$`)
)

// UnstufferIterator is an iterator over Unstuffers, for use with the MultiUnstuffer.
type UnstufferIterator interface {
	io.Closer

	Next() (*Unstuffer, error)
	Done() bool
}

// MultiUnstuffer wraps multiple readers, either specified directly or through a
// reader iterator interface that can produce them on demanad. This is useful
// when concatenating multiple file shards together as a single record store.
type MultiUnstuffer struct {
	readers    []*Unstuffer
	readerIter UnstufferIterator

	reader *Unstuffer
}

// NewMultiUnstuffer creates a MultiUnstuffer from a slice of readers. These are
// ordered, and will be consumed in the order given.
func NewMultiUnstuffer(readers []*Unstuffer) *MultiUnstuffer {
	return &MultiUnstuffer{
		readers: readers,
	}
}

// NewMultiUnstufferIter creates a MultiUnstuffer where each reader is requested, one
// at a time, through the given reader-returning function. The function is
// expected to return a nil Unstuffer if there are no more readers.
func NewMultiUnstufferIter(ri UnstufferIterator) *MultiUnstuffer {
	return &MultiUnstuffer{
		readerIter: ri,
	}
}

// ensureUnstuffer makes sure that there is a current reader available that isn't
// exhausted, if possible.
func (r *MultiUnstuffer) ensureUnstuffer() error {
	// Current and not exhausted.
	if r.reader != nil {
		if !r.reader.Done() {
			return nil
		}
		// Exhausted, close it.
		r.reader.Close()
		r.reader = nil
	}
	// If we get here, the reader is either nil or finished. Create a new one.

	// Try the function.
	if r.readerIter != nil && !r.readerIter.Done() {
		var err error
		if r.reader, err = r.readerIter.Next(); err != nil {
			return fmt.Errorf("ensure reader: %w", err)
		}
		return nil
	}

	// Try the list.
	if len(r.readers) == 0 {
		return io.EOF
	}
	r.reader = r.readers[0]
	r.readers = r.readers[1:]
	return nil
}

// Next gets the next record for these readers.
func (r *MultiUnstuffer) Next() ([]byte, error) {
	if err := r.ensureUnstuffer(); err != nil {
		return nil, fmt.Errorf("multi next: %w", err)
	}
	return r.reader.Next()
}

// Done returns whether this multi reader has exhausted all underlying readers.
func (r *MultiUnstuffer) Done() bool {
	if r.readerIter == nil && len(r.readers) == 0 {
		return true
	}
	// Current reader, not exhausted.
	if r.reader != nil && !r.reader.Done() {
		return false
	}
	// Not finished with the list.
	if len(r.readers) != 0 {
		return false
	}
	// Not finished with the reader iterator.
	if r.readerIter != nil && !r.readerIter.Done() {
		return false
	}
	return true
}

// Close closes the currently busy underlying reader and reader iterator, if any.
func (r *MultiUnstuffer) Close() error {
	defer func() {
		r.reader = nil
		r.readers = nil
		r.readerIter = nil
	}()

	var msgs []string
	if err := r.readerIter.Close(); err != nil {
		msgs = append(msgs, err.Error())
	}
	if err := r.reader.Close(); err != nil {
		msgs = append(msgs, err.Error())
	}
	for _, reader := range r.readers {
		if err := reader.Close(); err != nil {
			msgs = append(msgs, err.Error())
		}
	}

	if len(msgs) == 0 {
		return nil
	}

	return fmt.Errorf("wal dir close: %v", strings.Join(msgs, " :: "))
}

// FilesUnstufferIterator is an iterator over readers based on a list of file names.
type FilesUnstufferIterator struct {
	fsys     fs.FS
	names    []string
	file     io.ReadCloser
	nextName int
}

// NewFilesUnstufferIterator creates a new iterator from a list of file names.
func NewFilesUnstufferIterator(fsys fs.FS, names []string) *FilesUnstufferIterator {
	return &FilesUnstufferIterator{
		names: names,
		fsys:  fsys,
	}
}

// Next returns a new reader if possible, or io.EOF.
func (r *FilesUnstufferIterator) Next() (*Unstuffer, error) {
	if r.Done() {
		return nil, io.EOF
	}
	defer func() {
		r.nextName++
	}()

	if r.file != nil {
		r.file.Close()
	}

	f, err := r.fsys.Open(r.names[r.nextName])
	if err != nil {
		return nil, fmt.Errorf("next reader file: %w", err)
	}
	r.file = f
	return NewUnstuffer(f), nil
}

// Done returns true iff there are no more readers to produce.
func (r *FilesUnstufferIterator) Done() bool {
	return r.nextName >= len(r.names)
}

// Close closes the last file, if there is one open, and makes this return io.EOF ever after.
func (r *FilesUnstufferIterator) Close() error {
	r.nextName = len(r.names)
	if r.file != nil {
		return r.file.Close()
	}
	return nil
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