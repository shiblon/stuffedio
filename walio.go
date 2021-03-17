package stuffedio

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
)

var CRCTable = crc32.MakeTable(crc32.Koopman)

// RecordIterator is an interface that produces a bytes iterator using Done and Next methods.
type RecordIterator interface {
	Next() ([]byte, error)
	Done() bool
}

// WALReader is a write-ahead log reader implemented over a word-stuffed log.
// The WAL concept adds a couple of important bits of data on top of the
// unopinionated word-stuffing reader to ensure that records are checksummed
// and ordered.
//
// In particular, it knows how to skip truly duplicate entries (with the same
// index and checksum) where possibly only the last is not corrupt, and it
// knows that record indices should be both compact and monotonic.
type WALReader struct {
	src       RecordIterator
	buf       []byte
	nextIndex uint64
}

// WALReaderOption defines options for write-ahead log readers.
type WALReaderOption func(w *WALReader)

// ExpectFirstIndex sets the expected initial index for this reader. A value of
// zero (the default) indicates that any initial value is allowed, which
// reduces its ability to check that you are reading from the expected file.
func ExpectFirstIndex(index uint64) WALReaderOption {
	return func(r *WALReader) {
		r.nextIndex = index
	}
}

// NewWALReader creates a WALReader around the given record iterator, for
// example, a Reader from this package.
func NewWALReader(iter RecordIterator, opts ...WALReaderOption) *WALReader {
	r := &WALReader{
		src: iter,
	}

	for _, opt := range opts {
		opt(r)
	}

	return r
}

// unconditional read-ahead on the internal buffer.
func (r *WALReader) readNext() ([]byte, error) {
	if len(r.buf) != 0 {
		b := r.buf
		r.buf = nil
		return b, nil
	}
	if r.Done() {
		return nil, fmt.Errorf("wal read next: %w", io.EOF)
	}
	b, err := r.src.Next()
	if err != nil {
		return nil, fmt.Errorf("wal next: %w", err)
	}
	return b, nil
}

// Next returns the next entry from the WAL, verifying its checksum and
// returning an error if it does not match, or if the ordinal value is not the
// next one in the series. It skips corrupt entries in the underlying log,
// under the assumption that they would have been retried.
//
// If the underlying stream is done, this returns io.EOF.
func (r *WALReader) Next() (uint64, []byte, error) {
	if r.Done() {
		return 0, nil, io.EOF
	}
	defer func() {
		r.nextIndex++
	}()
	var result struct {
		val []byte
		err error
		idx uint64
	}
	for !r.Done() {
		buf, err := r.readNext()
		if err != nil {
			return 0, nil, fmt.Errorf("wal next: %w", err)
		}
		index := binary.LittleEndian.Uint64(buf[0:8])
		// Allow any next index if it's 0 (permissive initial condition).
		if r.nextIndex == 0 {
			r.nextIndex = index
		}

		if len(result.val) != 0 {
			// If we already have a result and this is a new index (handle errors
			// in index progression for this next record at the next read), then
			// emit it our existing result. We are at the end of a potential
			// run of duplicates. Leave the buffer in place for next time.
			if r.Done() || index != r.nextIndex {
				r.buf = buf
				return result.idx, result.val, result.err
			}

			// We have a successful result, we are not done, and the new index
			// is equal to our result. Discard and go around again until we've
			// exhausted this duplicate index.
			continue
		}
		if index != r.nextIndex {
			// No result yet, index is wrong for what would otherwise be the next result.
			return 0, nil, fmt.Errorf("wal next: expected index %d, got %d", r.nextIndex, index)
		}

		// No result yet. Check CRC, if it checks out, store in result and go
		// around again in case there are duplicates. Note that a failed CRC is
		// not necessarily a deal-killer, there might be duplicates (retried
		// entries) that fix it. In fact, this is the most likely case for
		// duplicate entries: the last one is right. We just can't assume that.
		if stored, computed := binary.LittleEndian.Uint32(buf[8:12]), crc32.Checksum(buf[12:], CRCTable); stored != computed {
			result.idx = 0
			result.val = nil
			result.err = fmt.Errorf("wal next: crc mismatch on index %d", index)
		} else {
			result.idx = index
			result.val = buf[12:]
			result.err = nil
		}
	}
	return result.idx, result.val, result.err
}

// Done returns true when all entries have been returned by Next.
func (r *WALReader) Done() bool {
	return r.src.Done() && len(r.buf) == 0
}

// Close closes underlying implementations if they are io.Closers.
func (r *WALReader) Close() error {
	r.buf = nil
	if c, ok := r.src.(io.Closer); ok {
		return c.Close()
	}
	return nil
}

// AppendCloser is an interface for appending records to a stream.
type AppendCloser interface {
	io.Closer
	Append([]byte) error
}

// WALWriter implements a write-ahead log around an AppendCloser (like a
// Writer from this package). It requires every appended entry to have an
// incremented index specified, and it checksums data before stuffing it into
// the log, allowing a WALReader to detect accidental corruption.
type WALWriter struct {
	dest      AppendCloser
	nextIndex uint64
}

// WALWriterOption specifies options for the write-ahead log writer.
type WALWriterOption func(*WALWriter)

// WithFirstIndex sets the first index for this writer. Use this to start
// appending to an existing log at the proper index (which must be the next
// one). Default is 0, indicating that any first index will work.
func WithFirstIndex(idx uint64) WALWriterOption {
	return func(w *WALWriter) {
		w.nextIndex = idx
	}
}

// NewWALWriter creates a WALWriter around the given record appender (for
// example, a Writer from this package).
func NewWALWriter(a AppendCloser, opts ...WALWriterOption) *WALWriter {
	w := &WALWriter{
		dest: a,
	}
	for _, opt := range opts {
		opt(w)
	}

	return w
}

// Append writes a new log entry into the WAL.
func (w *WALWriter) Append(index uint64, p []byte) error {
	if index == 0 {
		return fmt.Errorf("index 0 is invalid as a starting index for a write-ahead log")
	}
	if w.nextIndex != 0 && index != w.nextIndex {
		return fmt.Errorf("expected next index in sequence %d, got %d", w.nextIndex, index)
	}
	w.nextIndex = index + 1

	crc := crc32.Checksum(p, CRCTable)
	entry := make([]byte, len(p)+12)
	binary.LittleEndian.PutUint64(entry[0:8], index)
	binary.LittleEndian.PutUint32(entry[8:12], crc)
	copy(entry[12:], p)

	if err := w.dest.Append(entry); err != nil {
		return fmt.Errorf("wal append: %w", err)
	}
	return nil
}

// Close closes underlying implementations if they are io.Closers.
func (w *WALWriter) Close() error {
	return w.dest.Close()
}

// NextIndex returns the next expected write index for this writer.
func (w *WALWriter) NextIndex() uint64 {
	return w.nextIndex
}

// WAL returns a write-ahead log reader based on this stuffed log.
func (r *Reader) WAL(opts ...WALReaderOption) *WALReader {
	return NewWALReader(r, opts...)
}

// WAL returns a write-ahead log writer on top of a stuffed log.
func (w *Writer) WAL(opts ...WALWriterOption) *WALWriter {
	return NewWALWriter(w, opts...)
}
