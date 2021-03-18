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

// WALUnstuffer is a write-ahead log reader implemented over a word-stuffed log.
// The WAL concept adds a couple of important bits of data on top of the
// unopinionated word-stuffing reader to ensure that records are checksummed
// and ordered.
//
// In particular, it knows how to skip truly duplicate entries (with the same
// index and checksum) where possibly only the last is not corrupt, and it
// knows that record indices should be both compact and monotonic.
type WALUnstuffer struct {
	src        RecordIterator
	buf        []byte
	nextIndex  uint64
	descending bool
}

// WALUnstufferOption defines options for write-ahead log readers.
type WALUnstufferOption func(w *WALUnstuffer)

// ExpectFirstIndex sets the expected initial index for this reader. A value of
// zero (the default) indicates that any initial value is allowed, which
// reduces its ability to check that you are reading from the expected file.
func ExpectFirstIndex(index uint64) WALUnstufferOption {
	return func(r *WALUnstuffer) {
		r.nextIndex = index
	}
}

// ExpectDescending tells the unstuffer to expect indices to be in strictly
// descending order, rather than ascending.
func ExpectDescending() WALUnstufferOption {
	return func(r *WALUnstuffer) {
		r.descending = true
	}
}

// NewWALUnstuffer creates a WALUnstuffer around the given record iterator, for
// example, an Unstuffer from this package.
func NewWALUnstuffer(iter RecordIterator, opts ...WALUnstufferOption) *WALUnstuffer {
	r := &WALUnstuffer{
		src: iter,
	}

	for _, opt := range opts {
		opt(r)
	}

	return r
}

// unconditional read-ahead on the internal buffer.
func (r *WALUnstuffer) readNext() ([]byte, error) {
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
func (r *WALUnstuffer) Next() (uint64, []byte, error) {
	if r.Done() {
		return 0, nil, io.EOF
	}
	defer func() {
		if r.descending {
			r.nextIndex--
		} else {
			r.nextIndex++
		}
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
func (r *WALUnstuffer) Done() bool {
	return r.src.Done() && len(r.buf) == 0
}

// Close closes underlying implementations if they are io.Closers.
func (r *WALUnstuffer) Close() error {
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

// WALStuffer implements a write-ahead log around an AppendCloser (like a
// Stuffer from this package). It requires every appended entry to have an
// incremented index specified, and it checksums data before stuffing it into
// the log, allowing a WALUnstuffer to detect accidental corruption.
type WALStuffer struct {
	dest      AppendCloser
	nextIndex uint64
}

// WALStufferOption specifies options for the write-ahead log writer.
type WALStufferOption func(*WALStuffer)

// WithFirstIndex sets the first index for this writer. Use this to start
// appending to an existing log at the proper index (which must be the next
// one). Default is 0, indicating that any first index will work.
func WithFirstIndex(idx uint64) WALStufferOption {
	return func(w *WALStuffer) {
		w.nextIndex = idx
	}
}

// NewWALStuffer creates a WALStuffer around the given record appender (for
// example, a Stuffer from this package).
func NewWALStuffer(a AppendCloser, opts ...WALStufferOption) *WALStuffer {
	w := &WALStuffer{
		dest: a,
	}
	for _, opt := range opts {
		opt(w)
	}

	return w
}

// Append writes a new log entry into the WAL.
func (w *WALStuffer) Append(index uint64, p []byte) error {
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
func (w *WALStuffer) Close() error {
	return w.dest.Close()
}

// NextIndex returns the next expected write index for this writer.
func (w *WALStuffer) NextIndex() uint64 {
	return w.nextIndex
}

// WAL returns a write-ahead log reader based on this stuffed log.
func (r *Unstuffer) WAL(opts ...WALUnstufferOption) *WALUnstuffer {
	return NewWALUnstuffer(r, opts...)
}

// WAL returns a write-ahead log writer on top of a stuffed log.
func (w *Stuffer) WAL(opts ...WALStufferOption) *WALStuffer {
	return NewWALStuffer(w, opts...)
}

// WAL produces a write-ahead log over top of this multi reader.
func (r *MultiUnstuffer) WAL(opts ...WALUnstufferOption) *WALUnstuffer {
	return NewWALUnstuffer(r, opts...)
}

// WAL produces a write-ahead log over a reversed unstuffer.
func (r *ReverseUnstuffer) WAL(opts ...WALUnstufferOption) *WALUnstuffer {
	opts = append(opts, ExpectDescending())
	return NewWALUnstuffer(r, opts...)
}
