// Package orderedio implements an ordered log with explicit ordinals over top of a record Encoder/Decoder.
//
// This can be used as an implementation for a self-describing write-ahead-log.
//
// The Decoder and Encoder types can wrap stuffed recordio.Encoder and
// recordio.Decoder values.
//
// They are, like the simpler recordio Decoder/Encoder types, straightforward
// to use. However, they require an index (ordinal) to be passed for each entry
// when writing, and these indices are returned when reading.
//
// Additionally, each record has a checksum associated with it, and during
// reads, those checksums are checked.
//
// Repeated indices are allowed, with only the first valid record for that
// indices being returned on read.
//
// The first record's index must be 1 or higher. Options allow a specific
// starting index to be enforced (e.g., when the file name indicates the
// starting point of the log, one can specify that starting point when reading
// and receive an error if it is long).
//
// Indices must be in sequence, with the exception of repeats.
//
// An example of how this works is below:
//
//	buf := new(bytes.Buffer)
//	e := NewStreamEncoder(buf)
//
//	// Write messages.
//	msgs := []string{
//		"This is a message",
//		"This is another message",
//		"And here's a third",
//	}
//
//	for i, msg := range msgs {
//		if _, err := e.Encode(uint64(i)+1, []byte(msg)); err != nil {
//			log.Fatalf("Encode error: %v", err)
//		}
//	}
//
//	// Now read them back.
//	d := NewStreamDecoder(buf)
//	defer d.Close()
//	for !d.Done() {
//		idx, val, err := d.Next()
//		if err != nil {
//			log.Fatalf("Read error: %v", err)
//		}
//		fmt.Printf("%d: %q\n", idx, string(val))
//	}
//
//	// Output:
//	// 1: "This is a message"
//	// 2: "This is another message"
//	// 3: "And here's a third"
//
package orderedio // import "entrogo.com/stuffedio/orderedio"

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"

	"entrogo.com/stuffedio/recordio"
)

var CRCTable = crc32.MakeTable(crc32.Koopman)

// RecordIterator is an interface that produces a bytes iterator using Done and Next methods.
type RecordIterator interface {
	Next() ([]byte, error)
	Done() bool
}

// Decoder is a write-ahead log reader implemented over a word-stuffed log.
// The concept adds a couple of important bits of data on top of the
// unopinionated word-stuffing reader to ensure that records are checksummed
// and ordered.
//
// In particular, it knows how to skip truly duplicate entries (with the same
// index and checksum) where possibly only the last is not corrupt, and it
// knows that record indices should be both compact and monotonic.
type Decoder struct {
	src        RecordIterator
	buf        []byte
	nextIndex  uint64
	descending bool
}

// DecoderOption defines options for write-ahead log readers.
type DecoderOption func(d *Decoder)

// ExpectFirstIndex sets the expected initial index for this reader. A value of
// zero (the default) indicates that any initial value is allowed, which
// reduces its ability to check that you are reading from the expected file.
func ExpectFirstIndex(index uint64) DecoderOption {
	return func(d *Decoder) {
		d.nextIndex = index
	}
}

// ExpectDescending tells the unstuffer to expect indices to be in strictly
// descending order, rather than ascending. Default is false.
func ExpectDescending(desc bool) DecoderOption {
	return func(d *Decoder) {
		d.descending = desc
	}
}

// NewDecoder creates a Decoder around the given record iterator, for
// example, a recordio.Decoder.
func NewDecoder(iter RecordIterator, opts ...DecoderOption) *Decoder {
	d := &Decoder{
		src: iter,
	}

	for _, opt := range opts {
		opt(d)
	}

	return d
}

// NewStreamDecoder creates a Decoder over an io.Reader using the default
// recordio.Decoder as the underlying RecordIterator.
func NewStreamDecoder(r io.Reader, opts ...DecoderOption) *Decoder {
	return NewDecoder(recordio.NewDecoder(r), opts...)
}

// unconditional read-ahead on the internal buffer.
func (d *Decoder) readNext() ([]byte, error) {
	if len(d.buf) != 0 {
		b := d.buf
		d.buf = nil
		return b, nil
	}
	if d.Done() {
		return nil, fmt.Errorf("wal read next: %w", io.EOF)
	}
	b, err := d.src.Next()
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
func (d *Decoder) Next() (uint64, []byte, error) {
	if d.Done() {
		return 0, nil, io.EOF
	}
	defer func() {
		if d.descending {
			d.nextIndex--
		} else {
			d.nextIndex++
		}
	}()
	var result struct {
		val []byte
		err error
		idx uint64
	}
	for !d.Done() {
		buf, err := d.readNext()
		if err != nil {
			return 0, nil, fmt.Errorf("wal next: %w", err)
		}
		index := binary.LittleEndian.Uint64(buf[0:8])
		// Allow any next index if it's 0 (permissive initial condition).
		if d.nextIndex == 0 {
			d.nextIndex = index
		}

		if len(result.val) != 0 {
			// If we already have a result and this is a new index (handle errors
			// in index progression for this next record at the next read), then
			// emit it our existing result. We are at the end of a potential
			// run of duplicates. Leave the buffer in place for next time.
			if d.Done() || index != d.nextIndex {
				d.buf = buf
				return result.idx, result.val, result.err
			}

			// We have a successful result, we are not done, and the new index
			// is equal to our result. Discard and go around again until we've
			// exhausted this duplicate index.
			continue
		}
		if index != d.nextIndex {
			// No result yet, index is wrong for what would otherwise be the next result.
			return 0, nil, fmt.Errorf("wal next: expected index %d, got %d", d.nextIndex, index)
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
func (d *Decoder) Done() bool {
	return d.src.Done() && len(d.buf) == 0
}

// Close closes underlying implementations if they are io.Closers.
func (d *Decoder) Close() error {
	d.buf = nil
	if c, ok := d.src.(io.Closer); ok {
		return c.Close()
	}
	return nil
}

// EncodeCloser is an interface for appending records to a stream.
type EncodeCloser interface {
	io.Closer
	Encode([]byte) (int, error)
}

// Encoder implements a write-ahead log around an EncodeCloser (like an Encoder
// from the recordio package). It requires every appended entry to have an
// incremented index specified, and it checksums data before stuffing it into
// the log, allowing a Decoder to detect accidental corruption.
type Encoder struct {
	dest      EncodeCloser
	nextIndex uint64

	onClose func() error
}

// EncoderOption specifies options for the write-ahead log writer.
type EncoderOption func(*Encoder)

// WithFirstIndex sets the first index for this writer. Use this to start
// appending to an existing log at the proper index (which must be the next
// one). Default is 0, indicating that any first index will work.
func WithFirstIndex(idx uint64) EncoderOption {
	return func(e *Encoder) {
		e.nextIndex = idx
	}
}

// NewEncoder creates a Encoder around the given record appender (for
// example, a recordio.Encoder).
func NewEncoder(a EncodeCloser, opts ...EncoderOption) *Encoder {
	e := &Encoder{
		dest: a,
	}
	for _, opt := range opts {
		opt(e)
	}

	return e
}

// NewStreamEncoder creates an Encoder over an io.Writer, using the default
// recordio.Encoder as the underlying EncodeCloser.
func NewStreamEncoder(w io.Writer, opts ...EncoderOption) *Encoder {
	return NewEncoder(recordio.NewEncoder(w), opts...)
}

// Encode writes a new log entry into the WAL.
func (e *Encoder) Encode(index uint64, p []byte) (int, error) {
	if index == 0 {
		return 0, fmt.Errorf("index 0 is invalid as a starting index for a write-ahead log")
	}
	if e.nextIndex != 0 && index != e.nextIndex {
		return 0, fmt.Errorf("expected next index in sequence %d, got %d", e.nextIndex, index)
	}
	e.nextIndex = index + 1

	crc := crc32.Checksum(p, CRCTable)
	entry := make([]byte, len(p)+12)
	binary.LittleEndian.PutUint64(entry[0:8], index)
	binary.LittleEndian.PutUint32(entry[8:12], crc)
	copy(entry[12:], p)

	n, err := e.dest.Encode(entry)
	if err != nil {
		return 0, fmt.Errorf("wal append: %w", err)
	}
	return n, nil
}

// RegisterClose registers a function to call when the Encoder is closed.
// Can be used, for example, to do atomic renames when snapshotting to a WAL.
func (e *Encoder) RegisterClose(f func() error) {
	e.onClose = f
}

// Close closes underlying implementations if they are io.Closers.
func (e *Encoder) Close() error {
	err := e.dest.Close()
	if e.onClose != nil {
		if err := e.onClose(); err != nil {
			return err
		}
	}
	return err
}

// NextIndex returns the next expected write index for this writer.
func (e *Encoder) NextIndex() uint64 {
	return e.nextIndex
}
