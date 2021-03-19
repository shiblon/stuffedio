// Package stuffedio implements a straightforward self-synchronizing log using
// consistent-overhead word stuffing (yes, COWS) as described in Paul Khuong's
// https://www.pvk.ca/Blog/2021/01/11/stuff-your-logs/.
//
// Stuffed Logs
//
// This package contains a very simple Unstuffer and Stuffer that can be used
// to write delimited records into a log. It has no opinions about the content
// of those records.
//
// Thus, you can write arbitrary bytes to a Stuffer and it will delimit them
// appropriately, and in a way that---by construction---guarantees that the
// delimiter will not appear anywhere in the record. This makes it a
// self-synchronizing file format: you can always find the next record by
// searching for the delimiter.
//
// Using the Unstuffer/Stuffer interface, of course, does not require understanding
// what it is doing underneath. If you want to write to a file, you can simply
// open it for appending and wrap it in a Stuffer:
//
//   f, err := os.OpenFile(mypath, os.RDWR|os.CREATE|os.APPEND, 0755)
//   if err != nil {
//     log.Fatalf("Error opening: %v", err)
//   }
//   s := NewStuffer(f)
//   defer s.Close()
//
//   msgs := []string{
//     "msg 1",
//     "msg 2",
//     "msg 3",
//   }
//
//   for _, msg := range msgs {
//     if err := s.Append(msg); err != nil {
//       log.Fatalf("Error appending: %v", err)
//     }
//   }
//
// Reading from an existing log is similarly simple:
//
//   f, err := os.Open(mypath)
//   if err != nil {
//     log.Fatalf("Error opening: %v", err)
//   }
//   u := NewUnstuffer(f)
//   defer u.Close()
//
//   for !u.Done() {
//     b, err := u.Next()
//     if err != nil {
//       log.Fatalf("Error reading: %v", err)
//     }
//     fmt.Println(string(b))
//   }
//
// Sharded Parallel Reads
//
// The interfaces here are explicitly designed to allow many of the use cases
// outlined in the article above, including direct support of parallel sharded
// reads.
//
// To manage sharded reads, you might structure code something like this.
//
//   func processShard(r io.Reader, nominalLength int) error {
//     u := NewUnstuffer(r)
//     // If we're in the middle of a record, skip to the next full one.
//     if err := u.SkipPartial(); err != nil {
//       return fmt.Errorf("process shard skip: %w", err)
//     }
//     // Read until finished or until we exceed the shard length.
//     // The final record inside the shard is likely going to extend
//     // past the length a little, which is fine.
//     for !u.Done() && u.Consumed() < nominalLength {
//       b, err := u.Next()
//       if err != nil {
//         return fmt.Errorf("process shard next: %w", err)
//       }
//       // PROCESS b HERE
//     }
//   }
//
//   func main() {
//     const (
//       path = "/path/to/log"
//       shards = 2
//     )
//     stat, err := os.Stat(path)
//     if err != nil {
//       log.Fatalf("Can't stat %q: %v", path, err)
//     }
//     shardSize := stat.Size() / shards
//
//     g, ctx := errgroup.WithContext(context.Background())
//
//     for i := 0; i < shards; i++ {
//       i := i // local for use in closures.
//       g.Go(func() error {
//         f, err := os.Open(path)
//         if err != nil {
//           return fmt.Errorf("open: %w", err)
//         }
//         seekPos := i*shardLength
//         if _, err := f.Seek(seekPos, os.SEEK_SET); err != nil {
//           return fmt.Errorf("seek: %w", err)
//         }
//         size := shardLength
//         if i == shards-1 {
//           size := stat.Size - seekPos
//         }
//         processShard(f, size)
//       })
//     }
//
//     if err := g.Wait(); err != nil {
//       log.Fatal(err)
//     }
//   }
//
// The key idea in the above code is that the Consumed method returns how many
// actual underlying bytes have contributed to record output thus far. When
// more than the shard length has been consumed, that shard is finished. Simply
// opening the file multiple times for reading and seeking provides the
// appropriate io.Reader interface for each shard.
//
// Write-Ahead Logs
//
// The package also contains a write-ahead log implementation, embodied in the WALUnstuffer
// and WALStuffer types. These can wrap stuffed readers and writers (or other
// kinds of readers and writers if they satisfy the proper interface).
//
// They are, like the simpler Unstuffer/Stuffer types, straightforward to use.
// However, they require an index (ordinal) to be passed for each entry when
// writing, and these indices are returned when reading.
//
// Additionally, each record has a checksum associated with it, and during
// reads, those checksums are checked.
//
// Repeated indices are allowed, with only the first valid record for that
// indicdes being returned on read.
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
//	s := NewStuffer(buf).WAL()
//
//	// Write messages.
//	msgs := []string{
//		"This is a message",
//		"This is another message",
//		"And here's a third",
//	}
//
//	for i, msg := range msgs {
//		if err := s.Append(uint64(i)+1, []byte(msg)); err != nil {
//			log.Fatalf("Append error: %v", err)
//		}
//	}
//
//	// Now read them back.
//	u := NewUnstuffer(buf).WAL()
//	defer u.Close()
//	for !u.Done() {
//		idx, val, err := u.Next()
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
// MultiUnstuffer
//
// If you wish to implement, say, a write-ahead log over multiple ordered
// readers (effectively concatenating them), there is a MultiUnstuffer
// implementation contained here. There is also a handy file iterator that can
// be used to provide on-demand file opening for the MultiUnstuffer.
//
// The files themselves are packages into simple Unstuffer types, and then the
// WALUnstuffer can be used on top of that, preserving all of the WAL logic over
// top of a concatenated set of stuffed readers.
package stuffedio // import "entrogo.com/stuffedio"

import (
	"bytes"
	"fmt"
	"io"
)

var (
	reserved = []byte{0xfe, 0xfd}
)

const (
	radix      int = 0xfd
	smallLimit int = 0xfc
	largeLimit int = radix*radix - 1
)

var (
	// CorruptRecord errors are returned (wrapped, use errors.Is to detect)
	// when a situation is encountered that can't happen in a clean record.
	// It is usually safe to skip after receiving this error, provided that a
	// missing entry doesn't cause consistency issues for the reader.
	CorruptRecord = fmt.Errorf("corrupt record")
)

// Stuffer wraps an underlying writer and appends records to the stream when
// requested, encoding them using constant-overhead word stuffing.
type Stuffer struct {
	dest io.Writer
}

// NewStuffer creates a new Stuffer with the underlying output writer.
func NewStuffer(dest io.Writer) *Stuffer {
	return &Stuffer{
		dest: dest,
	}
}

func isDelimiter(b []byte, pos int) bool {
	if pos > len(b)-len(reserved) {
		return false
	}
	return bytes.Equal(b[pos:pos+len(reserved)], reserved)
}

func findReserved(p []byte, end int) (int, bool) {
	if end > len(p) {
		end = len(p)
	}
	if end < len(reserved) {
		return end, false
	}
	// Check up to the penultimate position (two-byte check).
	for i := 0; i < end-len(reserved)+1; i++ {
		if isDelimiter(p, i) {
			return i, true
		}
	}
	return end, false
}

// Append adds a record to the end of the underlying writer. It encodes it
// using word stuffing.
func (s *Stuffer) Append(p []byte) error {
	if len(p) == 0 {
		return nil
	}

	// Always start with the delimiter.
	buf := bytes.NewBuffer(reserved)

	// First block is small, try to find the reserved sequence in the first smallLimit bytes.
	// Format that block as |reserved 0|reserved 1|length|actual bytes...|.
	// Note that nowhere in these bytes can the reserved sequence fully appear (by construction).
	end, foundReserved := findReserved(p, smallLimit)

	// Add the size and data.
	rec := append(make([]byte, 0, 1+end), byte(end))
	rec = append(rec, p[:end]...)
	if _, err := buf.Write(rec); err != nil {
		return fmt.Errorf("write rec: %w", err)
	}

	// Set the starting point for the next rounds. If we found a delimiter, we
	// need to advance past it first.
	if foundReserved {
		end += len(reserved)
	}

	// The next blocks are larger, up to largeLimit bytes each. Find the
	// reserved sequence if it's in there.
	// Format each block as |len1|len2|actual bytes...|.

	p = p[end:]
	for len(p) != 0 {
		end, foundReserved = findReserved(p, largeLimit)
		// Little-Endian length.
		len1 := end % radix
		len2 := end / radix
		rec := append(make([]byte, 0, 2+end), byte(len1), byte(len2))
		rec = append(rec, p[:end]...)
		if _, err := buf.Write(rec); err != nil {
			return fmt.Errorf("write rec: %w", err)
		}

		if foundReserved {
			end += len(reserved)
		}
		p = p[end:]
	}
	// If the last pass through found a reserved sequence, then that means
	// it _ended_ with a reserved sequence. That means we need an empty record to terminate.
	// Empty records indicate "the whole thing was a delimiter" (zero
	// non-delimiter bytes, which is less than the record max).
	if foundReserved {
		if _, err := buf.Write([]byte{0, 0}); err != nil {
			return fmt.Errorf("write rec: %w", err)
		}
	}
	if _, err := io.Copy(s.dest, buf); err != nil {
		return fmt.Errorf("write rec: %w", err)
	}
	return nil
}

// Close cleans up the underlying streams. If the underlying stream is also an io.Closer, it will close it.
func (s *Stuffer) Close() error {
	if c, ok := s.dest.(io.Closer); ok {
		return c.Close()
	}
	return nil
}

// Unstuffer wraps an io.Reader and allows full records to be pulled at once.
type Unstuffer struct {
	src      io.Reader
	buf      []byte
	consumed int64 // number of bytes actually consumed by the decoder.
	pos      int   // position in the unused read buffer.
	end      int   // one past the end of unused data.
	ended    bool  // EOF reached, don't read again.
}

// NewUnstuffer creates an Unstuffer from the given src, which is assumed to be
// a word-stuffed log.
func NewUnstuffer(src io.Reader) *Unstuffer {
	return &Unstuffer{
		src: src,
		buf: make([]byte, 1<<17),
	}
}

// fillBuf ensures that the internal buffer is at least half full, which is
// enough space for one short read and one long read.
func (u *Unstuffer) fillBuf() error {
	if u.ended {
		return nil // just use pos/end, no more reading.
	}
	if u.end-u.pos >= len(u.buf)/2 {
		// Don't bother filling if it's at least half full.
		// The buffer is designed to
		return nil
	}
	// Room to move, shift left.
	if u.pos != 0 {
		copy(u.buf[:], u.buf[u.pos:u.end])
	}
	u.end -= u.pos
	u.pos = 0

	// Read as much as possible.
	n, err := u.src.Read(u.buf[u.end:])
	if err != nil {
		if err != io.EOF {
			return fmt.Errorf("fill buffer: %w", err)
		}
		u.ended = true
	}
	u.end += n
	if u.end < len(u.buf) {
		// Assume a short read means there's no more data.
		u.ended = true
	}
	return nil
}

// advance moves the pos pointer forward by n bytes.
// Silently fails to move all the way if it encounters end first.
func (u *Unstuffer) advance(n int) {
	u.pos += n
	u.consumed += int64(n)
}

// bufLen indicates how many bytes are available in the buffer.
func (u *Unstuffer) bufLen() int {
	return u.end - u.pos
}

// bufData returns a slice of the buffer contents in [pos, end).
func (u *Unstuffer) bufData() []byte {
	return u.buf[u.pos:u.end]
}

// Consumed returns the number of bytes consumed from the underlying stream (not read, used).
func (u *Unstuffer) Consumed() int64 {
	return u.consumed
}

// discardLeader advances the position of the buffer, only if it contains a leading delimiter.
func (u *Unstuffer) discardLeader() bool {
	if u.end-u.pos < len(reserved) {
		return false
	}
	if bytes.Equal(reserved, u.bufData()[:len(reserved)]) {
		u.advance(len(reserved))
		return true
	}
	return false
}

func (u *Unstuffer) atDelimiter() bool {
	return isDelimiter(u.bufData(), 0)
}

// Done indicates whether the underlying stream is exhausted and all records are returned.
func (u *Unstuffer) Done() bool {
	return u.end == u.pos && u.ended
}

// Close closes the underlying stream, if it happens to implement io.Closer.
func (u *Unstuffer) Close() error {
	if c, ok := u.src.(io.Closer); ok {
		return c.Close()
	}
	return nil
}

// scanN returns at most the next n bytes, fewer if it hits the end or a delimiter.
// It conumes them from the buffer. It does not read from the source: ensure
// that the buffer is full enough to proceed before calling. It can only go up
// to the penultimate byte, to ensure that it doesn't read half a delimiter.
func (u *Unstuffer) scanN(n int) []byte {
	// Ensure that we don't go beyond the end of the buffer. The caller should
	// never ask for more than this. But it can happen if, for example, the
	// underlying stream is exhausted on a final block, with only the implicit
	// delimiter.
	if size := u.bufLen(); n > size {
		n = size
	}
	start := u.pos
	for i := 0; i < n; i++ {
		if u.atDelimiter() {
			break
		}
		u.advance(1)
	}
	return u.buf[start:u.pos]
}

// discardToDelimiter attempts to read until it finds a delimiter. Assumes that
// the buffer begins full. It may be filled again, in here.
func (u *Unstuffer) discardToDelimiter() error {
	for !u.atDelimiter() && !u.Done() {
		u.scanN(u.bufLen())
		if err := u.fillBuf(); err != nil {
			return fmt.Errorf("discard: %w", err)
		}
	}
	return nil
}

// SkipPartial moves forward through the log until it finds a delimiter, if it
// isn't already on one. Can be used, for example, to get shards started on a
// record boundary without first getting a corruption error.
func (u *Unstuffer) SkipPartial() error {
	if u.Done() {
		return nil
	}
	if err := u.fillBuf(); err != nil {
		return fmt.Errorf("skip partial: %w", err)
	}
	if err := u.discardToDelimiter(); err != nil {
		return fmt.Errorf("skip partial: %w", err)
	}
	return nil
}

// Next returns the next record in the underying stream, or an error. It begins
// by consuming the stream until it finds a delimiter (requiring each record to
// start with one), so even if there was an error in a previous record, this
// can skip bytes until it finds a new one. It does not require the first
// record to begin with a delimiter. Returns a wrapped io.EOF when complete.
// More idiomatically, check Done after every iteration.
func (u *Unstuffer) Next() ([]byte, error) {
	buf := new(bytes.Buffer)
	if err := u.fillBuf(); err != nil {
		return nil, fmt.Errorf("next: %w", err)
	}
	if u.Done() {
		return nil, io.EOF
	}

	if !u.discardLeader() {
		// Find the first real delimiter for next time. This can help get
		// things on track after a corrupt record, or at the start of a shard
		// that comes in the middle of a record. We strictly require every
		// record to be prefixed with the delimiter, including the first,
		// allowing this logic to work properly.
		if err := u.discardToDelimiter(); err != nil {
			return nil, fmt.Errorf("next: error discarding to next delimiter after corruption: %w", err)
		}
		return nil, fmt.Errorf("next: no leading delimiter in record: %w", CorruptRecord)
	}

	// Read the first (small) section.
	b := u.scanN(1)
	if len(b) != 1 {
		return nil, fmt.Errorf("next: short read on size byte: %w", CorruptRecord)
	}
	smallSize := int(b[0])
	if smallSize > smallLimit {
		return nil, fmt.Errorf("next: short size header %d is too large: %w", smallSize, CorruptRecord)
	}

	// We keep track of whether we got a full segment. We track this for every
	// segment, overwriting it with later segments so as to get the "final
	// word" on whether the last segment was full.
	lastIsFull := smallSize == smallLimit

	// The size portion can't be part of a delimiter, because it would have
	// triggered a "too big" error. Now we scan for delimiters while reading
	// from the buffer. Technically, when everything goes well, we should
	// always read exactly the right number of bytes. But the point of this is
	// that sometimes a record will be corrupted, so we might encounter a
	// delimiter in an unexpected place, so we scan and then check the size of
	// the return value. It can be wrong. In that case, return a meaningful
	// error so the caller can decide whether to keep going with the next
	// record.
	b = u.scanN(smallSize)
	if len(b) != smallSize {
		return nil, fmt.Errorf("next: wanted short %d, got %d: %w", smallSize, len(b), CorruptRecord)
	}

	if _, err := buf.Write(b); err != nil {
		return nil, fmt.Errorf("next: %w", err)
	}
	if smallSize != smallLimit {
		// Implied delimiter in the data itself. Write the reserved word.
		if _, err := buf.Write(reserved); err != nil {
			return nil, fmt.Errorf("next: %w", err)
		}
	}

	// Now we read zero or more large sections, stopping when we hit a delimiter or the end of the input stream.
	for !u.atDelimiter() && !u.Done() {
		if err := u.fillBuf(); err != nil {
			return nil, fmt.Errorf("next: %w", err)
		}
		// Extract 2 size bytes, convert using the radix.
		b := u.scanN(2)
		if len(b) != 2 {
			return nil, fmt.Errorf("next: short read on size bytes: %w", CorruptRecord)
		}
		if int(b[0]) >= radix || int(b[1]) >= radix {
			return nil, fmt.Errorf("next: one of the two size bytes has an invalid value: %x: %w", b[0], CorruptRecord)
		}
		size := int(b[0]) + radix*int(b[1]) // little endian size, in radix base.
		if size > largeLimit {
			return nil, fmt.Errorf("next: large interior size %d: %w", size, CorruptRecord)
		}
		// New record segment, recalculate.
		lastIsFull = size == largeLimit
		b = u.scanN(size)
		if len(b) != size {
			return nil, fmt.Errorf("next: wanted long %d, got %d: %w", size, len(b), CorruptRecord)
		}
		if _, err := buf.Write(b); err != nil {
			return nil, fmt.Errorf("next: %w", err)
		}
		if size != largeLimit {
			// Implied delimiter in the data itself, append.
			if _, err := buf.Write(reserved); err != nil {
				return nil, fmt.Errorf("next: %w", err)
			}
		}
	}

	// The last block is special, because if it is short, that does *not* imply
	// that it ended with a delimiter inside it. It's just the end (and the
	// next block starting will halt the scan).
	// We remove implicit delimiters on final blocks that aren't full length,
	// but we should *not* remove the implicit delimiter on blocks that *are*
	// full length, because they never imply a delimiter in any case (full
	// length == more is on the way).
	//
	// Thus, if the last block is full length, we don't trim off the last
	// implicit delimiter bytes.
	end := buf.Len()
	if !lastIsFull {
		end -= len(reserved)
	}
	return buf.Bytes()[:end], nil
}

// ReverseUnstuffer can be used on an io.ReaderAt to read stuffed records in reverse.
// It does so by searching backwards for delimiters, and then reading them
// forward from there. Reproduces errors in the same way that a forward reader would.
type ReverseUnstuffer struct {
	src   io.ReaderAt
	rSize int64

	pos int64
}

// NewReverseUnstuffer creates a new unstuffer that works in reverse. Because
// ReaderAt doesn't supply a size, and there are no good standard interfaces to
// depend on for this, it is required to indicate how long the underlying data
// is for the io.ReaderAt. This allows the reverse reader to start at the end.
func NewReverseUnstuffer(r io.ReaderAt, size int64) *ReverseUnstuffer {
	u := &ReverseUnstuffer{
		src:   r,
		rSize: size,
		pos:   size,
	}
	return u
}

// Done indicates whether this reverse unstuffer has reached (and produced) the
// first record in the underlying reader.
func (u *ReverseUnstuffer) Done() bool {
	return u.pos == 0
}

// Next attempts to find and produce the next record in reverse in the
// underlying stream.
func (u *ReverseUnstuffer) Next() ([]byte, error) {
	if u.Done() {
		return nil, fmt.Errorf("reverse unstuff next: %w", io.EOF)
	}
	end := u.pos
	start := end
	for end >= int64(len(reserved)) {
		off := end - 1<<17
		if off < 0 {
			off = 0
		}
		r := io.NewSectionReader(u.src, off, end-off)
		buf := make([]byte, end-off)
		if _, err := r.Read(buf); err != nil {
			return nil, fmt.Errorf("reverse section read: %w", err)
		}
		// Search for the delimiter, right to left.
		// "start" and "end" are absolute positions over the entire underlying input.
		start = end
		size := len(buf)
		for i := 1; i <= size; i++ {
			if isDelimiter(buf, size-i) {
				start = end - int64(i)
				break
			}
		}
		if start != end {
			// Found it - exit and emit.
			break
		}

		end -= int64(len(buf) - len(reserved) + 1)
	}

	if end < int64(len(reserved)) {
		u.pos = 0 // can't get another record, make Done return true.
		return nil, fmt.Errorf("reverse unstuff, missing leading delimiter: %w", CorruptRecord)
	}

	// Found one, at absolute position "start". Now we try to emit that
	// record. We also keep track of where we found it, so that we can
	// search backward next time, as well.
	u.pos = start
	b, err := NewUnstuffer(io.NewSectionReader(u.src, start, u.rSize-start)).Next()
	if err != nil {
		return nil, fmt.Errorf("reverse unstuff from offset %d: %w", start, err)
	}
	return b, nil
}

// Close closes the underlying reader if it is also an io.Closer.
func (u *ReverseUnstuffer) Close() error {
	if c, ok := u.src.(io.Closer); ok {
		return c.Close()
	}
	return nil
}
