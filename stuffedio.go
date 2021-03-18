// Package stuffedio implements a straightforward self-synchronizing log using
// consistent-overhead word stuffing (yes, COWS) as described in Paul Khuong's
// https://www.pvk.ca/Blog/2021/01/11/stuff-your-logs/.
//
// Stuffed Logs
//
// This package contains a very simple stuffed-word Reader and Writer that can
// be used to write delimited records into a log. It has no opinions about the
// content of those records.
//
// Thus, you can write arbitrary bytes to a Writer and it will delimit them
// appropriately, and in a way that---by construction---guarantees that the
// delimiter will not appear anywhere in the record. This makes it a
// self-synchronizing file format: you can always find the next record by
// searching for the delimiter.
//
// Using the Reader/Writer interface, of course, does not require understanding
// what it is doing underneath. If you want to write to a file, you can simply
// open it for appending and wrap it in a Writer:
//
//   f, err := os.OpenFile(mypath, os.RDWR|os.CREATE|os.APPEND, 0755)
//   if err != nil {
//     log.Fatalf("Error opening: %v", err)
//   }
//   w := NewWriter(f)
//   defer w.Close()
//
//   msgs := []string{
//     "msg 1",
//     "msg 2",
//     "msg 3",
//   }
//
//   for _, msg := range msgs {
//     if err := w.Append(msg); err != nil {
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
//   r := NewReader(f)
//   defer r.Close()
//
//   for !r.Done() {
//     b, err := r.Next()
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
//     r := NewReader(r)
//     // If we're in the middle of a record, skip to the next full one.
//     if err := r.SkipPartial(); err != nil {
//       return fmt.Errorf("process shard skip: %w", err)
//     }
//     // Read until finished or until we exceed the shard length.
//     // The final record inside the shard is likely going to extend
//     // past the length a little, which is fine.
//     for !r.Done() && r.Consumed() < nominalLength {
//       b, err := r.Next()
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
// The package also contains a write-ahead log implementation, embodied in the WALReader
// and WALWriter types. These can wrap stuffed readers and writers (or other
// kinds of readers and writers if they satisfy the proper interface).
//
// They are, like the simpler Reader/Writer types, straightforward to use.
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
//	w := NewWriter(buf).WAL()
//
//	// Write messages.
//	msgs := []string{
//		"This is a message",
//		"This is another message",
//		"And here's a third",
//	}
//
//	for i, msg := range msgs {
//		if err := w.Append(uint64(i)+1, []byte(msg)); err != nil {
//			log.Fatalf("Append error: %v", err)
//		}
//	}
//
//	// Now read them back.
//	r := NewReader(buf).WAL()
//	defer r.Close()
//	for !r.Done() {
//		idx, val, err := r.Next()
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
// MultiReader
//
// If you wish to implement, say, a write-ahead log over multiple ordered
// readers (effectively concatenating them), there is a MultiReader
// implementation contained here. There is also a handy file iterator that can
// be used to provide on-demand file opening for the MultiReader.
//
// The files themselves are packages into simple Reader types, and then the
// WALReader can be used on top of that, preserving all of the WAL logic over
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

// Writer wraps an underlying writer and appends records to the stream when
// requested, encoding them using constant-overhead word stuffing.
type Writer struct {
	dest io.Writer
}

// NewWriter creates a new Writer with the underlying output writer.
func NewWriter(dest io.Writer) *Writer {
	return &Writer{
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
func (w *Writer) Append(p []byte) error {
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
	if _, err := io.Copy(w.dest, buf); err != nil {
		return fmt.Errorf("write rec: %w", err)
	}
	return nil
}

// Close cleans up the underlying streams. If the underlying stream is also an io.Closer, it will close it.
func (w *Writer) Close() error {
	if c, ok := w.dest.(io.Closer); ok {
		return c.Close()
	}
	return nil
}

// Reader wraps an io.Reader and allows full records to be pulled at once.
type Reader struct {
	src      io.Reader
	buf      []byte
	consumed int  // number of bytes actually consumed by the decoder.
	pos      int  // position in the unused read buffer.
	end      int  // one past the end of unused data.
	ended    bool // EOF reached, don't read again.
}

// NewReader creates a Reader from the given src, which is assumed to be
// a word-stuffed log.
func NewReader(src io.Reader) *Reader {
	return &Reader{
		src: src,
		buf: make([]byte, 1<<17),
	}
}

// fillBuf ensures that the internal buffer is at least half full, which is
// enough space for one short read and one long read.
func (r *Reader) fillBuf() error {
	if r.ended {
		return nil // just use pos/end, no more reading.
	}
	if r.end-r.pos >= len(r.buf)/2 {
		// Don't bother filling if it's at least half full.
		// The buffer is designed to
		return nil
	}
	// Room to move, shift left.
	if r.pos != 0 {
		copy(r.buf[:], r.buf[r.pos:r.end])
	}
	r.end -= r.pos
	r.pos = 0

	// Read as much as possible.
	n, err := r.src.Read(r.buf[r.end:])
	if err != nil {
		if err != io.EOF {
			return fmt.Errorf("fill buffer: %w", err)
		}
		r.ended = true
	}
	r.end += n
	if r.end < len(r.buf) {
		// Assume a short read means there's no more data.
		r.ended = true
	}
	return nil
}

// advance moves the pos pointer forward by n bytes.
// Silently fails to move all the way if it encounters end first.
func (r *Reader) advance(n int) {
	r.pos += n
	r.consumed += n
}

// bufLen indicates how many bytes are available in the buffer.
func (r *Reader) bufLen() int {
	return r.end - r.pos
}

// bufData returns a slice of the buffer contents in [pos, end).
func (r *Reader) bufData() []byte {
	return r.buf[r.pos:r.end]
}

// Consumed returns the number of bytes consumed from the underlying stream (not read, used).
func (r *Reader) Consumed() int {
	return r.consumed
}

// discardLeader advances the position of the buffer, only if it contains a leading delimiter.
func (r *Reader) discardLeader() bool {
	if r.end-r.pos < len(reserved) {
		return false
	}
	if bytes.Equal(reserved, r.bufData()[:len(reserved)]) {
		r.advance(len(reserved))
		return true
	}
	return false
}

func (r *Reader) atDelimiter() bool {
	return isDelimiter(r.bufData(), 0)
}

// Done indicates whether the underlying stream is exhausted and all records are returned.
func (r *Reader) Done() bool {
	return r.end == r.pos && r.ended
}

// Close closes the underlying stream, if it happens to implement io.Closer.
func (r *Reader) Close() error {
	if c, ok := r.src.(io.Closer); ok {
		return c.Close()
	}
	return nil
}

// scanN returns at most the next n bytes, fewer if it hits the end or a delimiter.
// It conumes them from the buffer. It does not read from the source: ensure
// that the buffer is full enough to proceed before calling. It can only go up
// to the penultimate byte, to ensure that it doesn't read half a delimiter.
func (r *Reader) scanN(n int) []byte {
	// Ensure that we don't go beyond the end of the buffer. The caller should
	// never ask for more than this. But it can happen if, for example, the
	// underlying stream is exhausted on a final block, with only the implicit
	// delimiter.
	if size := r.bufLen(); n > size {
		n = size
	}
	start := r.pos
	for i := 0; i < n; i++ {
		if r.atDelimiter() {
			break
		}
		r.advance(1)
	}
	return r.buf[start:r.pos]
}

// discardToDelimiter attempts to read until it finds a delimiter. Assumes that
// the buffer begins full. It may be filled again, in here.
func (r *Reader) discardToDelimiter() error {
	for !r.atDelimiter() && !r.Done() {
		r.scanN(r.bufLen())
		if err := r.fillBuf(); err != nil {
			return fmt.Errorf("discard: %w", err)
		}
	}
	return nil
}

// SkipPartial moves forward through the log until it finds a delimiter, if it
// isn't already on one. Can be used, for example, to get shards started on a
// record boundary without first getting a corruption error.
func (r *Reader) SkipPartial() error {
	if r.Done() {
		return nil
	}
	if err := r.fillBuf(); err != nil {
		return fmt.Errorf("skip partial: %w", err)
	}
	if err := r.discardToDelimiter(); err != nil {
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
func (r *Reader) Next() ([]byte, error) {
	buf := new(bytes.Buffer)
	if err := r.fillBuf(); err != nil {
		return nil, fmt.Errorf("next: %w", err)
	}
	if r.Done() {
		return nil, io.EOF
	}

	if !r.discardLeader() {
		// Find the first real delimiter for next time. This can help get
		// things on track after a corrupt record, or at the start of a shard
		// that comes in the middle of a record. We strictly require every
		// record to be prefixed with the delimiter, including the first,
		// allowing this logic to work properly.
		if err := r.discardToDelimiter(); err != nil {
			return nil, fmt.Errorf("next: error discarding to next delimiter after corruption: %w", err)
		}
		return nil, fmt.Errorf("next: no leading delimiter in record: %w", CorruptRecord)
	}

	// Read the first (small) section.
	b := r.scanN(1)
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
	b = r.scanN(smallSize)
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
	for !r.atDelimiter() && !r.Done() {
		if err := r.fillBuf(); err != nil {
			return nil, fmt.Errorf("next: %w", err)
		}
		// Extract 2 size bytes, convert using the radix.
		b := r.scanN(2)
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
		b = r.scanN(size)
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
