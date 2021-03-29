package recordio

import (
	"fmt"
	"io"
	"io/fs"
	"strings"
)

// DecoderIterator is an iterator over Decoders, for use with the MultiDecoder.
type DecoderIterator interface {
	io.Closer

	Next() (*Decoder, error)
	Done() bool
}

// MultiDecoder wraps multiple readers, either specified directly or through a
// reader iterator interface that can produce them on demanad. This is useful
// when concatenating multiple file shards together as a single record store.
type MultiDecoder struct {
	readers    []*Decoder
	readerIter DecoderIterator

	reader *Decoder

	prevConsumed int64
}

// NewMultiDecoder creates a MultiDecoder from a slice of readers. These are
// ordered, and will be consumed in the order given.
func NewMultiDecoder(readers []*Decoder) *MultiDecoder {
	return &MultiDecoder{
		readers: readers,
	}
}

// NewMultiDecoderIter creates a MultiDecoder where each reader is requested, one
// at a time, through the given reader-returning function. The function is
// expected to return a nil Decoder if there are no more readers.
func NewMultiDecoderIter(ri DecoderIterator) *MultiDecoder {
	return &MultiDecoder{
		readerIter: ri,
	}
}

// ensureDecoder makes sure that there is a current reader available that isn't
// exhausted, if possible.
func (d *MultiDecoder) ensureDecoder() error {
	// Current and not exhausted.
	if d.reader != nil {
		if !d.reader.Done() {
			return nil
		}
		// Exhausted, close it and remember how much it consumed.
		d.prevConsumed += d.reader.Consumed()
		d.reader.Close()
		d.reader = nil
	}
	// If we get here, the reader is either nil or finished. Create a new one.

	// Try the function.
	if d.readerIter != nil && !d.readerIter.Done() {
		var err error
		if d.reader, err = d.readerIter.Next(); err != nil {
			return fmt.Errorf("ensure reader: %w", err)
		}
		return nil
	}

	// Try the list.
	if len(d.readers) == 0 {
		return io.EOF
	}
	d.reader = d.readers[0]
	d.readers = d.readers[1:]
	return nil
}

// Consumed returns the total number of bytes consumed thus far.
func (d *MultiDecoder) Consumed() int64 {
	if d.reader != nil {
		return d.prevConsumed + d.reader.Consumed()
	}
	return d.prevConsumed
}

// Next gets the next record for these readers.
func (d *MultiDecoder) Next() ([]byte, error) {
	if err := d.ensureDecoder(); err != nil {
		return nil, fmt.Errorf("multi next: %w", err)
	}
	b, err := d.reader.Next()
	if err != nil {
		return nil, fmt.Errorf("multi next: %w", err)
	}
	return b, nil
}

// Done returns whether this multi reader has exhausted all underlying readers.
func (d *MultiDecoder) Done() bool {
	if d.readerIter == nil && len(d.readers) == 0 {
		return true
	}
	// Current reader, not exhausted.
	if d.reader != nil && !d.reader.Done() {
		return false
	}
	// Not finished with the list.
	if len(d.readers) != 0 {
		return false
	}
	// Not finished with the reader iterator.
	if d.readerIter != nil && !d.readerIter.Done() {
		return false
	}
	return true
}

// Close closes the currently busy underlying reader and reader iterator, if any.
func (d *MultiDecoder) Close() error {
	defer func() {
		d.reader = nil
		d.readers = nil
		d.readerIter = nil
	}()

	var msgs []string
	if err := d.readerIter.Close(); err != nil {
		msgs = append(msgs, err.Error())
	}
	if err := d.reader.Close(); err != nil {
		msgs = append(msgs, err.Error())
	}
	for _, reader := range d.readers {
		if err := reader.Close(); err != nil {
			msgs = append(msgs, err.Error())
		}
	}

	if len(msgs) == 0 {
		return nil
	}

	return fmt.Errorf("wal dir close: %v", strings.Join(msgs, " :: "))
}

// FilesDecoderIterator is an iterator over readers based on a list of file names.
type FilesDecoderIterator struct {
	fsys     fs.FS
	names    []string
	file     io.ReadCloser
	nextName int
}

// NewFilesDecoderIterator creates a new iterator from a list of file names.
func NewFilesDecoderIterator(fsys fs.FS, names []string) *FilesDecoderIterator {
	return &FilesDecoderIterator{
		names: names,
		fsys:  fsys,
	}
}

// Next returns a new reader if possible, or io.EOF.
func (d *FilesDecoderIterator) Next() (*Decoder, error) {
	if d.Done() {
		return nil, io.EOF
	}
	defer func() {
		d.nextName++
	}()

	if d.file != nil {
		d.file.Close()
	}

	fname := d.names[d.nextName]
	f, err := d.fsys.Open(fname)
	if err != nil {
		return nil, fmt.Errorf("next reader file: %w", err)
	}
	d.file = f
	return NewDecoder(f), nil
}

// Done returns true iff there are no more readers to produce.
func (d *FilesDecoderIterator) Done() bool {
	return d.nextName >= len(d.names)
}

// Close closes the last file, if there is one open, and makes this return io.EOF ever after.
func (d *FilesDecoderIterator) Close() error {
	d.nextName = len(d.names)
	if d.file != nil {
		return d.file.Close()
	}
	return nil
}
